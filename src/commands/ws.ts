import { Command } from "@effect/cli";
import { Socket } from "@effect/platform";
import { Effect, Queue, Schedule, Stream, Duration, Deferred } from "effect";

import { Storage } from "../services/Storage";

const POLYMARKET_WS_URL =
  "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const createHeartbeatPump = (
  controlQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
) =>
  Effect.gen(function* () {
    yield* Deferred.await(isSocketOpen);
    yield* Effect.logInfo("心跳泵已激活，准备工作...");
    const pingAction = Queue.offer(controlQueue, "PING").pipe(
      Effect.tap(() => Effect.logInfo("投递 PING 心跳...")),
    );
    yield* pingAction.pipe(Effect.repeat(Schedule.spaced("10 seconds")));
  });

const createSenderPump = (
  controlQueue: Queue.Dequeue<string>, // 💡 只读权限：只能从队列里拿
  write: (chunk: string) => Effect.Effect<void, Socket.SocketError>,
) =>
  Queue.take(controlQueue).pipe(
    Effect.flatMap(write),
    Effect.forever, // 永动机
  );

const createDynamicStrategy = (controlQueue: Queue.Enqueue<string>) =>
  Effect.gen(function* () {
    yield* Effect.logInfo("🚨 添加订阅");
    yield* Queue.offer(
      controlQueue,
      JSON.stringify({
        operation: "subscribe",
        assets_ids: [
          "85367286745806857961178482075931972831841231758328346969840810630055458089640",
        ],
        custom_feature_enabled: true,
      }),
    );
  });

export const createNetworkPump = (
  wsConnection: Socket.Socket,
  messageQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
): Effect.Effect<void, Socket.SocketError, never> => {
  const textDecoder = new TextDecoder();
  return wsConnection.run(
    (chunk) => Queue.offer(messageQueue, textDecoder.decode(chunk)),
    {
      onOpen: Effect.logInfo("🔥 [连接成功] 声明式网络流已通电，解开状态锁！").pipe(
        Effect.andThen(Deferred.succeed(isSocketOpen, void 0))),
    },
  );
};

export const wsSubCommands = Command.make("ws", {}, () =>
  Effect.gen(function* () {
    yield* Effect.logInfo(`🔌 正在初始化 Polymarket WebSocket 管道...`);

    const wsConnection = yield* Socket.makeWebSocket(POLYMARKET_WS_URL, {
      openTimeout: Duration.seconds(5),
    });
    const write = yield* wsConnection.writer;


    const messageQueue = yield* Queue.bounded<string>(100);
    const controlQueue = yield* Queue.unbounded<string>();
    const isSocketOpen = yield* Deferred.make<void, never>();

    const heartbeatPump = createHeartbeatPump(controlQueue, isSocketOpen);
    const senderPump = createSenderPump(controlQueue, write);
    const dynamicStrategy = createDynamicStrategy(controlQueue);
    const networkPump = createNetworkPump(
      wsConnection,
      messageQueue,
      isSocketOpen,
    );
    const storage = yield* Storage;
    const messageProcessor = Stream.fromQueue(messageQueue).pipe(
      Stream.map((msg) => msg.trim()),
      Stream.filter((msg) => msg !== "" && msg !== "PONG" && msg !== "[]"),
      Stream.take(50),
      Stream.zip(Stream.iterate(1, (n) => n + 1)),
      Stream.tap(([msg, count]) =>
        Effect.log(`📥 [${count}/50] 拦截流出数据: ${msg.slice(0, 100)}...`),
      ),
      Stream.map(([msg]) => msg),
      Stream.run(storage.makeJsonlSink("orderbook")),
    );

    yield* Effect.logInfo("📡 正在将网络基础设施挂载到后台...");

    yield* Effect.forkScoped(networkPump);
    yield* Effect.forkScoped(senderPump);
    yield* Effect.forkScoped(heartbeatPump);
    yield* Effect.forkScoped(dynamicStrategy);

    yield* Effect.logInfo("🎯 前台主业务开始阻塞拦截数据...");

    yield* messageProcessor;
  }).pipe(
    Effect.scoped,
    Effect.andThen(Effect.logInfo(`🎉 50条数据抓取完毕,资源已全部安全释放！`)),
  ),
);
