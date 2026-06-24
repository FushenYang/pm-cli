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
    Effect.flatMap((msg) => write(msg)),
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

  // 1. 严格死守 Socket 接口定义的契约签名
  // run 方法专门接收 (chunk: Uint8Array) => Effect
  return wsConnection.run(
    (chunk) =>
      Effect.gen(function* () {
        // 2. 纯净的数据解码与投递，利用 Effect 的管道流转
        const decoded = textDecoder.decode(chunk);
        yield* Queue.offer(messageQueue, decoded);
      }),
    {
      // 3. 严格遵循接口，在这里极其干净地解开你的状态锁
      onOpen: Effect.gen(function* () {
        yield* Effect.logInfo("🔥 [连接成功] 声明式网络流已通电，解开状态锁！");
        yield* Deferred.succeed(isSocketOpen, void 0);
      }),
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

    // 收件箱（原有的）
    const messageQueue = yield* Queue.bounded<string>(100);
    // 💡 改造 1：新增发件箱（控制队列）
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
