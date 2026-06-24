import { Command } from "@effect/cli";
import { Socket } from "@effect/platform";
import {
  Console,
  Effect,
  Queue,
  Schedule,
  Stream,
  Duration,
  Deferred,
} from "effect";

import {Storage} from "../services/Storage";

const POLYMARKET_WS_URL =
  "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const SUBSCRIBE_PAYLOAD = {
  type: "market",
  assets_ids: [
    "85367286745806857961178482075931972831841231758328346969840810630055458089640",
  ],
  custom_feature_enabled: true,
};

const createHeartbeatPump = (
  controlQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
) =>
  Effect.gen(function* () {
    yield* Deferred.await(isSocketOpen);
    yield* Effect.logInfo("心跳泵已激活，准备工作...");
    const pingAction = Queue.offer(controlQueue, "PING").pipe(
      Effect.tap(() => Effect.logDebug("投递 PING 心跳...")),
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
    yield* Effect.sleep("4 seconds");
    yield* Effect.logInfo("🚨 [策略触发] 运行 4 秒后，动态追加新市场订阅！");
    yield* Queue.offer(
      controlQueue,
      JSON.stringify({
        operation: "subscribe",
        assets_ids: [
          "21742633143463906290569050155826241533067272736897614950488156847949938836455",
        ],
        custom_feature_enabled: true,
      }),
    );
  });

const createNetworkPump = (
  wsConnection: Socket.Socket,
  messageQueue: Queue.Enqueue<string>, // 💡 只写权限：只能往收件箱塞
  controlQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
) =>
  wsConnection.runRaw(
    (msg) =>
      Queue.offer(
        messageQueue,
        typeof msg === "string" ? msg : new TextDecoder().decode(msg),
      ),
    {
      // 极其干净的开门动作
      onOpen: Effect.gen(function* () {
        yield* Effect.logInfo("🔥 [连接成功] 物理网络已通，解开状态锁！");
        // 1. 开锁！唤醒外面的心跳泵
        yield* Deferred.succeed(isSocketOpen, void 0);

        // 2. 投递初始订阅
        yield* Effect.logInfo("🔥 把初始订阅载荷投递到发件箱...");
        yield* Queue.offer(controlQueue, JSON.stringify(SUBSCRIBE_PAYLOAD));
      }).pipe(
        Effect.catchAll((err) => Effect.logError(`❌ 初始化失败: ${err}`)),
      ),
    },
  );

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
      controlQueue,
      isSocketOpen,
    );
    const storage = yield* Storage;
    const messageProcessor = Stream.fromQueue(messageQueue).pipe(
      Stream.map((msg) => msg.trim()),
      Stream.filter((msg) => msg !== "" && msg !== "PONG" && msg !== "[]"),
      Stream.take(500),
      Stream.zip(Stream.iterate(1, (n) => n + 1)),
      Stream.tap(([msg, count]) =>
        Effect.log(`📥 [${count}/500] 拦截流出数据: ${msg.slice(0, 100)}...`),
      ),
      Stream.map(([msg]) => msg),
      Stream.tap((msg) =>
        Effect.log(`📥 拦截流出数据:${msg.slice(0, 100)}...`),
      ),
      Stream.run(storage.makeJsonlSink("polymarket_raw")),
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
    Effect.andThen(Effect.logInfo(`🎉 500条数据抓取完毕,资源已全部安全释放！`)),
  ),
);
