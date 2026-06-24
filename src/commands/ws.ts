import { Command } from "@effect/cli";
import { Socket } from "@effect/platform";
import { Console, Effect, Queue, Schedule, Stream, Duration } from "effect";

const POLYMARKET_WS_URL =
  "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const SUBSCRIBE_PAYLOAD = {
  type: "market",
  assets_ids: [
    "85367286745806857961178482075931972831841231758328346969840810630055458089640",
  ],
  custom_feature_enabled: true,
};

export const wsSubCommands = Command.make("ws", {}, () =>
  Effect.gen(function* () {
    yield* Effect.logInfo(`🔌 正在初始化 Polymarket WebSocket 管道...`);

    const wsConnection = yield* Socket.makeWebSocket(POLYMARKET_WS_URL, {
      openTimeout: Duration.seconds(5),
    });

    // 收件箱（原有的）
    const messageQueue = yield* Queue.bounded<string>(100);
    // 💡 改造 1：新增发件箱（控制队列）
    const controlQueue = yield* Queue.unbounded<string>();

    const messageProcessor = Stream.fromQueue(messageQueue).pipe(
      Stream.take(5), // 我们多拿几条，等下看动态订阅的效果
      Stream.tap((msg) => Console.log(`📥 拦截流出数据:\n${msg.slice(0, 100)}...\n`)),
      Stream.runDrain,
    );

    yield* Effect.gen(function* () {
      const write = yield* wsConnection.writer;

      // 💡 改造 2：新增发送快递员（死循环从 controlQueue 拿指令，调用 write）
      const senderPump = Queue.take(controlQueue).pipe(
        Effect.flatMap((msg) => write(msg)),
        Effect.forever
      );

      // 完全保留你的 onOpenAction，只是把 write 替换成 Queue.offer 投递到发件箱！
      const onOpenAction = Effect.gen(function* () {
        yield* Effect.logInfo("🔥 [连接成功] 把初始订阅载荷投递到发件箱...");
        // 以前是 yield* write(...)，现在改成：
        yield* Queue.offer(controlQueue, JSON.stringify(SUBSCRIBE_PAYLOAD));

        yield* Effect.gen(function* () {
          yield* Queue.offer(controlQueue, "PING"); // 心跳也统一扔进发件箱
          yield* Effect.logDebug("💓 投递 PING 心跳...");
        }).pipe(Effect.repeat(Schedule.spaced("10 seconds")), Effect.fork);
      }).pipe(
        Effect.catchAll((error) => Effect.logError(`❌ 内部触发失败: ${error}`)),
      );

      // 完全保留你的接收泵
      const networkPump = wsConnection.runRaw(
        (msg) => Queue.offer(messageQueue, typeof msg === "string" ? msg : new TextDecoder().decode(msg)),
        { onOpen: onOpenAction },
      );

      // 💡 改造 3：新增一个动态策略微服务（在外面独立运行）
      const dynamicStrategy = Effect.gen(function* () {
        yield* Effect.sleep("4 seconds");
        yield* Effect.logInfo("🚨 [策略触发] 运行 4 秒后，动态追加新市场订阅！");
        yield* Queue.offer(controlQueue, JSON.stringify({
          operation: "subscribe",
          assets_ids: ["21742633143463906290569050155826241533067272736897614950488156847949938836455"],
          custom_feature_enabled: true
        }));
      });

      yield* Effect.logInfo("📡 正在并发激活所有组件...");

      yield* Effect.logInfo("📡 正在将网络基础设施挂载到后台...");
      yield* Effect.forkScoped(networkPump);
      yield* Effect.forkScoped(senderPump);

      yield* Effect.forkScoped(dynamicStrategy);
      yield* Effect.logInfo("🎯 前台主业务开始阻塞拦截数据...");
      yield* messageProcessor;

    }).pipe(Effect.scoped);

    yield* Effect.logInfo(`🎉 5条热门数据抓取完毕，连接已安全自动释放！`);
  }),
);