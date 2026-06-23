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

    // 1. 创建 WebSocket 实例，显式加上 5 秒超时控制
    const wsConnection = yield* Socket.makeWebSocket(POLYMARKET_WS_URL, {
      openTimeout: Duration.seconds(5),
    });

    const messageQueue = yield* Queue.bounded<string>(100);

    const messageProcessor = Stream.fromQueue(messageQueue).pipe(
      Stream.take(10),
      Stream.tap((msg) => Console.log(`📥 成功拦截实时流出数据:\n${msg}\n`)),
      Stream.runDrain,
    );

    yield* Effect.gen(function* () {
      // 💡 核心解药 1：把获取 writer 提到最外层（绑定当前作用域的 Scope），消灭 TS 报错
      const write = yield* wsConnection.writer;

      // 2. 此时 onOpenAction 内部无任何 Scope 依赖，完美契合标准
      const onOpenAction = Effect.gen(function* () {
        yield* Effect.logInfo(
          "🔥 [连接成功] 物理网络已通！开始倾倒订阅载荷...",
        );
        yield* write(JSON.stringify(SUBSCRIBE_PAYLOAD));
        yield* Effect.logInfo("✅ 订阅请求已安全打入！");

        // 安全启动心跳
        yield* Effect.gen(function* () {
          yield* write("PING");
          yield* Effect.logDebug("💓 发送 PING 心跳...");
        }).pipe(Effect.repeat(Schedule.spaced("10 seconds")), Effect.fork);
      }).pipe(
        Effect.catchAll((error) =>
          Effect.logError(`❌ 建立连接后内部触发失败: ${error}`),
        ),
      );

      // 3. 构建底层网络接收泵
      const networkPump = wsConnection
        .runRaw(
          (msg) =>
            Queue.offer(
              messageQueue,
              typeof msg === "string" ? msg : new TextDecoder().decode(msg),
            ),
          { onOpen: onOpenAction },
        )
        .pipe(
          // 如果 5 秒内无法建立物理握手或网络断开，强制抛出错误让主流程知情
          Effect.timeoutFail({
            onTimeout: () => new Error("WebSocket 连接或握手在 5 秒内超时！"),
            duration: Duration.seconds(5),
          }),
        );

      yield* Effect.logInfo("📡 正在并发激活 [网络接收泵] 与 [流处理器]...");

      // 💡 核心解药 2：用 Effect.all 并发结对运行它们！
      // 只要 networkPump 发生超时死亡，整个 Effect 块会立刻中断并把报错吐在终端，绝不傻等！
      yield* Effect.all([networkPump, messageProcessor], {
        concurrency: "unbounded",
      });
    }).pipe(Effect.scoped);

    yield* Effect.logInfo(`🎉 10条热门数据抓取完毕，连接已安全自动释放！`);
  }),
);
