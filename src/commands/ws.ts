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

    // 1. 创建 WebSocket 实例，显式加上 5 秒超时控制！防止无限挂死
    const wsConnection = yield* Socket.makeWebSocket(POLYMARKET_WS_URL, {
      openTimeout: Duration.seconds(5),
    });

    const messageQueue = yield* Queue.bounded<string>(100);

    const messageProcessor = Stream.fromQueue(messageQueue).pipe(
      Stream.take(10),
      Stream.tap((msg) => Console.log(`📥 成功拦截流出数据:\n${msg}\n`)),
      Stream.runDrain,
    );

    yield* Effect.gen(function* () {
      yield* Effect.logInfo("📡 正在激活底层数据网络泵...");

      // 2. 💡 破局核心：把获取 writer 和发送载荷的动作，全部挪进 runRaw 已经就绪的 onOpen 内部
      const onOpenAction = Effect.gen(function* () {
        yield* Effect.logInfo(
          "🔥 [连接成功] 物理网络已通！正在现场获取写入契约...",
        );

        // 此时物理长连接已开，yield* writer 会瞬间秒回，绝不阻塞！
        const write = yield* wsConnection.writer;

        yield* Effect.logInfo("📤 正在向远端管道倾倒订阅载荷...");
        yield* write(JSON.stringify(SUBSCRIBE_PAYLOAD));
        yield* Effect.logInfo("✅ 订阅请求已安全打入！");

        // 在这里安全地启动心跳
        yield* Effect.gen(function* () {
          yield* write("PING");
          yield* Effect.logDebug("💓 发送 PING 心跳...");
        }).pipe(Effect.repeat(Schedule.spaced("10 seconds")), Effect.fork);
      }).pipe(
        Effect.catchAll((error) =>
          Effect.logError(`❌ 建立连接后内部触发失败: ${error}`),
        ),
      );

      // 3. 让接收泵在后台欢快地奔跑
      yield* wsConnection
        .runRaw(
          (msg) =>
            Queue.offer(
              messageQueue,
              typeof msg === "string" ? msg : new TextDecoder().decode(msg),
            ),
          { onOpen: onOpenAction },
        )
        .pipe(
          // 如果 5 秒内连 TCP 握手都做不完，直接抛错清醒过来，不要死等
          Effect.timeout(Duration.seconds(5)),
          Effect.catchAll((err) =>
            Effect.logError(`⚠️ 物理连接或网络泵运行遭遇异常/超时: ${err}`),
          ),
          Effect.fork,
        );

      yield* Effect.logInfo(
        "⏳ 流量接收阀门已全开，开始阻塞等待前 10 条热门数据...",
      );
      yield* messageProcessor;
    }).pipe(Effect.scoped);

    yield* Effect.logInfo(`🎉 10条热门数据抓取完毕，连接已安全自动释放！`);
  }),
);
