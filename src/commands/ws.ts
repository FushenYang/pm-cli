import { Command } from "@effect/cli";
import { Socket } from "@effect/platform";
import { Console, Effect, Queue, Schedule, Stream } from "effect";

const POLYMARKET_WS_URL =
  "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const SUBSCRIBE_PAYLOAD = {
  type: "market",
  assets_ids: [
    "61595193871140044336898809781418183952441527621084596848414908595268863899573",
  ],
  custom_feature_enabled: true,
};

export const wsSubCommands = Command.make("ws", {}, () =>
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `🔌 正在尝试建立与 Polymarket CLOB WebSocket 的物理连接...`,
    );

    // 1. 创建 WebSocket 实例
    const wsConnection = yield* Socket.makeWebSocket(POLYMARKET_WS_URL);

    // 2. 💡 绝招：创建一个内部有界队列（Capacity 为 100），用来抗住并缓冲涌入的文本数据
    const messageQueue = yield* Queue.bounded<string>(100);

    // 3. 极其清爽的声明式流：直接把队列转成流，并严格拿 10 条就走！
    const messageProcessor = Stream.fromQueue(messageQueue).pipe(
      Stream.take(10),
      Stream.tap((msg) => Console.log(`📥 收到实时网络数据包:\n${msg}\n`)),
      Stream.runDrain,
    );

    // 4. 获取长连接的写入器 (Writer)，在 Scope 作用域内复用它
    const write = yield* wsConnection.writer;

    // 5. 编排并并发执行核心逻辑
    yield* Effect.gen(function* () {
      // 驱动 A：在后台悄悄发送心跳，直接复用已经获取的 write 契约
      yield* Effect.gen(function* () {
        yield* write("PING");
        yield* Effect.logDebug("💓 发送 PING 心跳...");
      }).pipe(Effect.repeat(Schedule.spaced("10 seconds")), Effect.fork);

      // 驱动 B：连接打开时，立即发送订阅载荷
      const onOpenAction = Effect.gen(function* () {
        yield* write(JSON.stringify(SUBSCRIBE_PAYLOAD));
        yield* Effect.logInfo("✅ 订阅请求已成功打入远端服务器...");
      }).pipe(
        // 💡 绝招：捕获并处理掉所有的 SocketError，把错误类型降维成 `never`
        Effect.catchAll((error) =>
          Effect.logError(`❌ 建立连接后发送订阅失败: ${error}`),
        ),
      );

      // 驱动 C：启动底层接收泵（runRaw 可以直接吐出原始 string），把数据无脑塞进我们的队列
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
          Effect.fork, // 让泵在后台跑
        );

      // 主阻塞点：等待我们的流处理器消费完 10 条数据，一旦消费完，立刻退出 Effect.scoped
      yield* messageProcessor;
    }).pipe(
      Effect.scoped, // 💡 只要消费完10条，scoped 结束，自动销毁连接，关掉泵，释放内存！
    );

    yield* Effect.logInfo(`🎉 10条热门数据抓取完毕，连接已安全自动释放！`);
  }),
);
