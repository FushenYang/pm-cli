import { Command } from "@effect/cli";
import { Socket } from "@effect/platform";
import { Effect, Queue, Stream, Duration, Deferred } from "effect";

import { Storage } from "../services/Storage";
import {
  createHeartbeatPump,
  createNetworkPump,
  createSenderPump,
} from "../infrastructure/polymarket/WebSocketPump";

const POLYMARKET_WS_URL =
  "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const createDynamicStrategy = (controlQueue: Queue.Enqueue<string>) =>
  Effect.gen(function* () {
    yield* Effect.logInfo("🚨 添加订阅");
    yield* Queue.offer(
      controlQueue,
      JSON.stringify({
        operation: "subscribe",
        assets_ids: [
          "2293765953960121477637189818103417477577027293347888634089788327148014326181",
        ],
        custom_feature_enabled: true,
      }),
    );
  });

const createTakeMessage = (
  count: number,
  messageQueue: Queue.Dequeue<string>,
) =>
  Effect.andThen(Storage, (storage) =>
    Stream.fromQueue(messageQueue).pipe(
      Stream.map((msg) => msg.trim()),
      Stream.filter((msg) => msg !== "" && msg !== "PONG" && msg !== "[]"),
      Stream.take(count),
      Stream.zip(Stream.iterate(1, (n) => n + 1)),
      Stream.tap(([msg, c]) =>
        Effect.log(`📥 [${c}/${count}] 拦截流出数据: ${msg.slice(0, 100)}...`),
      ),
      Stream.map(([msg]) => msg),
      Stream.run(storage.makeJsonlSink("orderbook")),
    ),
  );

export const wsSubCommands = Command.make("ws", {}, () =>
  Effect.gen(function* () {
    yield* Effect.logInfo(`🔌 正在初始化 Polymarket WebSocket 管道...`);

    const wsConnection = yield* Socket.makeWebSocket(POLYMARKET_WS_URL, {
      openTimeout: Duration.seconds(5),
    });
    const write = yield* wsConnection.writer;
    yield* Storage;

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

    yield* Effect.logInfo("📡 正在将网络基础设施挂载到后台...");

    yield* Effect.forkScoped(networkPump);
    yield* Effect.forkScoped(senderPump);
    yield* Effect.forkScoped(heartbeatPump);
    yield* Effect.forkScoped(dynamicStrategy);

    yield* Effect.logInfo("🎯 前台主业务开始阻塞拦截数据...");

    yield* createTakeMessage(50, messageQueue);
  }).pipe(
    Effect.scoped,
    Effect.andThen(Effect.logInfo(`🎉 50条数据抓取完毕,资源已全部安全释放！`)),
  ),
);
