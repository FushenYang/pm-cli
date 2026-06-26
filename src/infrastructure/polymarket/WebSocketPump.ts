import type { Socket } from "@effect/platform";
import { Deferred, Effect, Queue, Schedule } from "effect";

export const createNetworkPump = (
  wsConnection: Socket.Socket,
  messageQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
): Effect.Effect<void, Socket.SocketError, never> => {
  const textDecoder = new TextDecoder();
  return wsConnection.run(
    (chunk) => Queue.offer(messageQueue, textDecoder.decode(chunk)),
    {
      onOpen: Effect.logInfo(
        "🔥 [连接成功] 声明式网络流已通电，解开状态锁！",
      ).pipe(Effect.andThen(() => Deferred.succeed(isSocketOpen, void 0))),
    },
  );
};

export const createHeartbeatPump = (
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

export const createSenderPump = (
  controlQueue: Queue.Dequeue<string>, // 💡 只读权限：只能从队列里拿
  write: (chunk: string) => Effect.Effect<void, Socket.SocketError>,
) =>
  Queue.take(controlQueue).pipe(
    Effect.flatMap(write),
    Effect.forever, // 永动机
  );
