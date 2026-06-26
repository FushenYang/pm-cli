import type { Socket } from "@effect/platform";
import { Deferred, Effect, Queue, Schedule } from "effect";
import { TextDecoderService } from "../../services/TextDecoderService";

export const createNetworkPump = (
  wsConnection: Socket.Socket,
  messageQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
): Effect.Effect<void, Socket.SocketError, TextDecoderService> =>
  Effect.andThen(TextDecoderService, (decoder) => {
    return wsConnection.run(
      (chunk) => messageQueue.pipe(Queue.offer(decoder.decode(chunk))),
      {
        onOpen: Effect.logInfo(
          "🔥 [连接成功] 声明式网络流已通电，解开状态锁！",
        ).pipe(Effect.andThen(() => Deferred.succeed(isSocketOpen, void 0))),
      },
    );
  });

export const createHeartbeatPump = (
  controlQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
) =>
  Effect.gen(function* () {
    yield* Deferred.await(isSocketOpen);
    yield* Effect.logInfo("心跳泵已激活，准备工作...");

    yield* controlQueue.pipe(
      Queue.offer("PING"),
      Effect.tap(() => Effect.logInfo("投递 PING 心跳...")),
      Effect.repeat(Schedule.spaced("10 seconds")),
    );
  });

export const createSenderPump = (
  controlQueue: Queue.Dequeue<string>,
  write: (chunk: string) => Effect.Effect<void, Socket.SocketError>,
) => controlQueue.pipe(Queue.take, Effect.andThen(write), Effect.forever);
