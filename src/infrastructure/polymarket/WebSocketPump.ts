import { Socket } from "@effect/platform";
import { Deferred, Effect, Queue, Schedule, Stream } from "effect";
import { TextDecoderService } from "../../services/TextDecoderService";

export const createNetworkPump = (
  wsConnection: Socket.Socket,
  messageQueue: Queue.Enqueue<string>,
  isSocketOpen: Deferred.Deferred<void, never>,
) =>
  TextDecoderService.pipe(
    Effect.andThen((decoder) =>
      wsConnection.run(
        (chunk) => messageQueue.pipe(Queue.offer(decoder.decode(chunk))),
        {
          onOpen: Deferred.succeed(isSocketOpen, void 0),
        },
      ),
    ),
    Effect.tap(() => Effect.logInfo("网络泵已激活，开始工作...")),
  );

export const getSocketPump = (
  wsConnection: Socket.Socket,
  isSocketOpen: Deferred.Deferred<void, never>,
) =>
  Effect.gen(function* () {
    const decoder = yield* TextDecoderService;
    const queue = yield* Queue.unbounded<string>();
    yield* wsConnection.run(
      (chunk) => queue.pipe(Queue.offer(decoder.decode(chunk))),
      {
        onOpen: Deferred.succeed(isSocketOpen, void 0),
      },
    );
    return Stream.fromQueue(queue);
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
