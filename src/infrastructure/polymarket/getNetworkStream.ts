import { Stream, Effect, Deferred } from "effect";
import { Socket } from "@effect/platform";
import { TextDecoderService } from "../../services/TextDecoderService";
export const getNetworkStream = (
  wsConnection: Socket.Socket,
  isSocketOpen: Deferred.Deferred<void, never>,
): Stream.Stream<string, Socket.SocketError, TextDecoderService> =>
  Stream.unwrapScoped(
    Effect.gen(function* () {
      const decoder = yield* TextDecoderService;
      const rawStream = Stream.async<string, Socket.SocketError>((emit) =>
        wsConnection
          .run(
            (chunk) => Effect.promise(() => emit.single(decoder.decode(chunk))),
            {
              onOpen: Deferred.succeed(isSocketOpen, void 0),
            },
          )
          .pipe(Effect.catchAll((err) => Effect.sync(() => emit.fail(err)))),
      );
      return rawStream;
    }),
  );
