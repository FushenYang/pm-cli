import { Context, Effect, Sink, Stream } from "effect";
import type { TextEncoderService } from "./TextEncoderService";

export interface Storage {
  readonly writeStream: (
    key: string,
    byteStream: Stream.Stream<Uint8Array, Error, never>,
    options?: { dir?: string; ext?: string },
  ) => Effect.Effect<string, Error, never>;
  readonly makeJsonlSink: (
    key: string,
    options?: { dir?: string },
  ) => Sink.Sink<void, string, never, Error, TextEncoderService>;
}

export const Storage = Context.GenericTag<Storage>("infrastructure/Storage");
