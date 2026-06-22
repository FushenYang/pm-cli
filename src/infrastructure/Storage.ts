import { Context, Effect, Stream } from "effect";

export interface Storage {
  /**
   * 接收一个字节流并将其持久化到指定位置
   * 返回最终写入的路径或标识符符
   */
  readonly writeStream: (
    key: string,
    byteStream: Stream.Stream<Uint8Array, Error, never>,
    options?: { dir?: string; ext?: string }
  ) => Effect.Effect<string, Error, never>;
}

export const Storage = Context.GenericTag<Storage>("infrastructure/Storage");