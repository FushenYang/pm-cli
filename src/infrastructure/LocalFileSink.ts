// src/infrastructure/LocalFileSink.ts
import { FileSystem } from "@effect/platform";
import { Effect, Sink, Chunk } from "effect";

// 明确表明这是本地文件系统的实现
export const makeLocalCsvSink = (filePath: string) =>
  Sink.unwrapScoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const file = yield* fs.open(filePath, { flag: "w" });
      const encoder = new TextEncoder();

      return Sink.forEachChunk((chunk: Chunk.Chunk<string>) => {
        const lines = [...chunk].join("\n") + "\n";
        return file.write(encoder.encode(lines));
      });
    }),
  );
