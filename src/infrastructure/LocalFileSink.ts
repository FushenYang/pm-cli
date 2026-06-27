// src/infrastructure/LocalFileSink.ts
import { FileSystem } from "@effect/platform";
import { Effect, Sink, Chunk } from "effect";
import { TextEncoderService } from "../services/TextEncoderService";

// 明确表明这是本地文件系统的实现
export const makeLocalCsvSink = (filePath: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const file = yield* fs.open(filePath, { flag: "w" });
    const encoder = yield* TextEncoderService;
    return Sink.forEachChunk((chunk: Chunk.Chunk<string>) =>
      file.write(encoder.encode([...chunk].join("\n") + "\n")),
    );
  }).pipe(Sink.unwrapScoped);
