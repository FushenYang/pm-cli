import { Layer, Effect, Clock, Stream } from "effect";
import { FileSystem } from "@effect/platform/FileSystem";
import { Path } from "@effect/platform/Path";
import { Storage } from "./Storage";

export const LocalStorageLive = Layer.effect(
  Storage,
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const path = yield* Path;

    return Storage.of({
      writeStream: (key, byteStream, options) =>
        // 1. 去掉 Effect.gen 后面的手动泛型，让其自然推导
        Effect.gen(function* () {
          const ext = options?.ext ?? "csv";
          const dir = options?.dir ?? ".local";

          const millis = yield* Clock.currentTimeMillis;
          const timestamp = new Date(millis)
            .toISOString()
            .replace(/[-T:]/g, "")
            .split(".")[0];
          const targetDir = path.join(path.resolve("."), dir);
          const targetFilePath = path.join(
            targetDir,
            `${key}-${timestamp}.${ext}`,
          );

          yield* fs
            .makeDirectory(targetDir, { recursive: true })
            .pipe(
              Effect.mapError(
                (e) => new Error(`Failed to make directory: ${String(e)}`),
              ),
            );

          yield* byteStream.pipe(
            Stream.run(fs.sink(targetFilePath)),
            Effect.mapError(
              (e) => new Error(`Failed to write stream: ${String(e)}`),
            ),
          );

          return targetFilePath;
        }),
    });
  }),
);
