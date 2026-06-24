import { Layer, Effect, Clock, Stream, Sink } from "effect";
import { FileSystem } from "@effect/platform/FileSystem";
import { Path } from "@effect/platform/Path";
import { Storage } from "../services/Storage";

export const LocalStorageLive = Layer.effect(
  Storage,
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const path = yield* Path;

    return Storage.of({
      writeStream: makeWriteStream(fs, path),
      makeJsonlSink: implMakeJsonlSink(fs, path),
    });
  }),
);


const makeWriteStream = (
  fs: FileSystem, // 直接使用模块导出的接口类型
  path: Path
): Storage["writeStream"] => {      // 👈 终极魔法：直接“白嫖”接口里定义好的完美签名！
  // 此时参数 key, byteStream, options 的类型已经被自动推导出来了，不需要写任何 :xxx
  return (key, byteStream, options) =>
    Effect.gen(function* () {
      const ext = options?.ext ?? "csv";
      const dir = options?.dir ?? ".local";

      const millis = yield* Clock.currentTimeMillis;
      const timestamp = new Date(millis)
        .toISOString()
        .replace(/[-T:]/g, "")
        .split(".")[0];

      const targetDir = path.join(path.resolve("."), dir);
      const targetFilePath = path.join(targetDir, `${key}-${timestamp}.${ext}`);

      yield* fs
        .makeDirectory(targetDir, { recursive: true })
        .pipe(
          Effect.mapError((e) => new Error(`Failed to make directory: ${String(e)}`)),
        );

      yield* byteStream.pipe(
        Stream.run(fs.sink(targetFilePath)),
        Effect.mapError((e) => new Error(`Failed to write stream: ${String(e)}`)),
      );

      return targetFilePath;
    });
};

const implMakeJsonlSink = (fs: FileSystem, path: Path): Storage["makeJsonlSink"] => {
  const textEncoder = new TextEncoder();
  return (key, options) =>
    Sink.unwrap(
      Effect.gen(function* () {
        const dir = options?.dir ?? ".local";

        // 1. 生成高精度时间戳
        const millis = yield* Clock.currentTimeMillis;
        const timestamp = new Date(millis)
          .toISOString()
          .replace(/[-T:]/g, "")
          .split(".")[0];

        // 2. 拼接路径
        const targetDir = path.join(path.resolve("."), dir);
        const targetFilePath = path.join(targetDir, `${key}-${timestamp}.jsonl`);

        // 3. 确保目录存在
        yield* fs.makeDirectory(targetDir, { recursive: true });

        yield* Effect.logInfo(`📁 数据落盘管道已锁定目标: ${targetFilePath}`);

        // 4. 构建并返回底层的物理 Sink
        // fs.sink 默认接收 Uint8Array，通过 mapInput 向上游暴露出 string 接口
        return fs.sink(targetFilePath).pipe(
          Sink.mapInput((line: string) => textEncoder.encode(line + "\n"))
        );
      }).pipe(
        // 将 Platform 层的专有错误，统一抹平为接口契约中定义的标准 Error
        Effect.mapError(
          (e) => new Error(`MakeJsonlSink 初始化或写入失败: ${String(e)}`)
        )
      )
    );
}