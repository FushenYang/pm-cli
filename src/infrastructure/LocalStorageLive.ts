import { Layer, Effect, Clock, Stream, Sink, DateTime } from "effect";
import { FileSystem } from "@effect/platform/FileSystem";
import { Path } from "@effect/platform/Path";
import { Storage } from "../services/Storage";
import { TextEncoderService } from "../services/TextEncoderService";

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

const makeWriteStream =
  (
    fs: FileSystem, // 直接使用模块导出的接口类型
    path: Path,
  ): Storage["writeStream"] =>
  (key, byteStream, options) =>
    Effect.gen(function* () {
      const ext = options?.ext ?? "csv";
      const dir = options?.dir ?? ".local";
      const now = yield* DateTime.now;
      const timestamp = DateTime.format(now, {
        locale: "zh-CN",
        timeZone: "Asia/Shanghai",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hourCycle: "h23",
      }).replace(/\D/g, "");

      const targetDir = path.join(path.resolve("."), dir);
      const targetFilePath = path.join(targetDir, `${key}-${timestamp}.${ext}`);

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
    });

const implMakeJsonlSink =
  (fs: FileSystem, path: Path): Storage["makeJsonlSink"] =>
  (key, options) =>
    Effect.gen(function* () {
      const dir = options?.dir ?? ".local";
      const encoder = yield* TextEncoderService;
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
      return fs
        .sink(targetFilePath)
        .pipe(Sink.mapInput((line: string) => encoder.encode(line + "\n")));
    })
      .pipe(
        // 将 Platform 层的专有错误，统一抹平为接口契约中定义的标准 Error
        Effect.mapError(
          (e) => new Error(`MakeJsonlSink 初始化或写入失败: ${String(e)}`),
        ),
      )
      .pipe(Sink.unwrap);
