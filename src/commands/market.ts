import { Command } from "@effect/cli";
import { Effect, Stream } from "effect";
import { PolymarketHarvester } from "../services/PolymarketHarvester";
import { Storage } from "../services/Storage";

export const marketSubCommands = Command.make("market", {}, () =>
  Effect.gen(function* () {
    const polymarketService = yield* PolymarketHarvester;
    const rawStream = polymarketService.fetchAll();
    yield* Effect.log(`抓取市场最新信息...`);
    const storage = yield* Storage;
    const rowStream = rawStream.pipe(
      Stream.take(50),
      Stream.map(JSON.stringify),
      Stream.intersperse("\n"),
      Stream.encodeText,
    );
    const filename = yield* storage.writeStream("market", rowStream, {
      ext: "jsonl",
    });
    yield* Effect.logInfo(`✅ 抓取完成！数据已安全写入 ${filename}`);
  }),
);
