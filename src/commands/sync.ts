import { Command } from "@effect/cli";
import { Path, FileSystem } from "@effect/platform";
import { PolymarketApi } from "../services/PolymarketApi";
import { Effect } from "effect";
import {
  CSV_HEADER_ROW,
  marketToCsvRow,
} from "../adapters/MarketSummaryCsv.js";

export const syncSubCommand = Command.make(
  "sync",
  // 我们顺手加一个命令行参数（比如过滤选项），体验一下正规 CLI 的快感
  {},
  // 核心执行逻辑
  () =>
    Effect.gen(function* () {
      yield* Effect.log(`[pm-cli] 开始抓取市场数据...)`);
      const polymarketService = yield* PolymarketApi;
      const maybeMarkets = yield* polymarketService.fetchPage({
        limit: 100,
        offset: 0,
      });

      const csvContent = maybeMarkets.map(marketToCsvRow).join("\n");

      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const localDirPath = path.join(path.resolve("."), ".local");
      yield* fs.makeDirectory(localDirPath, { recursive: true });
      const targetFilePath = path.join(localDirPath, "final.csv");
      yield* fs.writeFileString(
        targetFilePath,
        CSV_HEADER_ROW + "\n" + csvContent,
      );

      yield* Effect.log("[pm-cli] 读取数据成功");
    }),
);
