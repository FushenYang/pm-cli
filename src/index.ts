import { Command } from "@effect/cli";
import { FetchHttpClient, Path, FileSystem } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Stream } from "effect";
import { NetworkLive } from "./infrastructure/NetworkLive.js";
import { LocalStorageLive } from "./infrastructure/LocalStorageLive.js";

import { PolymarketApi, PolymarketApiLive } from "./services/PolymarketApi.js";
import { CSV_HEADER_ROW, marketToCsvRow } from "./adapters/MarketSummaryCsv.js";
import { Storage } from "./infrastructure/Storage.js";
import {
  PolymarketHarvester,
  PolymarketHarvesterLive,
} from "./services/PolymarketHarvester.js";
// 1. 定义你的第一个 CLI 命令逻辑 (例如叫 sync 命令，未来用来同步全局数据)

const syncSubCommand = Command.make(
  "sync",
  // 我们顺手加一个命令行参数（比如过滤选项），体验一下正规 CLI 的快感
  {},
  // 核心执行逻辑
  () =>
    Effect.gen(function* () {
      yield* Console.log(`[pm-cli] 开始抓取市场数据...)`);
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

      yield* Console.log("[pm-cli] 读取数据成功");
    }),
);

const allSubCommands = Command.make("all", {}, () =>
  Effect.gen(function* () {
    const polymarketService = yield* PolymarketHarvester;
    const rawStream = polymarketService.fetchAll();
    yield* Effect.log(`抓取市场最新信息...`);
    const storage = yield* Storage;
    const rowStream = rawStream.pipe(
      Stream.filter(
        (market) =>
          !market.slug.includes("fifa") && !market.slug.includes("fifwc"),
      ),
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

// 2. 建立一个全局的主命令根节点 (Root Command)，把 sync 挂载为它的子命令
const rootCommand = Command.make("pm").pipe(
  Command.withSubcommands([syncSubCommand, allSubCommands]),
);

// 2. 将命令打包为标准的 CLI 应用程序
const cli = Command.run(rootCommand, {
  name: "Polymarket CLI Trader",
  version: "1.0.0",
});

// 3. 驱动主进程运行，并自动注入 Node.js 环境的整套大礼包（包含终端、文件系统、进程处理）
cli(process.argv).pipe(
  Effect.provide(PolymarketHarvesterLive),
  Effect.provide(PolymarketApiLive),
  Effect.provide(LocalStorageLive),
  Effect.provide(NodeContext.layer),
  Effect.provide(FetchHttpClient.layer),
  Effect.provide(NetworkLive),
  NodeRuntime.runMain,
);
