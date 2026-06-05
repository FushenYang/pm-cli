import { Command, Options } from "@effect/cli";
import {
  HttpClient,
  HttpClientRequest,
  FetchHttpClient,
  Path,
  FileSystem,
} from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Layer, Schema } from "effect";
import { NetworkLive } from "./infrastructure/NetworkLive.js";
import {
  type MarketSummary,
  MarketSummarySchema,
} from "./domain/MarketSummarySchema.js";
import { ConfigLive } from "./infrastructure/ConfigLive.js";

// 1. 定义你的第一个 CLI 命令逻辑 (例如叫 sync 命令，未来用来同步全局数据)
const syncSubCommand = Command.make(
  "sync",
  // 我们顺手加一个命令行参数（比如过滤选项），体验一下正规 CLI 的快感
  {
    unmoderated: Options.boolean("unmoderated").pipe(
      Options.withDefault(false),
    ),
  },
  // 核心执行逻辑
  ({ unmoderated }) =>
    Effect.gen(function* () {
      yield* Console.log(
        `[pm-cli] 开始抓取市场数据... (包含小众市场: ${unmoderated})`,
      );
      const baseClient = yield* HttpClient.HttpClient;

      const req = HttpClientRequest.get(
        "https://gamma-api.polymarket.com/markets",
      ).pipe(
        HttpClientRequest.setUrlParams({
          active: "true",
          limit: "1",
          closed: "false",
        }),
        HttpClientRequest.setHeader("Accept-Encoding", "identity"),
      );
      const response = yield* baseClient.execute(req);
      const rawData = yield* response.json;
      const rawList = rawData as unknown[];
      if (rawList.length === 0) {
        return yield* Effect.fail(
          new Error("Polymarket 接口今天居然没有返回任何数据！"),
        );
      }
      const oneRawMarket = rawList[0];
      yield* Console.log(
        "[pm-cli] 物理数据下载成功，正在启动 Schema 手术刀清洗提纯...",
      );

      const cleanMarket: MarketSummary =
        yield* Schema.decodeUnknown(MarketSummarySchema)(oneRawMarket);
      yield* Console.log("[pm-cli] 提纯成功！正在准备将其转换为工业 CSV 行...");

      const escapeCsv = (val: any) => `"${String(val).replace(/"/g, '""')}"`;

      const headers = Object.keys(cleanMarket).join(",");
      const rowValues = Object.values(cleanMarket).map(escapeCsv).join(",");
      const csvContent = `${headers}\n${rowValues}\n`;

      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const localDirPath = path.join(process.cwd(), ".local");
      yield* fs.makeDirectory(localDirPath, { recursive: true });
      const targetFilePath = path.join(localDirPath, "one.csv");
      yield* fs.writeFileString(targetFilePath, csvContent);

      yield* Console.log("[pm-cli] 读取数据成功");
    }),
);

// 2. 建立一个全局的主命令根节点 (Root Command)，把 sync 挂载为它的子命令
const rootCommand = Command.make("pm").pipe(
  Command.withSubcommands([syncSubCommand]),
);

// 2. 将命令打包为标准的 CLI 应用程序
const cli = Command.run(rootCommand, {
  name: "Polymarket CLI Trader",
  version: "1.0.0",
});

// 3. 驱动主进程运行，并自动注入 Node.js 环境的整套大礼包（包含终端、文件系统、进程处理）
cli(process.argv).pipe(
  Effect.provide(NodeContext.layer),
  Effect.provide(FetchHttpClient.layer),
  Effect.provide(NetworkLive),
  NodeRuntime.runMain,
);
