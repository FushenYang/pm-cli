import { Command } from "@effect/cli";
import { FetchHttpClient } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { NetworkLive } from "./infrastructure/NetworkLive";
import { LocalStorageLive } from "./infrastructure/LocalStorageLive";
import { PolymarketApiLive } from "./services/PolymarketApi";

import { PolymarketHarvesterLive } from "./services/PolymarketHarvester";
import { syncSubCommand } from "./commands/sync";
import { allSubCommands } from "./commands/all";

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
