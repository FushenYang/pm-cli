import { Command, Options } from "@effect/cli"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Console, Effect } from "effect"

// 1. 定义你的第一个 CLI 命令逻辑 (例如叫 sync 命令，未来用来同步全局数据)
const syncSubCommand = Command.make(
  "sync",
  // 我们顺手加一个命令行参数（比如过滤选项），体验一下正规 CLI 的快感
  { unmoderated: Options.boolean("unmoderated").pipe(Options.withDefault(true)) },
  // 核心执行逻辑
  ({ unmoderated }) => Effect.gen(function* () {
    yield* Console.log(`[pm-cli] 开始抓取市场数据... (包含小众市场: ${unmoderated})`)

    // 【未来这一步】：我们会在这里调用 HttpClient 去盘 Polymarket
    yield* Effect.sleep("1 second") // 模拟一下抓取耗时

    yield* Console.log("[pm-cli] 静态数据同步成功，本地 SQLite 账本已刷新！")
  })
)

// 2. 建立一个全局的主命令根节点 (Root Command)，把 sync 挂载为它的子命令
const rootCommand = Command.make("pm").pipe(
  Command.withSubcommands([syncSubCommand])
)

// 2. 将命令打包为标准的 CLI 应用程序
const cli = Command.run(rootCommand, {
  name: "Polymarket CLI Trader",
  version: "1.0.0"
})

// 3. 驱动主进程运行，并自动注入 Node.js 环境的整套大礼包（包含终端、文件系统、进程处理）
cli(process.argv).pipe(
  Effect.provide(NodeContext.layer),
  NodeRuntime.runMain
)