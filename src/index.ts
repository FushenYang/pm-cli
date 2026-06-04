import { Terminal } from "@effect/platform"
import { NodeRuntime, NodeTerminal } from "@effect/platform-node"
import { Effect,Console } from "effect"

const program = Effect.gen(function* () {
  const terminal = yield* Terminal.Terminal;
  const input = yield* terminal.readLine;
  yield* Console.log(`input: ${input}`);
}).pipe(Effect.forever);

program.pipe(Effect.provide(NodeTerminal.layer))
.pipe(NodeRuntime.runMain)
