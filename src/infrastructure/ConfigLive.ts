// src/infrastructure/ConfigLive.ts
import { Context, Layer, Option, Config, Effect } from "effect";

export class AppConfig extends Context.Tag("AppConfig")<
  AppConfig,
  { readonly proxyUrl: Option.Option<string> }
>() {}

export const ConfigLive = Layer.effect(
  AppConfig,
  Effect.gen(function* () {
    // 🌟 核心修正：1. 纯粹地组合出我们完美的平台无关配置大转盘（此时它是个 Config 描述）
    const proxyConfigBlueprint = Config.string("all_proxy").pipe(
      Config.orElse(() => Config.string("HTTP_PROXY")),
      Config.option,
    );

    const maybeProxy = yield* Effect.configProviderWith((provider) =>
      provider.load(proxyConfigBlueprint),
    );

    return {
      proxyUrl: maybeProxy, // 此时 maybeProxy 的类型是完美的 Option<string>，红线灰飞烟灭！
    };
  }),
);
