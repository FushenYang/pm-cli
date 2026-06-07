import { Effect, Layer, Option } from "effect";
import { setGlobalDispatcher, ProxyAgent } from "undici";
import { AppConfig, ConfigLive } from "./ConfigLive.js";

const UndiciProxyInitLive = Layer.effectDiscard(
  Effect.gen(function* () {
    // 1. 拿到两面性的 Option 配置
    const config = yield* AppConfig;

    // 2. 🌟 核心修正：根据 Option 的物理状态，决定今天拉不拉代理大闸！
    yield* Option.match(config.proxyUrl, {
      // 🟢 情况 A：如果没配代理（None），打印一行日志，原地无痛裸连放行
      onNone: () =>
        Effect.log("[Network] 🛡️ 未检测到系统代理，正在以原生裸连模式启动..."),

      // 🔵 情况 B：如果检测到了代理（Some），此时得到的 url 是纯粹的 string！
      onSome: (url) =>
        Effect.sync(() => {
          // 彻底对齐原生库，扔进真实的 string 变量
          setGlobalDispatcher(new ProxyAgent(url));
        }).pipe(
          Effect.andThen(
            Effect.log(`[Network] 🚀 检测到系统代理！已成功物理对接到：${url}`),
          ),
        ),
    });
  }),
);

export const NetworkLive = UndiciProxyInitLive.pipe(
  Layer.provide(ConfigLive),
  Layer.orDie,
);
