import { Context, Layer } from "effect";

// 🛠️ 1. 定义 TextDecoder 服务的身份 Tag（纯类型与标识）
export class TextDecoderService extends Context.Tag("TextDecoderService")<
  TextDecoderService,
  TextDecoder
>() {}

// 🛠️ 2. 实现全局单例的 Live Layer
// 只有在整个程序通电（Provide）的那一刻，它才会被无脑初始化一次，全局共享
export const TextDecoderLive = Layer.succeed(
  TextDecoderService,
  new TextDecoder(),
);
