import { Context, Layer } from "effect";

export class TextDecoderService extends Context.Tag("TextDecoderService")<
  TextDecoderService,
  TextDecoder
>() {
  static readonly layer = Layer.succeed(TextDecoderService, new TextDecoder());
}
