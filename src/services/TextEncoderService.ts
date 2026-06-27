import { Context, Layer } from "effect";

export class TextEncoderService extends Context.Tag("TextEncoderService")<
  TextEncoderService,
  TextEncoder
>() {
  static readonly layer = Layer.succeed(TextEncoderService, new TextEncoder());
}
