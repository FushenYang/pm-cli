import { Stream } from "effect";
import { type MarketSummary } from "../domain/MarketSummarySchema.js";
import { marketToCsvRow } from "./MarketSummaryCsv.js";

export const MarketEncoders = {
  // 转换为 JSONL 字节流
  toJsonl: (
    stream: Stream.Stream<MarketSummary, never, never>,
  ): Stream.Stream<Uint8Array, never, never> =>
    stream.pipe(
      Stream.map((market) => JSON.stringify(market) + "\n"),
      Stream.encodeText, // 将 string 转换为 Uint8Array
    ),

  // 转换为 CSV 字节流
  toCsv: (
    stream: Stream.Stream<MarketSummary, never, never>,
    header: string,
  ): Stream.Stream<Uint8Array, never, never> =>
    stream.pipe(
      Stream.map((market) => marketToCsvRow(market) + "\n"),
      // 可以通过 Stream.prepend 动态加上 Header
      Stream.encodeText,
    ),
};
