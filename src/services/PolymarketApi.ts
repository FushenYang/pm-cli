// src/services/PolymarketApi.ts
import { HttpClient, HttpClientRequest } from "@effect/platform";
import { Context, Effect, Layer, Option, Schema, Array } from "effect";
import {
  type MarketSummary,
  MarketSummarySchema,
} from "../domain/MarketSummarySchema.js";

export const FetchPageOptionsSchema = Schema.Struct({
  // limit 必须是整数，且范围严格锁定在 1 到 100 之间。默认值为 100
  limit: Schema.Int.pipe(
    Schema.between(1, 100),
    Schema.optionalWith({ default: () => 100 }),
  ),
  offset: Schema.Int.pipe(
    Schema.greaterThanOrEqualTo(0),
    Schema.optionalWith({ default: () => 0 }),
  ),
  active: Schema.Literal("true", "false", "all").pipe(
    Schema.optionalWith({ default: () => "true" as const }),
  ),
});
export type FetchPageOptions = Schema.Schema.Encoded<
  typeof FetchPageOptionsSchema
>;
export type HarvestOptions = Omit<FetchPageOptions, "limit" | "offset">;

// 🌟 1. 铸造高贵的“服务契约标签 (Service Tag)”
// 对外宣告：我是一个专门负责向 Polymarket 索要数据的核心服务
export class PolymarketApi extends Context.Tag("PolymarketApi")<
  PolymarketApi,
  {
    readonly fetchPage: (
      options?: FetchPageOptions,
    ) => Effect.Effect<
      Option.Option<Array.NonEmptyReadonlyArray<MarketSummary>>,
      never,
      never
    >;
  }
>() {}

// 🌟 2. 实现这个契约的工业图层 (Layer)
export const PolymarketApiLive = Layer.effect(
  PolymarketApi,
  Effect.gen(function* () {
    // 从当前运行大本营里，把干脏活的网络客户端实例揪出来
    const baseClient = yield* HttpClient.HttpClient;

    return {
      fetchPage: (options?: FetchPageOptions) =>
        Effect.gen(function* () {
          const validated = yield* Schema.decodeUnknown(FetchPageOptionsSchema)(
            options ?? {},
          );
          // A. 组装请求
          const req = HttpClientRequest.get(
            "https://gamma-api.polymarket.com/markets",
          ).pipe(
            HttpClientRequest.setUrlParams({
              active: validated.active,
              closed: "false",
              limit: String(validated.limit),
              offset: String(validated.offset),
            }),
            HttpClientRequest.setHeader("Accept-Encoding", "identity"),
          );

          // B. 执行网络请求并解析 json
          const response = yield* baseClient.execute(req);
          const rawData = yield* response.json;

          // C. 挂上 Schema 手术刀，对全页数组进行大批量提纯清洗
          const cleanPage = yield* Schema.decodeUnknown(
            Schema.Array(MarketSummarySchema),
          )(rawData);

          if (cleanPage.length > 0) {
            return Option.some(
              cleanPage as Array.NonEmptyReadonlyArray<MarketSummary>,
            );
          } else {
            return Option.none();
          }
        }).pipe(Effect.orDie),
    };
  }),
);
