// src/services/PolymarketHarvester.ts
import { Context, Layer, Stream, Option, Array, Effect } from "effect";
import { type HarvestOptions, PolymarketApi } from "./PolymarketApi.js";
import { type MarketSummary } from "../domain/MarketSummarySchema.js";

export class PolymarketHarvester extends Context.Tag("PolymarketHarvester")<
  PolymarketHarvester,
  {
    readonly fetchAll: (
      options?: HarvestOptions,
    ) => Stream.Stream<MarketSummary, never, never>;
  }
>() {}

export const PolymarketHarvesterLive = Layer.effect(
  PolymarketHarvester,
  Effect.gen(function* () {
    const api = yield* PolymarketApi;

    return {
      fetchAll: (options?: HarvestOptions) => {
        type Seed = {
          readonly offset: number;
          readonly limit: number;
          readonly active: "true" | "false" | "all";
        };
        type PageOutput = Option.Option<
          Array.NonEmptyReadonlyArray<MarketSummary>
        >;
        type paginateReturn = readonly [PageOutput, Option.Option<Seed>];

        const initialSeed: Seed = {
          offset: 0,
          limit: 100,
          active: options?.active ?? "true",
        };

        // 🌟 核心修正：使用一等公民异步翻页算子 Stream.paginateEffect！
        // 它天生就是为了吃进一个返回 Effect 容器的迭代函数而设计的！
        const chunkStream = Stream.paginateEffect(initialSeed, (currentSeed) =>
          Effect.gen(function* () {
            const maybePage = yield* api.fetchPage(currentSeed);

            if (Option.isNone(maybePage)) {
              return [maybePage, Option.none<Seed>()] as paginateReturn;
            }
            yield* Effect.log(
              `[PolymarketHarvester] ✅ 当前offset: ${currentSeed.offset} ,获取了 ${maybePage.value.length} 条数据`,
            );
            const nextSeed: Seed = {
              ...currentSeed,
              offset: currentSeed.offset + currentSeed.limit,
            };

            return [maybePage, Option.some(nextSeed)] as paginateReturn;
          }),
        );

        return chunkStream.pipe(
          Stream.map((maybePage) =>
            Option.match(maybePage, {
              onNone: () => Array.empty<MarketSummary>(),
              onSome: (page) => page,
            }),
          ),
          Stream.flatMap(Stream.fromIterable),
          Stream.orDie,
        );
      },
    };
  }),
);
