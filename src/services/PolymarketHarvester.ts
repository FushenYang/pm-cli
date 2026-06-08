// src/services/PolymarketHarvester.ts
import { Context, Layer, Stream, Option, Effect, Chunk } from "effect";
import { type FetchPageOptions, PolymarketApi } from "./PolymarketApi.js";
import { type MarketSummary } from "../domain/MarketSummarySchema.js";

export type HarvestOptions = Omit<FetchPageOptions, "limit" | "offset">;

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
        // 提取固定参数，不需要放进 Seed 里传来传去
        const limit = 100;
        const active = options?.active ?? "true";

        // 🌟 核心修正：使用 Stream.paginateChunkEffect
        // 它期待你每次返回一个 Chunk（数据块），然后它会自动在底层帮你打平成逐个发出的 Stream
        // 这里的 Seed 极度简化：只需要记录当前的 offset 即可（初始值为 0）
        return Stream.paginateChunkEffect(0, (currentOffset) =>
          Effect.gen(function* () {
            // 1. 调用底层 API（现在的 API 直接返回 ReadonlyArray）
            const page = yield* api.fetchPage({
              offset: currentOffset,
              limit,
              active,
            });

            yield* Effect.log(
              `[PolymarketHarvester] ✅ 当前offset: ${currentOffset} ,获取了 ${page.length} 条数据`
            );

            // 2. 将普通的原生数组转换为 Effect 高效的 Chunk 结构
            const chunk = Chunk.fromIterable(page);

            // 3. 判断终点：如果这一页数据为空，说明到底了，不返回 nextSeed
            if (page.length === 0) {
              // Option.none() 就是告诉流：工厂停工，流结束
              return [chunk, Option.none<number>()] as const;
            }

            // 4. 如果还有数据，算出下一页的 offset 传给流的下一次迭代
            const nextOffset = currentOffset + limit;
            return [chunk, Option.some(nextOffset)] as const;
          })
        );
      },
    };
  }),
);