import { Schema } from "effect";

export const MarketSummarySchema = Schema.Struct({
  id: Schema.String,
  conditionId: Schema.String,
  slug: Schema.String,
  question: Schema.String,
  groupItemTitle: Schema.String.pipe(
    Schema.optionalWith({ default: () => "" }),
  ),
  startDate: Schema.String.pipe(Schema.optionalWith({ default: () => "" })),
  endDate: Schema.String.pipe(Schema.optionalWith({ default: () => "" })),
  active: Schema.Boolean,
  closed: Schema.Boolean,
  volume24hr: Schema.Number,
  volumeNum: Schema.Number.pipe(Schema.optionalWith({ default: () => 0 })),
  liquidityNum: Schema.Number.pipe(Schema.optionalWith({ default: () => 0 })),
  outcomePrices: Schema.String.pipe(Schema.optionalWith({ default: () => "" })),
  clobTokenIds: Schema.String,
  // 🌟 魔法：自动把嵌套的 events 数组拍平为单根字符串
  events: Schema.transform(
    Schema.Array(Schema.Struct({ title: Schema.String })),
    Schema.String,
    {
      decode: (events) => events[0]?.title ?? "未分类事件",
      encode: (title) => [{ title }],
    },
  ),
}).pipe(
  // 🌟 核心去噪：告诉解码器，如果 Polymarket 还喂了其他几十个垃圾字段，一律无情忽略，只留下上面定义的黄金字段
  Schema.annotations({ preserveUnknownKeys: false }),
);

// 自动推导出清洗后的完美 TypeScript 类型，供全盘代码使用！
export type MarketSummary = Schema.Schema.Type<typeof MarketSummarySchema>;
