import { Schema } from "effect";

export const MarketSummarySchema = Schema.Struct({
  id: Schema.String,
  conditionId: Schema.String,
  slug: Schema.String,
  question: Schema.String,
  groupItemTitle: Schema.String,
  startDate: Schema.String,
  endDate: Schema.String,
  active: Schema.Boolean,
  closed: Schema.Boolean,
  volumeNum: Schema.Number,
  liquidityNum: Schema.Number,
  outcomePrices: Schema.String,
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
