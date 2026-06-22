import type { MarketSummary } from "../domain/MarketSummarySchema.js";

// 1. 独立导出表头，作为常量备用
export const CSV_HEADERS: ReadonlyArray<keyof MarketSummary> = [
  "id", "conditionId", "slug", "question", "groupItemTitle",
  "startDate", "endDate", "active", "closed", "volumeNum",
  "liquidityNum", "outcomePrices", "clobTokenIds", "events"
];

export const CSV_HEADER_ROW = CSV_HEADERS.join(",");

// 2. 纯粹的 Body 转换函数（不包含表头，不包含末尾换行符）
export const marketToCsvRow = (market: MarketSummary): string => {
  const escapeCsv = (val: unknown) => `"${String(val ?? "").replace(/"/g, '""')}"`;
  return CSV_HEADERS.map((key) => escapeCsv(market[key])).join(",") + "\n";
};