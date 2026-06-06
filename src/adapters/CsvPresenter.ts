import { Array } from "effect";
import type { MarketSummary } from "../domain/MarketSummarySchema.js";

export const marketListToCsv = (
  markets: Array.NonEmptyArray<MarketSummary>,
): string => {
  // 1. 抓取第一条数据作为 CSV 提取表头的绝对依据
  const firstMarket = markets[0];
  const headers = Object.keys(firstMarket).join(",");

  // 2. 折衷版单元格转义手術刀（顺从你的当前折衷心智，不加多余逻辑）
  const escapeCsv = (val: unknown) => `"${String(val).replace(/"/g, '""')}"`;

  // 3. 将整页的 market 数组循环遍历转换为多行标准的 CSV 数据
  const rowsContent = markets
    .map((market) => Object.values(market).map(escapeCsv).join(","))
    .join("\n");

  return `${headers}\n${rowsContent}\n`;
};
