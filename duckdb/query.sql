--SELECT count(*) FROM 's3://polymarket-cold-data/polymarket-cold-data/polymarket/order_filled/asset=100047051626368770751428911222043758145300953074613057308429094593654283636050/*.parquet';
--SELECT file_name, file_offset
--FROM parquet_metadata('s3://polymarket-cold-data/polymarket-cold-data/polymarket/order_filled/asset=100047051626368770751428911222043758145300953074613057308429094593654283636050/*.parquet')
--LIMIT 20;
-- query.sql
-- 确保开启必要的安全及高级网络选项
SET s3_use_ssl=true;

-- 使用 COPY 语法将查询流直接导出到本地物理文件
-- COPY (
--   SELECT
--     asset,
--     price,
--     amount_shares,
--     amount_usdc,
--     side,
--     order_type,
--     epoch_ms(block_timestamp * 1000) as timestamp,
--     transaction_hash
--   FROM 's3://polymarket-cold-data/polymarket-cold-data/polymarket/order_filled/asset=100047051626368770751428911222043758145300953074613057308429094593654283636050/*.parquet'
--   ORDER BY block_timestamp ASC
-- ) TO 'asset_100047_output.csv' WITH (HEADER 1, DELIMITER ',');

-- 统计全量 R2 桶里，成交笔数最多、涉及资金最大的明星资产
COPY (
SELECT
    asset,
    COUNT(*) as trade_count,                             -- 总成交笔数
    ROUND(SUM(amount_usdc)) as total_volume_usdc,         -- 总成交额 (USDC)
    MIN(price) as min_price,                             -- 历史最低价
    MAX(price) as max_price                              -- 历史最高价
FROM 's3://polymarket-cold-data/polymarket-cold-data/polymarket/order_filled/asset=112998834930140849739317686306046962022222428099467375077336504369963794934957/*.parquet'
GROUP BY asset
ORDER BY trade_count DESC
LIMIT 10) TO 'us-x-iran-permanent-peace-deal-by-no.csv' WITH (HEADER 1, DELIMITER ',');