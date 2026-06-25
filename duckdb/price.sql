SELECT
    timestamp,
    epoch_ms(timestamp::BIGINT) AS time,
    --market,
    --asset_id,
    price::DOUBLE AS price,
    side,
    best_bid::DOUBLE AS bid,
    best_ask::DOUBLE AS ask
FROM read_json_auto('.local/cleaned_price_changes.jsonl')