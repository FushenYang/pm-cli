jq -c '
  (if type == "array" then .[] else . end)
  | select(.event_type == "price_change")
  | .timestamp as $ts | .market as $mk
  | .price_changes[]
  | . + {timestamp: $ts, market: $mk}
' orderbook-20260624235536.jsonl > cleaned_price_changes.jsonl