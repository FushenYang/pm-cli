SELECT 
    id,
    unnest(markets)->>'slug' AS slug
FROM read_json_auto('.local/next-leader-out-of-power-before-2027-no-orban.json');