#!/bin/bash

# 1. 临时注入本地的 .env 变量
export $(cat .env | xargs)

# 2. 用一个括号把初始化 SQL 和你的 query.sql 文件的内容合并成一个大文本流，一脚踢给 duckdb
(
  echo "INSTALL httpfs;"
  echo "LOAD httpfs;"
  echo "SET s3_region='auto';"
  echo "SET s3_access_key_id='${RCLONE_CONFIG_R2_ACCESS_KEY_ID}';
  SET s3_secret_access_key='${RCLONE_CONFIG_R2_SECRET_ACCESS_KEY}';"
  echo "SET s3_endpoint='f12b69ba24dafd5d331cf364e76507f9.r2.cloudflarestorage.com';" # 💡 注意：s3_endpoint 最好去掉开头的 https://

  # 紧接着追加你核心逻辑脚本的内容
  cat query.sql
) | duckdb