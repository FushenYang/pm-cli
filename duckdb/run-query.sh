#!/bin/bash

# 1. 临时注入本地的 .env 变量
# export $(cat .env | xargs)

# 2. 用一个括号把初始化 SQL 和你的 query.sql 文件的内容合并成一个大文本流，一脚踢给 duckdb
(
  # 紧接着追加你核心逻辑脚本的内容
  cat ./duckdb/query.sql
) | duckdb