#!/bin/bash

SCRIPT_NAME=$1

# 1. 如果没有传参数，启动“智能寻航”模式：自动寻找最新修改的 .sql 文件
if [ -z "$SCRIPT_NAME" ]; then
  echo "🔍 未指定脚本名，正在查找最近修改的 SQL 文件..."
  
  # 寻找 ./duckdb 目录下最新修改的 .sql 文件名（去掉路径和后缀）
  # ls -t 按时间从新到旧排序，head -n 1 取第一个
  LATEST_SQL=$(ls -t ./duckdb/*.sql 2>/dev/null | head -n 1)

  if [ -z "$LATEST_SQL" ]; then
    echo "❌ 错误: ./duckdb 文件夹下没有找到任何 .sql 文件！"
    exit 1
  fi

  # 从相对路径中提取出纯文件名（例如把 ./duckdb/a.sql 变成 a）
  SCRIPT_NAME=$(basename "$LATEST_SQL" .sql)
  echo "💡 智能匹配到最新修改的脚本: ${SCRIPT_NAME}.sql"
fi

SCRIPT_PATH="./duckdb/${SCRIPT_NAME}.sql"

# 2. 检查该 SQL 文件是否存在（防止手动输入错误的文件名）
if [ ! -f "$SCRIPT_PATH" ]; then
  echo "❌ 错误: 找不到 SQL 文件: $SCRIPT_PATH"
  exit 1
fi

echo "🚀 正在使用 DuckDB 执行: $SCRIPT_PATH ..."

# 3. 如果需要注入环境变量，可以把解冻代码写在这里
# export $(cat .env | xargs) 2>/dev/null

# 4. 执行指定的 SQL 脚本，并透传后续可能存在的动态参数（如 -markdown）
cat "$SCRIPT_PATH" | duckdb "${@:2}"