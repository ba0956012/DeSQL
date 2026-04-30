#!/bin/bash
# 啟動 Eval Debug UI
# 用法:
#   ./run_eval_debug.sh          # 互動選擇 DB
#   ./run_eval_debug.sh financial  # 直接指定

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_BASE_URL="postgresql+psycopg2://postgres:postgres@localhost:5432"

if [ -z "$1" ]; then
  echo "🔬 Eval Debug — 選擇資料庫:"
  echo "  1) california_schools"
  echo "  2) financial"
  echo "  3) debit_card_specializing"
  read -p "請選擇 (1/2/3): " choice
  case "$choice" in
    1) DB="california_schools" ;;
    2) DB="financial" ;;
    3) DB="debit_card_specializing" ;;
    *) echo "無效選擇"; exit 1 ;;
  esac
else
  DB="$1"
  case "$DB" in
    debit) DB="debit_card_specializing" ;;
    schools) DB="california_schools" ;;
  esac
fi

echo "🔬 Starting with DB: $DB"

PROMPT_PROFILE=qwen3_en \
LLM_PROVIDER=bedrock \
LLM_MODEL=qwen.qwen3-next-80b-a3b \
SQL_LLM_PROVIDER=bedrock \
SQL_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0 \
CODE_LLM_PROVIDER=bedrock \
CODE_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0 \
ENABLE_CHART=false \
DOMAIN_RULES=false \
DATABASE_URL="${PG_BASE_URL}/bird_${DB}" \
PG_BASE_URL="${PG_BASE_URL}" \
BEDROCK_API_TOKEN="${BEDROCK_API_TOKEN}" \
streamlit run "$SCRIPT_DIR/app_eval.py" --server.port 8502 --server.headless true
