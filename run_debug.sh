#!/bin/bash
# 跑指定的 eval 題目，顯示完整 log
# 用法:
#   ./run_debug.sh financial 108          # 跑 financial #108
#   ./run_debug.sh debit 1472            # 跑 debit #1472
#   ./run_debug.sh schools 53            # 跑 schools #53
#   ./run_debug.sh financial 108,141,172  # 跑多題

DB=${1:-financial}
IDS=${2:-108}

# Map short names
case "$DB" in
  debit) DB="debit_card_specializing" ;;
  schools) DB="california_schools" ;;
esac

export PROMPT_PROFILE=qwen3_en
export LLM_PROVIDER=bedrock
export LLM_MODEL=qwen.qwen3-next-80b-a3b
export SQL_LLM_PROVIDER=bedrock
export SQL_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0
export CODE_LLM_PROVIDER=bedrock
export CODE_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0
export DEBUG=true

IFS=',' read -ra ID_ARRAY <<< "$IDS"
for QID in "${ID_ARRAY[@]}"; do
  echo "=========================================="
  echo "Running: $DB #$QID"
  echo "=========================================="
  conda run -n Greg-text-to-sql env \
    PROMPT_PROFILE=$PROMPT_PROFILE \
    LLM_PROVIDER=$LLM_PROVIDER \
    LLM_MODEL=$LLM_MODEL \
    SQL_LLM_PROVIDER=$SQL_LLM_PROVIDER \
    SQL_LLM_MODEL=$SQL_LLM_MODEL \
    CODE_LLM_PROVIDER=$CODE_LLM_PROVIDER \
    CODE_LLM_MODEL=$CODE_LLM_MODEL \
    DEBUG=true \
    python eval/run_eval.py --db "$DB" --id "$QID" --full-desc --desc-tag gpt41mini_v9 --tag debug_run
  
  echo ""
  echo "--- Log file ---"
  # Find the most recent log for this question
  LATEST_LOG=$(ls -t logs/pipeline_*"${QID}"* 2>/dev/null | head -1)
  if [ -n "$LATEST_LOG" ]; then
    echo "$LATEST_LOG"
    echo "---"
    cat "$LATEST_LOG"
  else
    echo "(no log found)"
  fi
  echo ""
done
