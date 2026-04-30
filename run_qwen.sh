#!/bin/bash
# 用 Qwen3 模型啟動 Streamlit（與 benchmark v0.4.0 相同配置）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PROMPT_PROFILE=qwen3_en \
LLM_PROVIDER=bedrock \
LLM_MODEL=qwen.qwen3-next-80b-a3b \
SQL_LLM_PROVIDER=bedrock \
SQL_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0 \
CODE_LLM_PROVIDER=bedrock \
CODE_LLM_MODEL=qwen.qwen3-coder-30b-a3b-v1:0 \
streamlit run "$SCRIPT_DIR/app.py" --server.port 8501 --server.headless true
