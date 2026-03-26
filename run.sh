#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
streamlit run "$SCRIPT_DIR/app.py" --server.port 8501 --server.headless true
