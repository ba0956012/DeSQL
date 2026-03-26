"""
設定檔：從 .env 讀取所有環境變數
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Azure OpenAI
os.environ.setdefault("AZURE_OPENAI_API_KEY", os.getenv("AZURE_OPENAI_API_KEY", ""))
os.environ.setdefault("AZURE_OPENAI_ENDPOINT", os.getenv("AZURE_OPENAI_ENDPOINT", ""))
os.environ.setdefault(
    "OPENAI_API_VERSION", os.getenv("OPENAI_API_VERSION", "2024-12-01-preview")
)

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")
LLM_DEPLOYMENT = os.getenv("LLM_DEPLOYMENT", "gpt-4.1-mini")
LLM_TEMPERATURE = int(os.getenv("LLM_TEMPERATURE", "0"))

# PostgreSQL
DB_URI = os.getenv("DATABASE_URL", "")

# Token pricing (USD per 1M tokens)
PRICE_INPUT = float(os.getenv("PRICE_INPUT", "0.15"))
PRICE_OUTPUT = float(os.getenv("PRICE_OUTPUT", "0.60"))

# Debug
DEBUG = os.getenv("DEBUG", "true").lower() in ("true", "1", "yes")
