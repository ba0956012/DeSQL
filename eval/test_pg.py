"""
測試本地 PostgreSQL 連線
用法：python eval/test_pg.py
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parent / ".env.eval", override=True)

# 從 .env.eval 讀取，或使用命令列參數覆蓋
PG_BASE_URL = os.environ.get("PG_BASE_URL")
if not PG_BASE_URL:
    print("❌ 請在 .env.eval 設定 PG_BASE_URL")
    sys.exit(1)
DEFAULT_URL = f"{PG_BASE_URL}/postgres"

url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL

print(f"連線：{url}")
try:
    engine = create_engine(url)
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version()")).scalar()
        print(f"✅ 連線成功")
        print(f"   版本：{version}")

        # 列出現有 schema
        schemas = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata ORDER BY 1")
        ).fetchall()
        print(f"   Schemas：{[s[0] for s in schemas]}")
    engine.dispose()
except Exception as e:
    print(f"❌ 連線失敗：{e}")
    sys.exit(1)
