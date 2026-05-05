"""
一次性產生 schema summary（compact_desc）

用法：
    python eval/generate_schema_summary.py --db california_schools
    python eval/generate_schema_summary.py --db financial
    python eval/generate_schema_summary.py --db debit_card_specializing
    python eval/generate_schema_summary.py --all   # 產生所有 DB 的 summary
"""

import argparse
import os
import sys
from pathlib import Path

EVAL_DIR = Path(__file__).parent
PROJECT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

# 保存 CLI 傳入的 LLM 環境變數
_cli_env_keys = ["LLM_PROVIDER", "LLM_MODEL", "LLM_DEPLOYMENT"]
_cli_env = {k: os.environ[k] for k in _cli_env_keys if k in os.environ}

from dotenv import load_dotenv

load_dotenv(EVAL_DIR / ".env.eval", override=True)

# 恢復 CLI 傳入的值
os.environ.update(_cli_env)

PG_BASE_URL = os.environ.get("PG_BASE_URL")
DB_PREFIX = "bird_"


def generate_for_db(db_id: str, model_tag: str = None):
    db_uri = f"{PG_BASE_URL}/{DB_PREFIX}{db_id}"
    os.environ["DATABASE_URL"] = db_uri

    # 載入 column_descs（直接實作，避免 import run_eval 觸發 pipeline 初始化）
    import csv

    column_descs = {}
    desc_dir = EVAL_DIR / "databases" / db_id / "database_description"
    if desc_dir.exists():
        for csv_file in sorted(desc_dir.glob("*.csv")):
            table_name = csv_file.stem.lower()
            with open(csv_file, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    col = row.get("original_column_name", "").strip().lower()
                    if not col:
                        continue
                    desc = row.get("column_description", "").strip()
                    val_desc = row.get("value_description", "").strip()
                    if not desc and not val_desc:
                        continue
                    text = desc
                    if val_desc and len(val_desc) < 200:
                        text = f"{desc} ({val_desc})" if desc else val_desc
                    column_descs[f"{table_name}.{col}"] = text

    if not column_descs:
        print(f"  ⚠️ No column descriptions found for {db_id}")
        return

    print(f"  Loading DB context for {db_id}...", flush=True)
    from db import build_db_context, generate_schema_summary

    eng, schema_info, enum_values = build_db_context(db_uri)

    print(
        f"  Generating summary ({len(column_descs)} column descs, {len(enum_values)} enums)...",
        flush=True,
    )
    result = generate_schema_summary(schema_info, column_descs, enum_values)

    # 檔名帶模型標記
    import json as _json

    if isinstance(result, dict):
        # 精煉的 column_descs JSON
        if model_tag:
            filename = f"column_descs_refined_{model_tag}.json"
        else:
            filename = "column_descs_refined.json"
        out_path = EVAL_DIR / "databases" / db_id / filename
        with open(out_path, "w", encoding="utf-8") as f:
            _json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  ✅ Saved to {out_path} ({len(result)} columns)")
        # Preview
        for k, v in list(result.items())[:5]:
            print(f"    {k}: {v[:80]}")
    else:
        # Fallback: compact text
        if model_tag:
            filename = f"description_compact_{model_tag}.txt"
        else:
            filename = "description_compact.txt"
        out_path = EVAL_DIR / "databases" / db_id / filename
        out_path.write_text(result, encoding="utf-8")
        print(f"  ✅ Saved to {out_path}")
        print(f"  Preview:\n{str(result)[:500]}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=None, help="DB name")
    parser.add_argument("--all", action="store_true", help="Generate for all eval DBs")
    parser.add_argument(
        "--model-tag",
        default=None,
        help="Model tag for filename (e.g., gpt41mini, qwen3)",
    )
    args = parser.parse_args()

    if args.all:
        dbs = ["california_schools", "debit_card_specializing", "financial"]
    elif args.db:
        dbs = [args.db]
    else:
        parser.print_help()
        return

    for db_id in dbs:
        print(f"\n📦 {db_id}", flush=True)
        generate_for_db(db_id, model_tag=args.model_tag)


if __name__ == "__main__":
    main()
