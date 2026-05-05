"""
Hint Rewriter 離線測試腳本

從 moderate 錯題 + 隨機抽樣正確題，測試 hint rewriter 的效果。
不跑 pipeline，只看改寫後的 hint 品質。

用法：
    python eval/test_hint_rewriter.py
"""

import json
import os
import random
import sys
from pathlib import Path

EVAL_DIR = Path(__file__).parent
PROJECT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from dotenv import load_dotenv

load_dotenv(EVAL_DIR / ".env.eval", override=True)

from openai import AzureOpenAI

client = AzureOpenAI(
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_version=os.environ.get("OPENAI_API_VERSION", "2024-12-01-preview"),
)
MODEL = os.environ.get("LLM_DEPLOYMENT", "gpt-4.1-mini")


def load_compact_desc(db_id: str) -> str:
    path = EVAL_DIR / "databases" / db_id / "description_compact.txt"
    return path.read_text(encoding="utf-8").strip() if path.exists() else ""


def load_schema_from_tables_json(db_id: str) -> str:
    with open(EVAL_DIR / "dev_tables.json") as f:
        tables = json.load(f)
    entry = next((t for t in tables if t["db_id"] == db_id), None)
    if not entry:
        return ""
    lines = []
    for i, tbl in enumerate(entry["table_names_original"]):
        cols = [c[1] for c in entry["column_names_original"] if c[0] == i]
        lines.append(f"Table: {tbl} ({', '.join(cols)})")
    return "\n".join(lines)


def rewrite_hint(question: str, hint: str, schema: str, compact_desc: str) -> str:
    prompt = f"""你是一個 SQL 查詢前處理助手。你的任務是根據使用者問題、原始提示（hint）、資料庫 schema 和欄位描述，重寫出一段更精確的提示，幫助 SQL 生成模型正確理解問題。

重寫規則：
1. 把 hint 中提到的概念對應到具體的表名和欄位名（使用 schema 中的實際名稱）
2. 如果 hint 提到計算公式，明確寫出用哪個欄位除以哪個欄位
3. 如果 hint 中的術語可能被誤解（如 "K-12" 可能被誤認為篩選條件），加上澄清
4. 保留原始 hint 的所有資訊，不要遺漏
5. 不要猜測答案，只做「概念 → 欄位」的對應
6. 輸出格式：直接輸出改寫後的 hint 文字，不要加任何前綴或解釋

使用者問題：{question}

原始 Hint：{hint if hint else "(無)"}

資料庫 Schema：
{schema}

欄位描述：
{compact_desc}

改寫後的 Hint："""

    resp = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content.strip()


def main():
    with open(EVAL_DIR / "dev.json") as f:
        all_questions = json.load(f)

    # 載入 moderate 錯題
    import glob

    results = []
    for f in sorted(
        glob.glob(str(EVAL_DIR / "results" / "desql_41mini_cn_validate" / "*.json"))
    ):
        with open(f) as fh:
            results.append(json.load(fh))

    wrong_moderate = [
        r
        for r in results
        if r["difficulty"] == "moderate" and not r.get("judge_correct")
    ]
    correct_moderate = [
        r for r in results if r["difficulty"] == "moderate" and r.get("judge_correct")
    ]

    # 抽樣：5 題錯題 + 5 題正確題
    random.seed(42)
    sample_wrong = random.sample(wrong_moderate, min(5, len(wrong_moderate)))
    sample_correct = random.sample(correct_moderate, min(5, len(correct_moderate)))

    lines = ["# Hint Rewriter 測試結果\n"]

    for label, samples in [("❌ 錯題", sample_wrong), ("✅ 正確題", sample_correct)]:
        lines.append(f"\n## {label}\n")
        for r in samples:
            db_id = r["db_id"]
            qid = r["question_id"]
            q_item = next(
                q
                for q in all_questions
                if q["db_id"] == db_id and q["question_id"] == qid
            )

            schema = load_schema_from_tables_json(db_id)
            compact = load_compact_desc(db_id)

            print(f"Processing [{label}] {db_id} #{qid}...")
            rewritten = rewrite_hint(
                q_item["question"], q_item.get("evidence", ""), schema, compact
            )

            lines.append(f"### [{db_id}] Question #{qid}\n")
            lines.append(f"**題目:** {q_item['question']}\n")
            lines.append(f"**原始 Hint:** {q_item.get('evidence', '(無)')}\n")
            lines.append(f"**改寫 Hint:** {rewritten}\n")
            if label == "❌ 錯題":
                lines.append(f"**Gold SQL:** `{r.get('gold_sql', 'N/A')}`\n")
                lines.append(f"**Pipeline SQL:** `{r.get('pipeline_sql', 'N/A')}`\n")
                lines.append(f"**Judge 理由:** {r.get('judge_reason', '')}\n")
            lines.append("---\n")

    output_path = EVAL_DIR / "report" / "hint_rewriter_test.md"
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n✅ 結果寫入 {output_path}")


if __name__ == "__main__":
    main()
