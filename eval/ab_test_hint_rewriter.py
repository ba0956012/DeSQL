"""
Hint Rewriter A/B 測試腳本

從 3 個 DB 各抽 10 題（共 30 題），每題跑兩次 pipeline：
  A: 原始 hint（baseline）
  B: 改寫 hint（rewritten）

比較兩組正確率，輸出逐題對比報告。

用法：
    python eval/ab_test_hint_rewriter.py
    python eval/ab_test_hint_rewriter.py --limit 5   # 每個 DB 只抽 5 題（快速測試）
"""

import argparse
import json
import os
import random
import sqlite3
import sys
import time
from pathlib import Path

EVAL_DIR = Path(__file__).parent
PROJECT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from dotenv import load_dotenv
load_dotenv(EVAL_DIR / ".env.eval", override=True)

from openai import AzureOpenAI

PG_BASE_URL = os.environ.get("PG_BASE_URL")
DB_PREFIX = "bird_"

# Hint rewriter LLM client
_client = AzureOpenAI(
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_version=os.environ.get("OPENAI_API_VERSION", "2024-12-01-preview"),
)
_MODEL = os.environ.get("LLM_DEPLOYMENT", "gpt-4.1-mini")


def load_compact_desc(db_id):
    path = EVAL_DIR / "databases" / db_id / "description_compact.txt"
    return path.read_text(encoding="utf-8").strip() if path.exists() else ""


def load_schema_from_tables_json(db_id):
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


def rewrite_hint(question, hint, schema, compact_desc):
    prompt = f"""你是一個 SQL 查詢前處理助手。根據使用者問題、原始提示、資料庫 schema 和欄位描述，重寫出更精確的提示。

重寫規則：
1. 把 hint 中的概念對應到具體的表名和欄位名
2. 如果 hint 提到計算公式，明確寫出用哪個欄位
3. 如果術語可能被誤解，加上澄清
4. 保留原始 hint 的所有資訊
5. 不要猜測答案，只做概念→欄位的對應
6. 直接輸出改寫後的 hint，不要前綴或解釋

使用者問題：{question}
原始 Hint：{hint if hint else "(無)"}

資料庫 Schema：
{schema}

欄位描述：
{compact_desc}

改寫後的 Hint："""

    resp = _client.chat.completions.create(
        model=_MODEL, temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content.strip()


def run_gold_sql(db_id, gold_sql):
    sqlite_path = EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"
    conn = sqlite3.connect(str(sqlite_path))
    cur = conn.cursor()
    cur.execute(gold_sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, r)) for r in rows]


def run_pipeline(question, db_id):
    os.environ["DATABASE_URL"] = f"{PG_BASE_URL}/{DB_PREFIX}{db_id}"
    for mod_name in list(sys.modules.keys()):
        if mod_name in ("db", "config", "pipeline", "retrieval_subgraph") or mod_name.startswith("nodes"):
            del sys.modules[mod_name]
    from pipeline import app
    from logger import init_run_logger
    init_run_logger(question)
    merged = {}
    for event in app.stream({"question": question, "retry": 0}):
        for node_name, node_output in event.items():
            if isinstance(node_output, dict):
                merged.update(node_output)
    return merged


def llm_judge(question, expected, actual_answer):
    if not expected:
        return {"correct": False, "reason": "no expected result"}
    if len(expected) == 1 and len(expected[0]) == 1:
        expected_str = str(list(expected[0].values())[0])
    elif len(expected) <= 10:
        expected_str = json.dumps(expected, ensure_ascii=False, default=str)
    else:
        expected_str = json.dumps(expected[:10], ensure_ascii=False, default=str)
        expected_str += f"\n... (共 {len(expected)} 筆)"

    prompt = f"""你是一個評測裁判。請判斷「系統回答」是否正確回答了「問題」。

判斷標準：
- 比對語意和數值是否一致
- 數值允許微小的四捨五入差異
- 不要求格式完全一致
- 列表只要包含相同項目即可（順序不重要）
- 部分結果但方向正確，視為正確

問題：{question}
標準答案：{expected_str}
系統回答：{actual_answer}

只輸出純 JSON：
{{"correct": true/false, "reason": "簡短說明"}}"""

    resp = _client.chat.completions.create(
        model=_MODEL, temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.choices[0].message.content.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
    if text.endswith("```"):
        text = text[:-3]
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        return {"correct": False, "reason": f"parse error: {text[:100]}"}


def run_single(item, use_rewritten_hint=False):
    """跑單題，回傳 (correct, answer, reason)"""
    question = item["question"]
    hint = item.get("evidence", "")

    if use_rewritten_hint:
        schema = load_schema_from_tables_json(item["db_id"])
        compact = load_compact_desc(item["db_id"])
        hint = rewrite_hint(question, hint, schema, compact)

    if hint:
        question += f"\n(Hint: {hint})"

    try:
        expected = run_gold_sql(item["db_id"], item["SQL"])
    except Exception as e:
        return False, f"gold SQL error: {e}", str(e)

    try:
        result = run_pipeline(question, item["db_id"])
        answer = result.get("display_answer") or result.get("final_answer") or "無法回答"
    except Exception as e:
        return False, f"pipeline error: {e}", str(e)

    verdict = llm_judge(item["question"], expected, answer)
    return verdict.get("correct", False), answer, verdict.get("reason", "")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10, help="每個 DB 抽幾題")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    with open(EVAL_DIR / "dev.json") as f:
        all_questions = json.load(f)

    dbs = ["california_schools", "financial", "debit_card_specializing"]
    random.seed(args.seed)

    sample = []
    for db in dbs:
        candidates = [q for q in all_questions if q["db_id"] == db]
        n = min(args.limit, len(candidates))
        sample.extend(random.sample(candidates, n))

    print(f"🧪 A/B 測試：{len(sample)} 題（每 DB {args.limit} 題）")
    print(f"   每題跑 2 次 pipeline（A=原始 hint, B=改寫 hint）")
    print(f"   預計耗時 {len(sample) * 2 * 0.5:.0f}~{len(sample) * 2 * 1:.0f} 分鐘\n")

    results = []
    start = time.time()

    for i, item in enumerate(sample):
        db_id = item["db_id"]
        qid = item["question_id"]
        diff = item["difficulty"]
        print(f"[{i+1}/{len(sample)}] {db_id} #{qid} ({diff})")

        try:
            # A: baseline
            print(f"  A (原始 hint)...", end="", flush=True)
            a_correct, a_answer, a_reason = run_single(item, use_rewritten_hint=False)
            print(f" {'✅' if a_correct else '❌'}")
        except Exception as e:
            print(f" 💥 A 失敗: {e}")
            a_correct, a_answer, a_reason = False, f"crash: {e}", str(e)

        try:
            # B: rewritten hint
            print(f"  B (改寫 hint)...", end="", flush=True)
            b_correct, b_answer, b_reason = run_single(item, use_rewritten_hint=True)
            print(f" {'✅' if b_correct else '❌'}")
        except Exception as e:
            print(f" 💥 B 失敗: {e}")
            b_correct, b_answer, b_reason = False, f"crash: {e}", str(e)

        change = ""
        if not a_correct and b_correct:
            change = "🟢 翻正"
        elif a_correct and not b_correct:
            change = "🔴 翻錯"
        elif a_correct and b_correct:
            change = "⚪ 都對"
        else:
            change = "⚫ 都錯"
        print(f"  → {change}")

        results.append({
            "db_id": db_id, "question_id": qid, "difficulty": diff,
            "question": item["question"][:60],
            "a_correct": a_correct, "b_correct": b_correct, "change": change,
            "a_reason": a_reason, "b_reason": b_reason,
        })

    elapsed = time.time() - start

    # 統計
    a_total = sum(1 for r in results if r["a_correct"])
    b_total = sum(1 for r in results if r["b_correct"])
    flipped_good = sum(1 for r in results if not r["a_correct"] and r["b_correct"])
    flipped_bad = sum(1 for r in results if r["a_correct"] and not r["b_correct"])

    print(f"\n{'='*60}")
    print(f"📊 A/B 測試結果（{len(results)} 題，{elapsed:.0f}s）")
    print(f"{'='*60}")
    print(f"  A (原始 hint): {a_total}/{len(results)} ({a_total/len(results)*100:.1f}%)")
    print(f"  B (改寫 hint): {b_total}/{len(results)} ({b_total/len(results)*100:.1f}%)")
    print(f"  翻正（A錯→B對）: {flipped_good}")
    print(f"  翻錯（A對→B錯）: {flipped_bad}")
    print(f"  淨提升: {flipped_good - flipped_bad} 題 ({(flipped_good - flipped_bad)/len(results)*100:.1f}%)")
    print(f"{'='*60}")

    # 寫入詳細報告
    report_path = EVAL_DIR / "report" / "ab_test_hint_rewriter.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({
            "summary": {
                "total": len(results),
                "a_correct": a_total, "b_correct": b_total,
                "flipped_good": flipped_good, "flipped_bad": flipped_bad,
                "elapsed_seconds": elapsed,
            },
            "details": results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n📄 詳細報告: {report_path}")


if __name__ == "__main__":
    main()
