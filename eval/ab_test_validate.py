"""
Validate Node A/B 測試

跑 30 題（每 DB 10 題），用新的 validate prompt。
A 組（baseline）直接從之前的 desql_41mini_cn_validate 結果讀取。
B 組（新 validate）跑完整 pipeline。

用法：
    python eval/ab_test_validate.py
    python eval/ab_test_validate.py --limit 5
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

_client = AzureOpenAI(
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_version=os.environ.get("OPENAI_API_VERSION", "2024-12-01-preview"),
)
_MODEL = os.environ.get("LLM_DEPLOYMENT", "gpt-4.1-mini")


def run_gold_sql(db_id, gold_sql):
    sqlite_path = EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"
    conn = sqlite3.connect(str(sqlite_path))
    cur = conn.cursor()
    cur.execute(gold_sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, r)) for r in rows]


def run_pipeline(question, db_id, column_descs=None):
    os.environ["DATABASE_URL"] = f"{PG_BASE_URL}/{DB_PREFIX}{db_id}"
    for mod_name in list(sys.modules.keys()):
        if mod_name in (
            "db",
            "config",
            "pipeline",
            "retrieval_subgraph",
        ) or mod_name.startswith("nodes"):
            del sys.modules[mod_name]
    from pipeline import app
    from logger import init_run_logger

    init_run_logger(question)
    init_state = {"question": question, "retry": 0}
    if column_descs:
        init_state["column_descs"] = column_descs
    merged = {}
    for event in app.stream(init_state):
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
        model=_MODEL,
        temperature=0,
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


def load_baseline_result(db_id, qid):
    """從 desql_41mini_cn_validate 讀取 baseline 結果"""
    path = EVAL_DIR / "results" / "desql_41mini_cn_validate" / f"{db_id}_{qid}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit", type=int, default=14, help="每個 DB 抽幾題（預設 14，3 DB 共 42 題）"
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--full-desc", action="store_true", help="注入 BIRD 原始 CSV 欄位描述"
    )
    args = parser.parse_args()

    with open(EVAL_DIR / "dev.json") as f:
        all_questions = json.load(f)

    dbs = ["california_schools", "financial", "debit_card_specializing"]
    random.seed(args.seed)

    # 分層抽樣：從每個 DB 各抽 baseline 錯題和對題，比例各半
    import glob

    baseline_results = {}
    for f_path in sorted(
        glob.glob(str(EVAL_DIR / "results" / "desql_41mini_cn_validate" / "*.json"))
    ):
        with open(f_path) as fh:
            r = json.load(fh)
        baseline_results[(r["db_id"], r["question_id"])] = r

    sample = []
    for db in dbs:
        candidates = [q for q in all_questions if q["db_id"] == db]
        has_baseline = [
            q for q in candidates if (db, q["question_id"]) in baseline_results
        ]

        correct = [
            q
            for q in has_baseline
            if baseline_results[(db, q["question_id"])].get("judge_correct")
        ]
        wrong = [
            q
            for q in has_baseline
            if not baseline_results[(db, q["question_id"])].get("judge_correct")
        ]

        half = args.limit // 2
        n_wrong = min(half, len(wrong))
        n_correct = min(args.limit - n_wrong, len(correct))

        sample.extend(random.sample(wrong, n_wrong))
        sample.extend(random.sample(correct, n_correct))

    print(f"🧪 Validate A/B 測試（分層抽樣）：{len(sample)} 題")
    n_a_correct = sum(
        1
        for q in sample
        if baseline_results.get((q["db_id"], q["question_id"]), {}).get("judge_correct")
    )
    n_a_wrong = len(sample) - n_a_correct
    print(f"   抽樣分佈：{n_a_correct} 題原本對 + {n_a_wrong} 題原本錯")
    print(f"   A = baseline (desql_41mini_cn_validate)")
    print(f"   B = 新 validate prompt")
    print(f"   預計耗時 {len(sample) * 0.5:.0f}~{len(sample) * 1:.0f} 分鐘\n")

    # 載入 BIRD desc（如果 --full-desc）
    desc_cache = {}
    if args.full_desc:
        sys.path.insert(0, str(EVAL_DIR))
        from run_eval import load_column_descs

        for db in dbs:
            col_descs = load_column_descs(db)
            if col_descs:
                desc_cache[db] = col_descs

    results = []
    start = time.time()

    for i, item in enumerate(sample):
        db_id = item["db_id"]
        qid = item["question_id"]
        diff = item["difficulty"]
        print(f"[{i+1}/{len(sample)}] {db_id} #{qid} ({diff})", end="", flush=True)

        # A: baseline
        baseline = load_baseline_result(db_id, qid)
        a_correct = baseline.get("judge_correct", False) if baseline else False

        # B: new validate
        try:
            question = item["question"]
            if item.get("evidence"):
                question += f"\n(Hint: {item['evidence']})"
            schema_desc = desc_cache.get(db_id, "")
            result = run_pipeline(
                question,
                db_id,
                column_descs=schema_desc if isinstance(schema_desc, dict) else None,
            )
            answer = (
                result.get("display_answer") or result.get("final_answer") or "無法回答"
            )
            expected = run_gold_sql(db_id, item["SQL"])
            verdict = llm_judge(item["question"], expected, answer)
            b_correct = verdict.get("correct", False)
            b_reason = verdict.get("reason", "")
        except Exception as e:
            b_correct = False
            b_reason = f"crash: {e}"

        change = ""
        if not a_correct and b_correct:
            change = "🟢 翻正"
        elif a_correct and not b_correct:
            change = "🔴 翻錯"
        elif a_correct and b_correct:
            change = "⚪ 都對"
        else:
            change = "⚫ 都錯"

        a_icon = "✅" if a_correct else "❌"
        b_icon = "✅" if b_correct else "❌"
        print(f"  A:{a_icon} B:{b_icon} {change}")

        results.append(
            {
                "db_id": db_id,
                "question_id": qid,
                "difficulty": diff,
                "question": item["question"][:60],
                "a_correct": a_correct,
                "b_correct": b_correct,
                "change": change,
                "b_reason": b_reason,
            }
        )

    elapsed = time.time() - start

    a_total = sum(1 for r in results if r["a_correct"])
    b_total = sum(1 for r in results if r["b_correct"])
    flipped_good = sum(1 for r in results if not r["a_correct"] and r["b_correct"])
    flipped_bad = sum(1 for r in results if r["a_correct"] and not r["b_correct"])

    print(f"\n{'='*60}")
    print(f"📊 Validate A/B 測試結果（{len(results)} 題，{elapsed:.0f}s）")
    print(f"{'='*60}")
    print(
        f"  A (舊 validate): {a_total}/{len(results)} ({a_total/len(results)*100:.1f}%)"
    )
    print(
        f"  B (新 validate): {b_total}/{len(results)} ({b_total/len(results)*100:.1f}%)"
    )
    print(f"  翻正: {flipped_good}")
    print(f"  翻錯: {flipped_bad}")
    print(f"  淨提升: {flipped_good - flipped_bad} 題")
    print(f"{'='*60}")

    report_path = EVAL_DIR / "report" / "ab_test_validate.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "summary": {
                    "total": len(results),
                    "a_correct": a_total,
                    "b_correct": b_total,
                    "flipped_good": flipped_good,
                    "flipped_bad": flipped_bad,
                    "elapsed_seconds": elapsed,
                },
                "details": results,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"\n📄 報告: {report_path}")


if __name__ == "__main__":
    main()
