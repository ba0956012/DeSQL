"""
評測 deepeye-sql 結果（predicted SQL）在 BIRD dev set 上的準確度。

用法：
    python eval/eval_deepeye.py --model gemma3-27b
    python eval/eval_deepeye.py --model gemma3-27b --db california_schools
    python eval/eval_deepeye.py --model all
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from collections import Counter

EVAL_DIR = Path(__file__).parent
PROJECT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from dotenv import load_dotenv

load_dotenv(EVAL_DIR / ".env.eval", override=True)

# 我們測試的 3 個 DB
TARGET_DBS = ["california_schools", "financial", "debit_card_specializing"]


def run_sql_on_sqlite(db_id: str, sql: str) -> list:
    """在 SQLite 上執行 SQL，回傳 list of dict"""
    sqlite_path = EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"
    if not sqlite_path.exists():
        return []
    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        conn.close()
        raise e


def format_sql_result(question: str, sql_result: list) -> str:
    """把 SQL 執行結果轉成自然語言回答（和 DeSQL 的 format_answer 對齊）"""
    from llm import llm as fmt_llm
    from langchain_core.messages import HumanMessage

    if not sql_result:
        return "查無資料"

    if len(sql_result) <= 10:
        result_str = json.dumps(sql_result, ensure_ascii=False, default=str)
    else:
        result_str = json.dumps(sql_result[:10], ensure_ascii=False, default=str)
        result_str += f"\n... (共 {len(sql_result)} 筆)"

    prompt = f"""你是一個資料分析助手。根據使用者的問題和查詢結果，用自然、易懂的繁體中文回答。

規則：
- 直接回答問題，不要提及 SQL、資料庫等技術細節
- 必須保留具體數值，不要用模糊描述
- 如果結果是列表，列出所有項目（或至少前 20 筆）
- 如果結果是單一數值，直接回答該數值
- 保持簡潔但完整

使用者問題：{question}
查詢結果：{result_str}
"""
    res = fmt_llm.invoke([HumanMessage(content=prompt)])
    return res.content.strip()


def llm_judge(question: str, expected: list, actual_answer: str) -> dict:
    """用 LLM 比對自然語言回答和 gold SQL 結果"""
    from llm import llm as judge_llm
    from langchain_core.messages import HumanMessage

    # 格式化 expected
    if len(expected) == 1 and len(expected[0]) == 1:
        expected_str = str(list(expected[0].values())[0])
    elif len(expected) <= 10:
        expected_str = json.dumps(expected, ensure_ascii=False, default=str)
    else:
        expected_str = json.dumps(expected[:10], ensure_ascii=False, default=str)
        expected_str += f"\n... (共 {len(expected)} 筆，僅顯示前 10 筆)"

    prompt = f"""你是一個評測裁判。請判斷「系統回答」是否正確回答了「問題」。

判斷標準：
- 比對「系統回答」和「標準答案」的語意和數值是否一致
- 數值允許微小的四捨五入差異
- 不要求格式完全一致，只要語意正確即可
- 如果標準答案是一個列表，系統回答只要包含相同的項目即可（順序不重要）
- 如果系統回答只列出部分結果但方向正確（如標準答案有 10 筆，系統回答列了前 5 筆且都正確），視為正確

問題：{question}
標準答案：{expected_str}
系統回答：{actual_answer}

只輸出純 JSON：
{{"correct": true/false, "reason": "簡短說明判斷理由"}}"""

    res = judge_llm.invoke([HumanMessage(content=prompt)])
    try:
        text = res.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        return json.loads(text.strip())
    except (json.JSONDecodeError, KeyError):
        return {"correct": False, "reason": f"parse error: {res.content[:200]}"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model", required=True, help="Model name (e.g. gemma3-27b) or 'all'"
    )
    parser.add_argument("--db", default=None, help="Only evaluate this DB")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    # 載入 dev.json
    with open(EVAL_DIR / "dev.json") as f:
        all_questions = json.load(f)

    # 決定要跑哪些模型
    deepeye_dir = EVAL_DIR / "deepeye-sql"
    if args.model == "all":
        model_files = sorted(deepeye_dir.glob("*.json"))
    else:
        model_files = [deepeye_dir / f"{args.model}.json"]

    target_dbs = [args.db] if args.db else TARGET_DBS

    for model_file in model_files:
        model_name = model_file.stem
        print(f"\n{'='*60}")
        print(f"📊 Model: {model_name}")
        print(f"{'='*60}")

        with open(model_file) as f:
            predictions = json.load(f)

        for db_id in target_dbs:
            # 篩選該 DB 的題目
            candidates = [q for q in all_questions if q["db_id"] == db_id]
            if args.limit:
                candidates = candidates[: args.limit]

            # 結果目錄
            result_dir = EVAL_DIR / "results" / f"deepeye_{model_name}"
            result_dir.mkdir(parents=True, exist_ok=True)

            print(f"\n  {db_id} ({len(candidates)} 題)")
            results = []

            for i, item in enumerate(candidates):
                qid = item["question_id"]
                log_path = result_dir / f"{db_id}_{qid}.json"

                # 跳過已完成
                if log_path.exists():
                    with open(log_path) as fh:
                        existing = json.load(fh)
                    results.append({"correct": existing.get("judge_correct", False)})
                    continue

                pred_sql = predictions.get(str(qid), "")
                if not pred_sql:
                    verdict = {"correct": False, "reason": "no prediction"}
                    results.append(verdict)
                    _save(log_path, item, "", [], pred_sql, [], verdict)
                    print(f"    [{i+1}/{len(candidates)}] #{qid} ⚠️ no prediction")
                    continue

                # 執行 gold SQL
                try:
                    expected = run_sql_on_sqlite(db_id, item["SQL"])
                except Exception as e:
                    verdict = {"correct": False, "reason": f"gold SQL error: {e}"}
                    results.append(verdict)
                    _save(log_path, item, str(e), [], pred_sql, [], verdict)
                    print(f"    [{i+1}/{len(candidates)}] #{qid} ⚠️ gold SQL error")
                    continue

                # 執行 predicted SQL
                try:
                    actual = run_sql_on_sqlite(db_id, pred_sql)
                except Exception as e:
                    verdict = {"correct": False, "reason": f"predicted SQL error: {e}"}
                    results.append(verdict)
                    _save(log_path, item, "", expected, pred_sql, [], verdict)
                    icon = "❌"
                    print(
                        f"    [{i+1}/{len(candidates)}] #{qid} {icon} SQL error: {str(e)[:50]}"
                    )
                    continue

                # Format + LLM Judge
                answer = format_sql_result(item["question"], actual)
                verdict = llm_judge(item["question"], expected, answer)
                results.append(verdict)
                _save(log_path, item, "", expected, pred_sql, actual, verdict, answer)

                icon = "✅" if verdict.get("correct") else "❌"
                print(
                    f"    [{i+1}/{len(candidates)}] #{qid} ({item['difficulty']}) {icon}"
                )

            # 統計
            total = len(results)
            correct = sum(1 for r in results if r.get("correct"))
            print(f"\n  {db_id}: {correct}/{total} ({correct/total*100:.1f}%)")

            by_diff = {}
            for item, result in zip(candidates, results):
                diff = item["difficulty"]
                by_diff.setdefault(diff, {"total": 0, "correct": 0})
                by_diff[diff]["total"] += 1
                if result.get("correct"):
                    by_diff[diff]["correct"] += 1
            for diff in ["simple", "moderate", "challenging"]:
                if diff in by_diff:
                    d = by_diff[diff]
                    pct = d["correct"] / d["total"] * 100 if d["total"] else 0
                    print(f"    {diff:12s}: {d['correct']}/{d['total']} ({pct:.1f}%)")


def _save(path, item, error, expected, pred_sql, actual, verdict, answer=""):
    log = {
        "question_id": item["question_id"],
        "db_id": item["db_id"],
        "difficulty": item["difficulty"],
        "question": item["question"],
        "evidence": item.get("evidence", ""),
        "gold_sql": item["SQL"],
        "expected_result": expected[:10] if expected else [],
        "predicted_sql": pred_sql,
        "predicted_result": actual[:10] if actual else [],
        "predicted_row_count": len(actual) if actual else 0,
        "pipeline_answer": answer,
        "judge_correct": verdict.get("correct", False),
        "judge_reason": verdict.get("reason", ""),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2, default=str)


if __name__ == "__main__":
    main()
