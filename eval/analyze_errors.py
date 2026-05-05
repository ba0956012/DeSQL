"""
錯題瓶頸分析：對照 log、pipeline SQL、Gold SQL，找出錯誤發生在哪個環節。

用法：
    python eval/analyze_errors.py
    python eval/analyze_errors.py --tag desql_41mini_cn_validate --db california_schools
"""

import argparse
import json
import glob
import sqlite3
import sys
from pathlib import Path
from collections import Counter

EVAL_DIR = Path(__file__).parent


def load_results(tag, db_filter=None):
    results = []
    for f in sorted(glob.glob(str(EVAL_DIR / "results" / tag / "*.json"))):
        with open(f) as fh:
            r = json.load(fh)
        if db_filter and r["db_id"] != db_filter:
            continue
        results.append(r)
    return results


def run_gold_sql(db_id, gold_sql):
    sqlite_path = EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"
    try:
        conn = sqlite3.connect(str(sqlite_path))
        cur = conn.cursor()
        cur.execute(gold_sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        conn.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return f"ERROR: {e}"


def classify_error(r, gold_result):
    """分類錯誤的環節"""
    pipeline_sql = r.get("pipeline_sql", "")
    pipeline_answer = r.get("pipeline_answer", "")
    gold_sql = r.get("gold_sql", "")
    judge_reason = r.get("judge_reason", "")
    expected = r.get("expected_result", [])

    # 1. Pipeline 完全失敗（無 SQL 產出）
    if (
        not pipeline_sql
        or "pipeline error" in judge_reason.lower()
        or "recursion" in judge_reason.lower()
    ):
        return "pipeline_crash", "Pipeline 崩潰或 recursion limit"

    # 2. SQL 執行錯誤
    if "SQL 執行錯誤" in pipeline_answer or "error" in pipeline_answer.lower()[:30]:
        return "sql_exec_error", "SQL 執行失敗"

    # 3. 查無資料
    if (
        "查無" in pipeline_answer
        or "無法" in pipeline_answer[:20]
        or "沒有" in pipeline_answer[:20]
        or "no data" in pipeline_answer.lower()[:30]
    ):
        return "empty_result", "SQL 結果為空（WHERE 條件太嚴格或值不匹配）"

    # 4. 比較 pipeline SQL 和 gold SQL 的結構差異
    p_sql_lower = pipeline_sql.lower()
    g_sql_lower = gold_sql.lower()

    # 檢查 JOIN 差異
    p_joins = p_sql_lower.count("join")
    g_joins = g_sql_lower.count("join")

    # 檢查 WHERE 差異
    p_has_where = "where" in p_sql_lower
    g_has_where = "where" in g_sql_lower

    # 5. 數值/計算錯誤（judge 說數值不符）
    if any(
        w in judge_reason
        for w in ["數值", "數量", "分數", "比例", "百分", "不一致", "不符"]
    ):
        if abs(p_joins - g_joins) >= 1:
            return (
                "wrong_join",
                f"JOIN 結構不同（pipeline {p_joins} joins vs gold {g_joins} joins），導致數值錯誤",
            )
        return "wrong_calculation", "SQL 欄位或計算邏輯錯誤"

    # 6. 回答不完整
    if any(w in judge_reason for w in ["不完整", "未包含", "缺少", "部分", "只列出"]):
        return "incomplete_answer", "回答不完整（可能 SQL 少了欄位或 Python 處理不當）"

    # 7. 欄位/實體選錯
    if any(w in judge_reason for w in ["不正確", "選錯", "混淆", "不符"]):
        return "wrong_entity", "選錯欄位或實體"

    return "other", f"其他: {judge_reason[:60]}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="desql_41mini_cn_validate")
    parser.add_argument("--db", default=None)
    parser.add_argument(
        "--difficulty", default=None, help="simple/moderate/challenging"
    )
    args = parser.parse_args()

    results = load_results(args.tag, args.db)
    wrong = [r for r in results if not r.get("judge_correct")]
    if args.difficulty:
        wrong = [r for r in wrong if r["difficulty"] == args.difficulty]

    print(f"分析 {args.tag} 的錯題（{len(wrong)} 題）\n")

    categories = Counter()
    details = []

    for r in wrong:
        gold_result = run_gold_sql(r["db_id"], r["gold_sql"])
        cat, desc = classify_error(r, gold_result)
        categories[cat] += 1

        details.append(
            {
                "db_id": r["db_id"],
                "question_id": r["question_id"],
                "difficulty": r["difficulty"],
                "question": r["question"][:60],
                "category": cat,
                "description": desc,
                "gold_sql_short": r["gold_sql"][:80],
                "pipeline_sql_short": r.get("pipeline_sql", "")[:80],
                "judge_reason": r.get("judge_reason", "")[:80],
            }
        )

    # 印出分類統計
    print("=== 錯誤環節分佈 ===")
    for cat, cnt in categories.most_common():
        pct = cnt / len(wrong) * 100
        print(f"  {cat:25s}: {cnt:3d} ({pct:.0f}%)")

    # 按 DB 分
    print(f"\n=== 按 DB 分佈 ===")
    db_cats = {}
    for d in details:
        key = d["db_id"]
        if key not in db_cats:
            db_cats[key] = Counter()
        db_cats[key][d["category"]] += 1
    for db in sorted(db_cats):
        print(f"\n  {db}:")
        for cat, cnt in db_cats[db].most_common():
            print(f"    {cat:25s}: {cnt}")

    # 印出每題明細
    print(f"\n=== 逐題明細 ===")
    for d in sorted(details, key=lambda x: (x["category"], x["db_id"])):
        print(
            f"\n[{d['category']}] {d['db_id']} #{d['question_id']} ({d['difficulty']})"
        )
        print(f"  Q: {d['question']}")
        print(f"  Desc: {d['description']}")
        print(f"  Gold SQL: {d['gold_sql_short']}")
        print(f"  Pipe SQL: {d['pipeline_sql_short']}")
        print(f"  Judge: {d['judge_reason']}")

    # 存 JSON
    output = {
        "summary": dict(categories.most_common()),
        "total_wrong": len(wrong),
        "details": details,
    }
    out_path = EVAL_DIR / "report" / "error_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n📄 報告: {out_path}")


if __name__ == "__main__":
    main()
