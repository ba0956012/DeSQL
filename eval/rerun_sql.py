"""
補跑腳本：把 API 回傳的 SQL 在本地 PG 上重新執行，補上 data 並重新 judge。

用法：
    python eval/rerun_sql.py --tag greg_evidence_compactdesc --db debit_card_specializing
    python eval/rerun_sql.py --tag greg_evidence_compactdesc --db debit_card_specializing --only-empty
"""

import argparse
import json
import glob
import os
import sys
import sqlite3
from pathlib import Path

EVAL_DIR = Path(__file__).parent
PROJECT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from dotenv import load_dotenv
load_dotenv(EVAL_DIR / ".env.eval", override=True)

from sqlalchemy import create_engine, text as sa_text

PG_BASE_URL = os.environ.get("PG_BASE_URL")
if not PG_BASE_URL:
    print("❌ 請在 .env.eval 設定 PG_BASE_URL")
    sys.exit(1)
DB_PREFIX = "bird_"


def run_sql_on_pg(db_id, sql):
    engine = create_engine(f"{PG_BASE_URL}/{DB_PREFIX}{db_id}")
    with engine.connect() as conn:
        rp = conn.execute(sa_text(sql))
        cols = list(rp.keys())
        rows = rp.fetchall()
    engine.dispose()
    return [dict(zip(cols, r)) for r in rows]


def run_gold_sql_on_sqlite(db_id, gold_sql):
    sqlite_path = EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"
    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.cursor()
    cursor.execute(gold_sql)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


def format_answer(question, data):
    from llm import llm as fmt_llm
    from langchain_core.messages import HumanMessage
    if not data:
        return "查無資料"
    data_str = json.dumps(data[:20], ensure_ascii=False, default=str)
    if len(data) > 20:
        data_str += f"\n... (共 {len(data)} 筆)"
    prompt = f"""你是一個資料分析助手。根據使用者的問題和查詢結果，用自然、易懂的繁體中文回答。
規則：
- 直接回答問題，不要提及 SQL、資料庫等技術細節
- 如果結果包含數字，適當加上單位或格式化
- 保持簡潔
使用者問題：{question}
查詢結果：{data_str}"""
    try:
        res = fmt_llm.invoke([HumanMessage(content=prompt)])
        return res.content.strip()
    except Exception:
        return json.dumps(data[:5], ensure_ascii=False, default=str)


def llm_judge(question, expected, actual_answer):
    from llm import llm as judge_llm
    from langchain_core.messages import HumanMessage
    if len(expected) == 1 and len(expected[0]) == 1:
        expected_str = str(list(expected[0].values())[0])
    elif len(expected) <= 10:
        expected_str = json.dumps(expected, ensure_ascii=False, default=str)
    else:
        expected_str = json.dumps(expected[:10], ensure_ascii=False, default=str)
        expected_str += f"\n... (共 {len(expected)} 筆，僅顯示前 10 筆)"
    prompt = f"""你是一個評測裁判。請判斷「系統回答」是否正確回答了「問題」。
判斷標準：
- 比對語意和數值是否一致，數值允許微小四捨五入差異
- 不要求格式完全一致，只要語意正確即可
- 部分正確但方向正確視為正確
問題：{question}
標準答案：{expected_str}
系統回答：{actual_answer}
只輸出純 JSON：
{{"correct": true/false, "reason": "簡短說明判斷理由"}}"""
    res = judge_llm.invoke([HumanMessage(content=prompt)])
    try:
        t = res.content.strip()
        if t.startswith("```"): t = t.split("\n", 1)[1]
        if t.endswith("```"): t = t[:-3]
        return json.loads(t.strip())
    except:
        return {"correct": False, "reason": f"parse error: {res.content[:200]}"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--only-empty", action="store_true", help="只補跑 data 為空的題目")
    args = parser.parse_args()

    pattern = str(EVAL_DIR / "results" / args.tag / f"{args.db}_*.json")
    files = sorted(glob.glob(pattern))
    print(f"找到 {len(files)} 筆 log\n")

    updated = 0
    for f in files:
        with open(f) as fh:
            d = json.load(fh)

        sql = d.get("pipeline_sql", "")
        if not sql:
            continue

        # 只補跑空結果的
        if args.only_empty:
            api_data = d.get("api_data", "")
            if api_data not in ("[]", "", "null"):
                continue

        qid = d["question_id"]
        print(f"[{qid}] {d['question'][:60]}")

        # 在本地 PG 重跑 SQL
        try:
            pg_data = run_sql_on_pg(args.db, sql)
        except Exception as e:
            print(f"  ⚠️ SQL 執行失敗: {e}")
            d["api_data"] = "[]"
            d["pipeline_answer"] = f"SQL 執行失敗: {e}"
            d["judge_correct"] = False
            d["judge_reason"] = f"SQL error: {e}"
            with open(f, "w", encoding="utf-8") as fh:
                json.dump(d, fh, ensure_ascii=False, indent=2)
            updated += 1
            continue

        # 取得 expected
        try:
            expected = run_gold_sql_on_sqlite(args.db, d["gold_sql"])
        except Exception as e:
            print(f"  ⚠️ Gold SQL 失敗: {e}")
            continue

        # format + judge
        answer = format_answer(d["question"], pg_data)
        verdict = llm_judge(d["question"], expected, answer)
        icon = "✅" if verdict.get("correct") else "❌"
        print(f"  Data: {len(pg_data)} rows → {icon} {verdict.get('reason', '')[:80]}")

        # 更新 log
        d["api_data"] = json.dumps(pg_data[:20], ensure_ascii=False, default=str)
        d["pipeline_answer"] = answer
        d["judge_correct"] = verdict.get("correct", False)
        d["judge_reason"] = verdict.get("reason", "")
        with open(f, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=2)
        updated += 1

    print(f"\n✅ 更新了 {updated} 筆 log")


if __name__ == "__main__":
    main()
