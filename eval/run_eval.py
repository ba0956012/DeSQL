"""
BIRD-SQL 評測腳本

用法：
    python eval/run_eval.py --db california_schools --id 0       # 單題
    python eval/run_eval.py --db california_schools              # 該 DB 全部
    python eval/run_eval.py --db california_schools --limit 10   # 前 10 題
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from collections import Counter

# 設定 eval 目錄
EVAL_DIR = Path(__file__).parent
PROJECT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from dotenv import load_dotenv
load_dotenv(EVAL_DIR / ".env.eval", override=True)

PG_BASE_URL = os.environ.get("PG_BASE_URL")
if not PG_BASE_URL:
    print("❌ 請在 .env.eval 設定 PG_BASE_URL")
    sys.exit(1)
DB_PREFIX = "bird_"


def load_db_description(db_id: str, question: str = None) -> str:
    """載入欄位描述。優先使用手寫的精簡版，否則從 CSV 自動生成。"""
    # 優先用精簡版
    compact_path = EVAL_DIR / "databases" / db_id / "description_compact.txt"
    if compact_path.exists():
        return compact_path.read_text(encoding="utf-8").strip()

    # fallback: 從 CSV 自動生成（帶關鍵字篩選）
    desc_dir = EVAL_DIR / "databases" / db_id / "database_description"
    if not desc_dir.exists():
        return ""
    import csv
    import re

    keywords = set()
    if question:
        stop_words = {"the", "a", "an", "is", "are", "was", "were", "of", "in", "for",
                      "to", "and", "or", "that", "which", "what", "how", "many", "much",
                      "with", "from", "by", "on", "at", "has", "have", "do", "does",
                      "all", "each", "every", "than", "more", "less", "most", "least",
                      "between", "their", "its", "not", "no", "please", "list", "give",
                      "name", "number", "find", "show", "tell", "provide", "state"}
        words = re.findall(r'[a-zA-Z]+', question.lower())
        keywords = {w for w in words if w not in stop_words and len(w) > 2}

    all_lines = []
    for csv_file in sorted(desc_dir.glob("*.csv")):
        table_name = csv_file.stem
        table_lines = []
        with open(csv_file, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                col = row.get("original_column_name", "")
                desc = row.get("column_description", "")
                val_desc = row.get("value_description", "")
                if not desc and not val_desc:
                    continue
                if keywords:
                    text_to_match = f"{col} {desc} {val_desc}".lower()
                    if not any(kw in text_to_match for kw in keywords):
                        continue
                parts = [f"  {col}"]
                if desc:
                    parts.append(f": {desc}")
                if val_desc and len(val_desc) < 200:
                    parts.append(f" ({val_desc})")
                table_lines.append("".join(parts))
        if table_lines:
            all_lines.append(f"Table: {table_name}")
            all_lines.extend(table_lines)
            all_lines.append("")
    return "\n".join(all_lines)


def load_full_description(db_id: str) -> str:
    """載入完整的 BIRD CSV 欄位描述（欄位名轉小寫）"""
    desc_dir = EVAL_DIR / "databases" / db_id / "database_description"
    if not desc_dir.exists():
        return ""
    import csv
    lines = []
    for csv_file in sorted(desc_dir.glob("*.csv")):
        table_name = csv_file.stem.lower()
        lines.append(f"Table: {table_name}")
        with open(csv_file, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                col = row.get("original_column_name", "").lower()
                desc = row.get("column_description", "")
                val_desc = row.get("value_description", "")
                if desc or val_desc:
                    parts = [f"  {col}"]
                    if desc:
                        parts.append(f": {desc}")
                    if val_desc and len(val_desc) < 300:
                        parts.append(f" ({val_desc})")
                    lines.append("".join(parts))
        lines.append("")
    return "\n".join(lines)


def load_column_descs(db_id: str) -> dict:
    """載入 BIRD CSV 欄位描述為 dict，格式: {table.column: "desc (val_desc)"}"""
    desc_dir = EVAL_DIR / "databases" / db_id / "database_description"
    if not desc_dir.exists():
        return {}
    import csv
    result = {}
    for csv_file in sorted(desc_dir.glob("*.csv")):
        table_name = csv_file.stem.lower()
        with open(csv_file, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
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
                result[f"{table_name}.{col}"] = text
    return result


def llm_summarize_desc(question: str, desc: str) -> str:
    """用 LLM 根據問題和靜態 desc 生成注意事項，附加在 desc 後面"""
    from llm import llm as sum_llm
    from langchain_core.messages import HumanMessage

    prompt = f"""根據使用者問題和以下資料庫欄位描述，列出回答這個問題時需要特別注意的事項。

注意事項應包含：
- 這個問題應該用哪些表和欄位
- 容易選錯的欄位（如名稱相似但意義不同的欄位）
- 篩選條件應該用哪個欄位（不要猜，根據描述判斷）

使用者問題：{question}

欄位描述：
{desc}

只輸出 2-3 行注意事項，不要重複描述內容："""

    try:
        res = sum_llm.invoke([HumanMessage(content=prompt)])
        return res.content.strip()
    except Exception:
        return ""


def run_gold_sql_on_sqlite(db_id: str, gold_sql: str) -> list:
    sqlite_path = EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"
    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.cursor()
    cursor.execute(gold_sql)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


def run_pipeline(question: str, db_id: str) -> dict:
    os.environ["DATABASE_URL"] = f"{PG_BASE_URL}/{DB_PREFIX}{db_id}"

    for mod_name in list(sys.modules.keys()):
        if mod_name in ("db", "pipeline", "retrieval_subgraph") or mod_name.startswith("nodes"):
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


def run_pipeline_with_state(init_state: dict, db_id: str) -> dict:
    os.environ["DATABASE_URL"] = f"{PG_BASE_URL}/{DB_PREFIX}{db_id}"

    for mod_name in list(sys.modules.keys()):
        if mod_name in ("db", "pipeline", "retrieval_subgraph") or mod_name.startswith("nodes"):
            del sys.modules[mod_name]

    from pipeline import app
    from logger import init_run_logger
    init_run_logger(init_state["question"])

    merged = {}
    for event in app.stream(init_state):
        for node_name, node_output in event.items():
            if isinstance(node_output, dict):
                merged.update(node_output)
    return merged


def llm_judge(question: str, expected: list, actual_answer: str) -> dict:
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


def _write_eval_log(item, expected, pipeline_sql, pipeline_answer, verdict, use_evidence, use_desc, tag, final_answer="", task_plan="", code="", sql_row_count=0):
    eval_log = {
        "question_id": item["question_id"],
        "db_id": item["db_id"],
        "difficulty": item["difficulty"],
        "question": item["question"],
        "evidence": item.get("evidence", ""),
        "use_evidence": use_evidence,
        "use_desc": use_desc,
        "gold_sql": item["SQL"],
        "expected_result": expected[:10] if expected else [],
        "expected_count": len(expected) if expected else 0,
        "pipeline_sql": pipeline_sql,
        "pipeline_answer": pipeline_answer,
        "final_answer": final_answer,
        "task_plan": task_plan,
        "code": code,
        "sql_row_count": sql_row_count,
        "judge_correct": verdict.get("correct", False),
        "judge_reason": verdict.get("reason", ""),
    }
    log_dir = EVAL_DIR / "results"
    if tag:
        log_dir = log_dir / tag
    else:
        evidence_tag = "with_evidence" if use_evidence else "no_evidence"
        log_dir = log_dir / evidence_tag
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{item['db_id']}_{item['question_id']}.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(eval_log, f, ensure_ascii=False, indent=2)


def run_single(item: dict, verbose: bool = True, use_evidence: bool = True, use_desc: bool = False, desc_in_question: bool = False, dynamic_desc: bool = False, full_desc: bool = False, tag: str = None) -> dict:
    """跑單題，回傳評測結果 dict"""
    if verbose:
        print(f"\n📋 Question #{item['question_id']} ({item['difficulty']})")
        print(f"   Q: {item['question'][:80]}")

    # Step 1: Gold SQL on SQLite
    try:
        expected = run_gold_sql_on_sqlite(item["db_id"], item["SQL"])
    except Exception as e:
        if verbose:
            print(f"   ⚠️ Gold SQL 執行失敗: {e}")
        verdict = {"correct": False, "reason": f"gold SQL error: {e}", "error": True}
        _write_eval_log(item, [], "", "gold SQL 執行失敗", verdict, use_evidence, use_desc, tag)
        return verdict

    # Step 2: Pipeline on PG
    try:
        question = item["question"]
        if use_evidence and item.get("evidence"):
            question += f"\n(Hint: {item['evidence']})"
        init_state = {"question": question, "retry": 0}

        # 注入 BIRD 原始 CSV 欄位描述（需 --full-desc）
        if full_desc:
            col_descs = load_column_descs(item["db_id"])
            if col_descs:
                init_state["column_descs"] = col_descs

        if dynamic_desc:
            # 先設定 DB，避免 llm import 時觸發錯誤的 db 連線
            os.environ["DATABASE_URL"] = f"{PG_BASE_URL}/{DB_PREFIX}{item['db_id']}"
            static_desc = load_db_description(item["db_id"])
            if static_desc:
                q_with_hint = question  # 已包含 evidence hint
                notes = llm_summarize_desc(q_with_hint, static_desc)
                desc = static_desc
                if notes:
                    desc += f"\n\n注意事項：\n{notes}"
                init_state["schema_desc"] = desc
                if desc:
                    init_state["schema_desc"] = desc
        elif use_desc:
            desc = load_db_description(item["db_id"], question=item["question"])
            if desc:
                if desc_in_question:
                    question += f"\n\nColumn descriptions:\n{desc}"
                    init_state["question"] = question
                else:
                    init_state["schema_desc"] = desc
        result = run_pipeline_with_state(init_state, item["db_id"])
        answer = result.get("display_answer") or result.get("final_answer") or "無法回答"
    except Exception as e:
        if verbose:
            print(f"   ⚠️ Pipeline 執行失敗: {e}")
        verdict = {"correct": False, "reason": f"pipeline error: {e}", "error": True}
        _write_eval_log(item, expected, "", f"Pipeline 執行失敗: {e}", verdict, use_evidence, use_desc, tag)
        return verdict

    if verbose:
        print(f"   Answer: {answer[:100]}")

    # Step 3: LLM Judge
    verdict = llm_judge(item["question"], expected, answer)
    icon = "✅" if verdict.get("correct") else "❌"
    if verbose:
        print(f"   {icon} {verdict.get('reason', '')}")

    # 寫入評測 log
    raw_answer = result.get("final_answer", "")
    _write_eval_log(
        item, expected, result.get("sql", ""), answer, verdict,
        use_evidence, use_desc, tag,
        final_answer=raw_answer,
        task_plan=result.get("task_plan", ""),
        code=result.get("code", ""),
        sql_row_count=len(result.get("sql_result", [])),
    )

    return verdict


def print_summary(results: list, items: list):
    """印出準確度統計"""
    total = len(results)
    correct = sum(1 for r in results if r.get("correct"))
    errors = sum(1 for r in results if r.get("error"))

    print("\n" + "=" * 60)
    print(f"📊 評測結果：{correct}/{total} ({correct/total*100:.1f}%)")
    if errors:
        print(f"   ⚠️ 執行錯誤：{errors} 題")
    print()

    # 按難度分類
    by_diff = {}
    for item, result in zip(items, results):
        diff = item["difficulty"]
        if diff not in by_diff:
            by_diff[diff] = {"total": 0, "correct": 0}
        by_diff[diff]["total"] += 1
        if result.get("correct"):
            by_diff[diff]["correct"] += 1

    for diff in ["simple", "moderate", "challenging"]:
        if diff in by_diff:
            d = by_diff[diff]
            pct = d["correct"] / d["total"] * 100 if d["total"] > 0 else 0
            print(f"   {diff:12s}: {d['correct']}/{d['total']} ({pct:.1f}%)")

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="california_schools")
    parser.add_argument("--id", type=int, default=None, help="單題模式：指定 question_id")
    parser.add_argument("--limit", type=int, default=None, help="批次模式：最多跑幾題")
    parser.add_argument("--no-evidence", action="store_true", help="不使用 evidence hint")
    parser.add_argument("--with-desc", action="store_true", help="附加 BIRD database description 到問題中")
    parser.add_argument("--desc-in-question", action="store_true", help="把 description 接在 question 裡（而非注入 SQL prompt）")
    parser.add_argument("--dynamic-desc", action="store_true", help="用 LLM 從完整 description 動態精簡出跟問題相關的欄位說明")
    parser.add_argument("--full-desc", action="store_true", help="注入 BIRD 原始 CSV 欄位描述（load_full_description）")
    parser.add_argument("--tag", default=None, help="實驗標籤（如 v1, gpt4o, no_retrieval），用於區分不同版本的結果")
    args = parser.parse_args()

    with open(EVAL_DIR / "dev.json") as f:
        all_questions = json.load(f)

    # 篩選題目
    candidates = [q for q in all_questions if q["db_id"] == args.db]

    if args.id is not None:
        # 單題模式
        item = next((q for q in candidates if q["question_id"] == args.id), None)
        if not item:
            print(f"❌ 找不到 db_id={args.db}, question_id={args.id}")
            sys.exit(1)
        verdict = run_single(item, verbose=True, use_evidence=not args.no_evidence, use_desc=args.with_desc, desc_in_question=args.desc_in_question, dynamic_desc=args.dynamic_desc, full_desc=args.full_desc, tag=args.tag)
        icon = "✅" if verdict.get("correct") else "❌"
        print(f"\n結果：{icon} {'PASS' if verdict.get('correct') else 'FAIL'}")
    else:
        # 批次模式
        if args.limit:
            candidates = candidates[:args.limit]

        evidence_label = "without evidence" if args.no_evidence else "with evidence"
        desc_label = " +dynamic-desc" if args.dynamic_desc else (" +desc(in Q)" if args.desc_in_question else (" +desc" if args.with_desc else (" +full-desc" if args.full_desc else "")))
        tag_label = f", tag={args.tag}" if args.tag else ""
        print(f"🚀 開始評測：{args.db}（{len(candidates)} 題，{evidence_label}{desc_label}{tag_label}）\n")
        start = time.time()
        results = []
        tag_dir = EVAL_DIR / "results" / (args.tag or ("with_evidence" if not args.no_evidence else "no_evidence"))
        for i, item in enumerate(candidates):
            # 跳過已完成的題目
            log_path = tag_dir / f"{item['db_id']}_{item['question_id']}.json"
            if log_path.exists():
                with open(log_path) as fh:
                    existing = json.load(fh)
                results.append({"correct": existing.get("judge_correct", False)})
                print(f"[{i+1}/{len(candidates)}] ⏭️ #{item['question_id']} (已完成)")
                continue
            print(f"[{i+1}/{len(candidates)}]", end="")
            verdict = run_single(item, verbose=True, use_evidence=not args.no_evidence, use_desc=args.with_desc, desc_in_question=args.desc_in_question, dynamic_desc=args.dynamic_desc, full_desc=args.full_desc, tag=args.tag)
            results.append(verdict)

        elapsed = time.time() - start
        print_summary(results, candidates)
        print(f"⏱️ 總耗時：{elapsed:.0f}s（平均 {elapsed/len(candidates):.1f}s/題）")


if __name__ == "__main__":
    main()
