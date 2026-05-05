"""Analyze always-wrong questions using LLM to classify error source in the pipeline."""

import json, glob, os, sys, time
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Collect results from multiple runs
tags = [
    "test_rulesql",
    "test_rulesql2",
    "test_rulesql3",
    "test_datanote",
    "test_llmcheck",
    "test_llmcheck3",
    "test_aggonly3",
]

question_results = defaultdict(list)
for tag in tags:
    for db_suffix in ["schools", "financial", "debit"]:
        rdir = f"eval/results/{tag}_{db_suffix}"
        if not os.path.exists(rdir):
            continue
        for f in sorted(glob.glob(os.path.join(rdir, "*.json"))):
            with open(f) as fh:
                r = json.load(fh)
            qid = r.get("question_id")
            correct = r.get("judge_correct", False)
            question_results[qid].append((tag, correct, r))

# Find always-wrong
always_wrong = []
for qid, results in sorted(question_results.items()):
    if len(results) < 3:
        continue
    if all(not c for _, c, _ in results):
        _, _, r = results[-1]
        always_wrong.append(r)

print(f"Found {len(always_wrong)} always-wrong questions")

# Use LLM to classify each
from dotenv import load_dotenv

load_dotenv()
load_dotenv("eval/.env.eval", override=True)

import requests


def ask_llm(prompt):
    """Call Bedrock Converse API (same as pipeline uses)."""
    base_url = os.environ.get(
        "BEDROCK_BASE_URL", "https://bedrock-runtime.us-east-1.amazonaws.com"
    )
    token = os.environ.get("BEDROCK_API_TOKEN", "")
    model = "qwen.qwen3-next-80b-a3b"
    endpoint = f"{base_url}/model/{model}/converse"

    body = {
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {"temperature": 0},
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    for attempt in range(3):
        try:
            resp = requests.post(endpoint, json=body, headers=headers, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                return (
                    data.get("output", {})
                    .get("message", {})
                    .get("content", [{}])[0]
                    .get("text", "")
                )
            else:
                if attempt < 2:
                    time.sleep(2)
                else:
                    return f"ERROR: HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                return f"ERROR: {e}"


results_out = []
for r in always_wrong:
    qid = r.get("question_id")
    question = r.get("question", "")
    evidence = r.get("evidence", "")
    gold_sql = r.get("gold_sql", "")
    pipeline_sql = r.get("pipeline_sql", "")
    task_plan = r.get("task_plan", "")
    code = r.get("code", "")
    pipeline_answer = str(r.get("final_answer", ""))[:300]
    expected = str(r.get("expected_result", ""))[:300]
    sql_rows = r.get("sql_row_count", 0)

    prompt = f"""Analyze why this data analysis pipeline produced a wrong answer. The pipeline has these steps:
1. QA Analysis: Reads the question, plans which tables/columns to use, writes sql_task and python_task
2. SQL Generation: Generates SQL based on QA's plan (SQL only fetches data, no complex aggregation)
3. Python Code: Processes SQL results according to python_task
4. Format Answer: Formats the final answer

Question: {question}
Evidence/Hint: {evidence}

Gold SQL (reference, does everything in one query): {gold_sql[:400]}
Expected answer: {expected}

Pipeline SQL: {pipeline_sql[:400]}
SQL returned {sql_rows} rows
Task plan: {task_plan[:400]}
Python code: {code[:400]}
Pipeline answer: {pipeline_answer}

Classify the PRIMARY error source (pick ONE):
- QA_WRONG_TABLE: QA chose wrong or missing tables
- QA_WRONG_LOGIC: QA's sql_task or python_task has wrong logic/formula
- QA_WRONG_FILTER: QA's filters are wrong (wrong column, wrong value)
- SQL_WRONG: SQL didn't follow QA's plan correctly
- PYTHON_WRONG: Python code didn't follow python_task correctly
- FORMAT_WRONG: Answer is correct but formatted differently than expected
- MULTI_STEP: Question requires multi-step logic that pipeline can't handle

Output JSON only: {{"category": "...", "reason": "one sentence explanation"}}"""

    answer = ask_llm(prompt)
    try:
        parsed = json.loads(answer)
    except:
        parsed = {"category": "PARSE_ERROR", "reason": answer[:200]}

    entry = {
        "qid": qid,
        "db": r.get("db_id"),
        "difficulty": r.get("difficulty"),
        "category": parsed.get("category", "UNKNOWN"),
        "reason": parsed.get("reason", ""),
        "question": question[:80],
    }
    results_out.append(entry)
    print(
        f"  #{qid} ({r.get('difficulty')}, {r.get('db_id')}): {entry['category']} — {entry['reason'][:80]}"
    )

# Summary
print(f"\n=== Summary ({len(results_out)} questions) ===")
cats = Counter(e["category"] for e in results_out)
for cat, cnt in cats.most_common():
    print(f"  {cat}: {cnt}")
    for e in results_out:
        if e["category"] == cat:
            print(f"    #{e['qid']} ({e['difficulty']}, {e['db']}): {e['reason'][:70]}")

# Save
with open("eval/report/always_wrong_analysis.json", "w") as f:
    json.dump(results_out, f, indent=2, ensure_ascii=False)
print(f"\nSaved to eval/report/always_wrong_analysis.json")
