"""
Python Task Refiner — 在看到實際 SQL 結果後，重新生成精確的 python_task。

取代 QA node 盲目生成的 python_task，根據實際資料的欄位、型別、分佈來決定
Python 應該怎麼處理資料。
"""

import json
from langchain_core.messages import HumanMessage

from llm import code_llm as llm
from utils import debug_log


def _build_data_summary(sql_result, sample):
    """Build a concise data summary for the refiner LLM."""
    if not sql_result or not isinstance(sql_result, list):
        return ""
    if not isinstance(sql_result[0], dict):
        return ""

    cols = list(sql_result[0].keys())
    total = len(sql_result)
    lines = [f"Row count: {total}", f"Columns: {cols}"]

    for col in cols:
        vals = [r.get(col) for r in sql_result if r.get(col) is not None]
        if not vals:
            lines.append(f"  {col}: all null")
            continue

        sample_val = vals[0]
        type_name = type(sample_val).__name__
        unique_count = len(set(str(v) for v in vals))

        if unique_count <= 10:
            from collections import Counter as _C

            counts = _C(str(v) for v in vals)
            dist = ", ".join(f"{v}:{c}" for v, c in counts.most_common())
            lines.append(f"  {col} (type={type_name}): {unique_count} unique — {dist}")
        else:
            lines.append(f"  {col} (type={type_name}): {unique_count} unique values")

    return "\n".join(lines)


REFINE_PROMPT = """You are a data analysis expert. Given the user's question, the SQL query that was executed, and the actual data returned, write a precise Python task description.

User Question: {question}

SQL executed: {sql}

Actual data summary:
{data_summary}

Sample rows (first 3):
{sample_str}

Original plan from QA:
  expected_result: {expected_result}
  original python_task: {original_task}

Now write a REFINED python_task that is precise and accounts for the actual data. Rules:
- If the question asks "how many X and Y", specify to count EACH category separately, not just total rows.
- If the question asks for a specific formula, write it explicitly with column names from the actual data.
- Reference ONLY columns that exist in the data (see Columns list above).
- Use the correct Python types for comparison (see type= in data summary). For example, if a column is type=float, compare with float not string.
- If SQL already filtered the data precisely, say "extract the result directly" instead of re-filtering.
- Be specific about what the final result should look like (single number, list of names, dict, etc.)

Output ONLY the refined python_task text (one paragraph, no JSON wrapping):"""


def refine_python_task(state):
    """Refine python_task based on actual SQL results."""
    sql_result = state.get("sql_result", [])
    if not sql_result:
        return {}  # No data, nothing to refine

    task_plan = state.get("task_plan", "")
    if not task_plan:
        return {}

    try:
        plan = json.loads(task_plan)
    except (json.JSONDecodeError, KeyError):
        return {}

    original_task = plan.get("python_task", "")
    expected_result = plan.get("expected_result", {})

    # Build data context
    sample = state.get("sample", [])[:3]
    sample_str = (
        json.dumps(sample, indent=2, ensure_ascii=False, default=str)
        if sample
        else "[]"
    )
    data_summary = _build_data_summary(sql_result, sample)

    prompt = REFINE_PROMPT.format(
        question=state.get("question", ""),
        sql=state.get("sql", "")[:300],
        data_summary=data_summary,
        sample_str=sample_str[:500],
        expected_result=(
            json.dumps(expected_result, ensure_ascii=False)
            if expected_result
            else "N/A"
        ),
        original_task=original_task,
    )

    debug_log("refine_python_task", prompt=prompt[:400])

    try:
        res = llm.invoke([HumanMessage(content=prompt)])
        refined = res.content.strip()
        # Remove any JSON wrapping if LLM added it
        if refined.startswith('"') and refined.endswith('"'):
            refined = refined[1:-1]
        debug_log(
            "refine_python_task", original=original_task[:100], refined=refined[:200]
        )
    except Exception as e:
        debug_log("refine_python_task", error=str(e))
        return {}  # On error, keep original

    # Update task_plan with refined python_task
    plan["python_task"] = refined
    return {"task_plan": json.dumps(plan, ensure_ascii=False, indent=2)}
