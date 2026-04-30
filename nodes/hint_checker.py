"""
Hint Compliance Checker — LLM 檢查 QA plan 是否遵循 Hint。

放在 question_analysis 之後、generate_sql 之前。
用 LLM 比對 Hint 和 task_plan，如果有遺漏就修正。
"""

import json
from langchain_core.messages import HumanMessage
from llm import qa_llm as llm
from utils import debug_log, clean_llm_json


def check_hint_compliance(state):
    """Check if task_plan follows hint. Fix if not."""
    question = state.get("question", "")
    task_plan = state.get("task_plan", "")

    if not task_plan:
        return {}

    # Extract hint
    hint_start = question.find("(Hint:")
    if hint_start == -1:
        return {}  # No hint
    hint_text = question[hint_start + 6:].rstrip(")").strip()
    if not hint_text:
        return {}

    try:
        plan = json.loads(task_plan)
    except (json.JSONDecodeError, KeyError):
        return {}

    # Ask LLM to check hint compliance
    prompt = f"""Check if the query plan follows the Hint correctly.

Hint: {hint_text}

Query Plan:
  tables_needed: {plan.get('tables_needed', [])}
  filters: {json.dumps(plan.get('filters', []), ensure_ascii=False)[:300]}
  sql_task: {plan.get('sql_task', '')[:200]}
  python_task: {plan.get('python_task', '')[:200]}

Check:
1. If the Hint mentions a specific table (e.g., "in the frpm"), is that table in tables_needed?
2. If the Hint mentions a specific column = value, is it in the filters or sql_task?
3. If the Hint provides a formula, does sql_task fetch the raw columns needed?

If the plan already follows the Hint correctly, output: {{"ok": true}}
If something is missing, output: {{"ok": false, "fixes": [{{"action": "add_table", "table": "table_name"}}, {{"action": "add_filter", "table": "t", "column": "c", "value": "v"}}, {{"action": "append_sql_task", "text": "additional instruction"}}]}}
Output ONLY JSON:"""

    debug_log("hint_checker", prompt=prompt[:300])
    try:
        res = llm.invoke([HumanMessage(content=prompt)])
        debug_log("hint_checker", response=res.content[:300])
        result = clean_llm_json(res.content)
    except Exception as e:
        debug_log("hint_checker", error=str(e))
        return {}

    if result.get("ok", True):
        debug_log("hint_checker", result="plan follows hint")
        return {}

    # Apply fixes
    fixes = result.get("fixes", [])
    if not fixes:
        return {}

    changed = False
    for fix in fixes:
        action = fix.get("action", "")
        if action == "add_table":
            table = fix.get("table", "").lower()
            if table and table not in [t.lower() for t in plan.get("tables_needed", [])]:
                plan.setdefault("tables_needed", []).append(table)
                changed = True
                debug_log("hint_checker", fix=f"added table: {table}")

        elif action == "add_filter":
            new_filter = {
                "description": f"From Hint: {fix.get('column', '')} = {fix.get('value', '')}",
                "table": fix.get("table", ""),
                "column": fix.get("column", ""),
                "operator": "=",
                "value": fix.get("value", ""),
            }
            plan.setdefault("filters", []).append(new_filter)
            changed = True
            debug_log("hint_checker", fix=f"added filter: {new_filter}")

        elif action == "append_sql_task":
            text = fix.get("text", "")
            if text:
                plan["sql_task"] = plan.get("sql_task", "") + " " + text
                changed = True
                debug_log("hint_checker", fix=f"appended to sql_task: {text[:80]}")

    if changed:
        new_plan = json.dumps(plan, ensure_ascii=False, indent=2)
        debug_log("hint_checker", action="plan_modified")
        return {"task_plan": new_plan}

    return {}
