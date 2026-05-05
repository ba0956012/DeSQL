"""
Python code 生成、sandbox 執行節點
"""

import ast
import json
import os
from datetime import datetime, timedelta, date
from collections import Counter, defaultdict
from decimal import Decimal

from langchain_core.messages import HumanMessage

from llm import code_llm as llm
from utils import debug_log, clean_llm_json, strip_code_fences
from prompts import load_profile


def _safe_import(name, *args, **kwargs):
    _ALLOWED = {"datetime", "collections", "decimal", "_datetime", "time"}
    if name in _ALLOWED:
        return __import__(name, *args, **kwargs)
    raise ImportError(f"import '{name}' 已停用，請使用已提供的內建函式")


SAFE_BUILTINS = {
    "len": len,
    "sum": sum,
    "min": min,
    "max": max,
    "sorted": sorted,
    "reversed": reversed,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "range": range,
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "round": round,
    "abs": abs,
    "any": any,
    "all": all,
    "isinstance": isinstance,
    "hasattr": hasattr,
    "getattr": getattr,
    "type": type,
    "print": print,
    "next": next,
    "iter": iter,
    "Counter": Counter,
    "defaultdict": defaultdict,
    "Decimal": Decimal,
    "datetime": datetime,
    "timedelta": timedelta,
    "date": date,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "KeyError": KeyError,
    "IndexError": IndexError,
    "AttributeError": AttributeError,
    "ZeroDivisionError": ZeroDivisionError,
    "__import__": _safe_import,
}


def _normalize_dates(data):
    if not isinstance(data, list):
        return data
    for row in data:
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            if isinstance(v, datetime) and not isinstance(v, date):
                row[k] = v.date()
    return data


def _validate_and_run(code, data):
    data = _normalize_dates(data)
    lines = code.split("\n")
    lines = [ln for ln in lines if not ln.strip().startswith(("import ", "from "))]
    code = "\n".join(lines)
    try:
        ast.parse(code)
    except Exception as e:
        return False, str(e), None, None
    namespace = {"__builtins__": SAFE_BUILTINS, "data": data}
    try:
        exec(code, namespace)
    except NameError as e:
        return False, f"{e}。請直接使用已提供的內建函式，不要使用 import", None, None
    except ImportError as e:
        return False, f"{e}。請直接使用已提供的內建函式，不要使用 import", None, None
    except Exception as e:
        return False, str(e), None, None
    if "result" not in namespace:
        return False, "missing result", None, None
    return True, "OK", namespace["result"], namespace.get("chart_data", [])


def _profile_data(sql_result, sample):
    """計算資料特徵，幫助 code 理解資料結構"""
    if not sql_result or not isinstance(sql_result, list):
        return ""
    if not isinstance(sql_result[0], dict):
        return ""

    cols = list(sql_result[0].keys())
    total = len(sql_result)
    lines = []

    for col in cols:
        vals = [r.get(col) for r in sql_result if r.get(col) is not None]
        null_count = total - len(vals)

        if not vals:
            lines.append(f"{col}: all null ({total} rows)")
            continue

        # 判斷欄位類型
        numeric_vals = []
        for v in vals:
            try:
                numeric_vals.append(float(v))
            except (ValueError, TypeError):
                break

        is_numeric = len(numeric_vals) == len(vals) and len(vals) > 0

        # Distinct values
        str_vals = [str(v) for v in vals]
        unique_strs = set(str_vals)
        unique_count = len(unique_strs)

        if is_numeric and unique_count > 10:
            # 數值欄位，多 distinct → 連續型
            parts = [f"{col}: {unique_count} unique values"]
            parts.append(
                f"min={min(numeric_vals):.4g}, max={max(numeric_vals):.4g}, sum={sum(numeric_vals):.4g}"
            )
            if null_count > 0:
                parts.append(f"{null_count} nulls")
            lines.append(", ".join(parts))
        elif unique_count <= 10:
            # 分類欄位（≤10 distinct）→ 列出每個值的 count
            from collections import Counter as _Counter

            counts = _Counter(str_vals)
            dist_parts = [f"{v}: {c}" for v, c in counts.most_common()]
            parts = [
                f"{col}: categorical, {unique_count} values — {', '.join(dist_parts)}"
            ]
            if null_count > 0:
                parts.append(f"{null_count} nulls")
            lines.append(", ".join(parts))
        else:
            # 文字欄位，多 distinct → ID 或名稱
            parts = [f"{col}: {unique_count} unique values"]
            if null_count > 0:
                parts.append(f"{null_count} nulls")
            if unique_count < total:
                parts.append("has duplicates")
            lines.append(", ".join(parts))

    # 檢查整行是否有完全重複
    row_strs = [str(sorted(r.items())) for r in sql_result]
    unique_rows = len(set(row_strs))
    if unique_rows < total:
        lines.append(
            f"WARNING: {total - unique_rows} duplicate rows in data (total {total}, unique {unique_rows})"
        )

    return "Data profile (computed from full dataset):\n" + "\n".join(
        f"  {line}" for line in lines
    )


def _check_code_task(state, python_task, expected_result, sql_result):
    """Check python_task against actual SQL result. Return data_notes string (empty if no issues).

    Uses a 'select from library' approach: LLM picks applicable warnings from a predefined set,
    and can optionally add a custom note. This prevents free-form hallucination while allowing
    context-specific guidance.
    """
    from llm import llm as _check_llm

    # Predefined warning library — each has a fixed, tested message for code LLM
    WARNING_LIBRARY = {
        "PERCENTAGE": "PERCENTAGE WARNING: The SQL data may only contain the subset (numerator). "
        "Use len(data) as denominator, do NOT re-filter to count the subset again.",
        "DISTINCT": "DISTINCT WARNING: The data has duplicate values in a key column. "
        "When counting items (districts, accounts, customers, schools), "
        "use len(set(...)) on the entity ID column, not len(data).",
        "COLUMN_MISMATCH": "COLUMN MISMATCH WARNING: The python_task references a column "
        "that may not exist in the SQL result. Check actual column names.",
        "ALREADY_AGGREGATED": "ALREADY AGGREGATED WARNING: SQL used GROUP BY, so the data "
        "already contains aggregated values. Do NOT re-aggregate in Python.",
        "SORT_DIRECTION": "SORT DIRECTION WARNING: Check sort order carefully. "
        "youngest/newest/latest = MAX(date). oldest/earliest/first = MIN(date).",
        "FIRST_LAST": "FIRST/LAST WARNING: 'first/earliest' = sort ascending, take [0]. "
        "'last/latest' = sort descending, take [0].",
    }

    try:
        question = state.get("question", "")
        cols = (
            list(sql_result[0].keys())
            if sql_result and isinstance(sql_result[0], dict)
            else []
        )
        row_count = len(sql_result)

        sample_vals = {}
        for col in cols[:10]:
            vals = [r.get(col) for r in sql_result[:5] if r.get(col) is not None]
            sample_vals[col] = [str(v)[:30] for v in vals[:3]]

        # Detect duplicates in key columns
        dup_info = ""
        if row_count > 1:
            dup_parts = []
            for col in cols[:6]:
                all_vals = [r.get(col) for r in sql_result if r.get(col) is not None]
                unique_count = len(set(str(v) for v in all_vals))
                if unique_count < len(all_vals) and unique_count < row_count * 0.9:
                    dup_parts.append(
                        f"{col}: {unique_count} unique out of {len(all_vals)} rows"
                    )
            if dup_parts:
                dup_info = f"\nDuplicate detection: {'; '.join(dup_parts)}"

        warning_ids = ", ".join(WARNING_LIBRARY.keys())
        prompt = f"""You are verifying a Python task against actual SQL query results.

Question: {question}
Python task: {python_task}
Expected output: {json.dumps(expected_result, ensure_ascii=False)}

SQL returned: {row_count} rows, Columns: {cols}
Sample: {json.dumps(sample_vals, ensure_ascii=False, default=str)[:500]}
{dup_info}

Available warnings: {warning_ids}
- PERCENTAGE: applies when computing a ratio but SQL only has the subset
- DISTINCT: applies when counting entities and data has duplicates in the key column (NOT in filter columns like city/status that are already constrained by WHERE)
- COLUMN_MISMATCH: applies when python_task references a column not in Columns list
- ALREADY_AGGREGATED: applies when SQL used GROUP BY (few rows with pre-computed values)
- SORT_DIRECTION: applies when question asks for youngest/oldest/newest and sort might be wrong
- FIRST_LAST: applies when question asks for first/last item and indexing might be wrong

Select which warnings apply to this task. Be conservative — only select if you are confident the issue exists.
Output JSON: {{"select": ["WARNING_ID", ...], "custom": "optional extra note if none of the above covers the issue, otherwise empty string"}}
If no issues: {{"select": [], "custom": ""}}
Output ONLY JSON:"""

        debug_log("check_code_task", prompt=prompt[:300])
        res = _check_llm.invoke([HumanMessage(content=prompt)])
        debug_log("check_code_task", response=res.content[:200])

        result = clean_llm_json(res.content)
        selected = result.get("select", [])
        custom = result.get("custom", "")

        if not selected and not custom:
            debug_log("check_code_task", result="no issues")
            return ""

        # Assemble data_notes from selected warnings
        notes = []
        for wid in selected:
            if wid in WARNING_LIBRARY:
                notes.append(WARNING_LIBRARY[wid])
                debug_log("check_code_task", selected_warning=wid)
        if custom:
            notes.append(f"ADDITIONAL NOTE: {custom}")
            debug_log("check_code_task", custom_note=custom[:100])

        return "\n".join(notes)
    except Exception as e:
        debug_log("check_code_task", error=str(e))
        return ""


def generate_code(state):
    profile = load_profile()

    sql_result = state.get("sql_result", [])
    if not isinstance(sql_result, list):
        sql_result = [sql_result] if sql_result else []
    if not sql_result:
        return {"final_answer": "查無資料", "error": ""}

    enable_chart = os.environ.get("ENABLE_CHART", "true").lower() in (
        "true",
        "1",
        "yes",
    )

    error_context = ""
    if state.get("error") and state.get("code"):
        error_context = (
            f"\nPrevious code failed:\nError: {state['error']}\n"
            f"Previous code:\n{state.get('code', '')}\nPlease fix the issue.\n"
        )

    # Extract result guidance from task_plan
    task_plan = state.get("task_plan", "")
    result_guidance = ""
    python_task = ""
    expected_result = {}
    if task_plan:
        try:
            plan = json.loads(task_plan)
            expected_result = plan.get("expected_result", {})
            python_task = plan.get("python_task", "")
        except (json.JSONDecodeError, KeyError):
            pass

    parts = []
    if expected_result:
        parts.append(
            f"Expected result: {expected_result.get('type', '')} — {expected_result.get('description', '')}"
        )
    if python_task:
        parts.append(f"Python task: {python_task}")
    if parts:
        result_guidance = "\n".join(parts)

    # Enhance: verify python_task against actual SQL result, produce data_notes
    data_notes = ""
    if python_task and sql_result:
        data_notes = _check_code_task(state, python_task, expected_result, sql_result)

    result_guidance_section = f"\n{result_guidance}\n" if result_guidance else ""

    sample = state.get("sample", [])
    if not isinstance(sample, list):
        sample = [sample] if sample else []

    chart_instruction = profile.build_chart_instruction(enable_chart)
    data_profile = _profile_data(sql_result, sample)
    prompt = profile.build_code_prompt(
        state.get("question", ""),
        state.get("sql", ""),
        len(sql_result),
        sample,
        result_guidance_section,
        error_context,
        chart_instruction,
        data_profile=data_profile,
        data_notes=data_notes,
    )

    debug_log("generate_code", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    code = strip_code_fences(res.content)
    return {"code": code}


def run_code(state):
    code = state.get("code", "")
    if not code:
        return {"error": "No code generated", "retry": state.get("retry", 0) + 1}
    debug_log("run_code", code=code)
    ok, msg, result, chart_data = _validate_and_run(code, state.get("sql_result", []))
    debug_log("run_code", ok=ok, msg=msg, result=result)
    if not ok:
        return {"error": msg, "retry": state.get("retry", 0) + 1}
    output = {"final_answer": str(result), "error": ""}
    if chart_data:
        output["chart_data"] = chart_data
    return output
