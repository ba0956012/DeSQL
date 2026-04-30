"""
SQL 生成與執行節點
"""

import json
import re
from langchain_core.messages import HumanMessage
from sqlalchemy import text as sa_text

from db import engine, SCHEMA_INFO, ENUM_VALUES
from llm import sql_llm as llm
from utils import debug_log, strip_code_fences
from retrieval_subgraph import format_enum_info, build_conditions_context
from domain_rules import DOMAIN_SQL_RULES
from prompts import load_profile


def _validate_filter_values(filters):
    """Validate filter values against DB. Return warning text for SQL prompt if issues found."""
    warnings = []
    checked = 0
    for f in filters:
        table = f.get("table", "")
        column = f.get("column", "")
        value = f.get("value")
        op = f.get("operator", "")

        # Only validate exact match filters with string values
        if not table or not column or not value or op not in ("=", "LIKE", "IN"):
            continue
        if not isinstance(value, str) or len(value) < 2:
            continue

        checked += 1
        try:
            # Check if value exists
            check_sql = f"""SELECT COUNT(*) FROM "{table}" WHERE "{column}" = '{value}'"""
            with engine.connect() as conn:
                count = conn.execute(sa_text(check_sql)).scalar()

            if count == 0:
                # Value not found — try to find similar values
                like_sql = f"""SELECT DISTINCT "{column}" FROM "{table}" WHERE "{column}" ILIKE '%{value}%' LIMIT 5"""
                with engine.connect() as conn:
                    similar = [r[0] for r in conn.execute(sa_text(like_sql)).fetchall()]

                if similar:
                    warnings.append(
                        f"\nWARNING: '{value}' not found in {table}.{column}. "
                        f"Similar values: {similar}. Use the exact value from this list."
                    )
                    debug_log("validate_filter", action="similar_found", table=table, column=column,
                              value=value, similar=similar)
                else:
                    # Try case-insensitive exact match
                    ilike_sql = f"""SELECT DISTINCT "{column}" FROM "{table}" WHERE LOWER("{column}") = LOWER('{value}') LIMIT 3"""
                    with engine.connect() as conn:
                        case_matches = [r[0] for r in conn.execute(sa_text(ilike_sql)).fetchall()]
                    if case_matches:
                        warnings.append(
                            f"\nWARNING: '{value}' not found in {table}.{column} (case mismatch). "
                            f"Correct value: '{case_matches[0]}'"
                        )
                        debug_log("validate_filter", action="case_mismatch", table=table, column=column,
                                  value=value, case_match=case_matches[0])
                    else:
                        debug_log("validate_filter", action="not_found", table=table, column=column, value=value)
            else:
                debug_log("validate_filter", action="ok", table=table, column=column, value=value, count=count)
        except Exception as e:
            debug_log("validate_filter", action="error", error=str(e))
            continue

    debug_log("validate_filter_summary", checked=checked, warnings=len(warnings))

    return "\n".join(warnings) if warnings else ""


def _embed_descs_in_ddl(schema_info: str, column_descs: dict) -> str:
    """把欄位描述嵌入 DDL 的每個欄位行後面作為 SQL 註解。"""
    import re
    lines = schema_info.split("\n")
    result = []
    current_table = ""
    for line in lines:
        lower = line.strip().lower()
        if lower.startswith("create table"):
            match = re.search(r'create\s+table\s+"?(\w+)"?', lower)
            if match:
                current_table = match.group(1)
        elif current_table and (line.startswith("\t") or line.startswith("    ")) and not lower.startswith(")"):
            col_match = re.match(r'\s+(\w+)\s+', line)
            if col_match:
                col_name = col_match.group(1).lower()
                key = f"{current_table}.{col_name}"
                desc = column_descs.get(key, "")
                if desc:
                    if len(desc) > 150:
                        desc = desc[:147] + "..."
                    line = line.rstrip()
                    line = f"{line}  -- {desc}"
        if lower.startswith(")"):
            current_table = ""
        result.append(line)
    return "\n".join(result)


def generate_sql(state):
    profile = load_profile()
    conditions_context = build_conditions_context(state)
    enum_info = format_enum_info(ENUM_VALUES)

    # Build error context
    sql_error_context = ""
    if state.get("error") and state.get("sql"):
        validation = state.get("sql_validation", "")
        if validation:
            sql_error_context = profile.build_error_context_validation(validation, state["sql"])
        else:
            sql_error_context = profile.build_error_context_failed(state["sql"], state["error"])
    elif state.get("sql") and not state.get("sql_result") and state.get("sql_retry", 0) > 0:
        sql_error_context = profile.build_error_context_empty(state["sql"])

    all_rules = profile.SQL_RULES + DOMAIN_SQL_RULES
    rules_text = "\n".join(f"- {r}" for r in all_rules)

    base_schema = state.get("filtered_schema") or SCHEMA_INFO
    column_descs = state.get("column_descs")
    schema_text = _embed_descs_in_ddl(base_schema, column_descs) if column_descs else base_schema

    schema_desc = state.get("schema_desc", "")
    schema_desc_section = f"\nColumn descriptions:\n{schema_desc}\n" if schema_desc else ""

    # Extract SQL task from task_plan
    task_plan = state.get("task_plan", "")
    sql_task = ""
    tables_info = ""
    join_info = ""
    filter_validation = ""
    if task_plan:
        try:
            tp = json.loads(task_plan)
            sql_task = tp.get("sql_task", "")
            tables = tp.get("tables_needed", [])
            joins = tp.get("join_path", [])
            filters = tp.get("filters", [])
            if tables:
                tables_info = f"Tables: {', '.join(str(t) for t in tables)}"
            if joins:
                join_info = f"Joins: {', '.join(str(j) for j in joins)}"
            # Validate filter values against DB
            if filters and state.get("sql_retry", 0) == 0:
                filter_validation = _validate_filter_values(filters)
        except (json.JSONDecodeError, KeyError):
            pass

    if sql_task:
        task_section = profile.build_task_section(sql_task, tables_info, join_info)
        prompt = profile.build_sql_prompt_with_task(
            task_section, rules_text, schema_text, schema_desc_section,
            enum_info, conditions_context + filter_validation, sql_error_context
        )
    else:
        prompt = profile.build_sql_prompt_no_task(
            state["question"], rules_text, schema_text, schema_desc_section,
            enum_info, conditions_context, sql_error_context
        )

    debug_log("generate_sql", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    sql = strip_code_fences(res.content)
    debug_log("generate_sql", final_sql=sql)
    return {"sql": sql}


def validate_sql_result(state):
    """Rule-based SQL result validation: check if SQL covered all tables from task_plan."""
    sql_result = state.get("sql_result", [])
    if not sql_result or state.get("sql_retry", 0) >= 2:
        return {"sql_validation": ""}

    task_plan = state.get("task_plan", "")
    if not task_plan:
        return {"sql_validation": ""}

    try:
        tp = json.loads(task_plan)
        planned_tables = set(str(t).lower() for t in tp.get("tables_needed", []))
    except (json.JSONDecodeError, KeyError):
        return {"sql_validation": ""}

    if not planned_tables:
        return {"sql_validation": ""}

    # Parse actual SQL to find which tables were used
    sql = state.get("sql", "").lower()
    sql_tables = set(re.findall(r'\bfrom\s+(\w+)', sql) + re.findall(r'\bjoin\s+(\w+)', sql))

    missing_tables = planned_tables - sql_tables
    if not missing_tables:
        return {"sql_validation": ""}

    # Only flag if the missing table has columns referenced in python_task
    reason = f"SQL is missing table(s): {', '.join(sorted(missing_tables))}. " \
             f"Task plan requires: {', '.join(sorted(planned_tables))}. " \
             f"SQL only uses: {', '.join(sorted(sql_tables))}. " \
             f"Please add the missing table(s) with appropriate JOIN."

    debug_log("validate_sql_result", result="missing_tables",
              planned=sorted(planned_tables), actual=sorted(sql_tables),
              missing=sorted(missing_tables))

    return {
        "sql_validation": reason,
        "error": f"SQL result validation: {reason}",
        "sql_retry": state.get("sql_retry", 0) + 1,
    }


def execute_sql(state):
    sql = state.get("sql", "")
    if not sql:
        return {
            "sql_result": [], "sample": [], "error": "No SQL generated",
            "sql_retry": state.get("sql_retry", 0) + 1,
        }
    debug_log("execute_sql", sql=sql)
    try:
        with engine.connect() as conn:
            rp = conn.execute(sa_text(sql))
            cols = list(rp.keys())
            rows = rp.fetchall()
        result = [dict(zip(cols, r)) for r in rows]
        debug_log("execute_sql", row_count=len(result))
        output = {"sql_result": result, "sample": result[:5], "error": ""}
        if not result:
            output["sql_retry"] = state.get("sql_retry", 0) + 1
        return output
    except Exception as e:
        debug_log("execute_sql", error=str(e))
        return {
            "sql_result": [],
            "sample": [],
            "error": f"SQL 執行錯誤: {str(e)}",
            "sql_retry": state.get("sql_retry", 0) + 1,
        }
