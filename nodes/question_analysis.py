"""
問題分解與 Schema Mapping 節點（含 review 機制 + FK 路徑補全）
"""

import json
import os
import re
from langchain_core.messages import HumanMessage

from db import SCHEMA_INFO
from llm import qa_llm as llm, schema_llm as review_llm
from utils import debug_log, clean_llm_json
from prompts import load_profile

ENABLE_QA_REVIEW = os.getenv("ENABLE_QA_REVIEW", "false").lower() in (
    "true",
    "1",
    "yes",
)


def _filter_enum_for_question(question: str, enum_values: dict) -> str:
    """Filter enum_values to only include columns relevant to the question.
    Rules:
    - Skip columns where all values are purely numeric
    - Skip columns with >15 values unless a value matches the question
    - Truncate value lists to max 10 items
    - Always include columns where a value appears in the question text
    """
    question_lower = question.lower()

    lines = []
    for col_key, vals in sorted(enum_values.items()):
        str_vals = [str(v) for v in vals]

        # Skip if all values are numeric
        if all(v.replace(".", "").replace("-", "").isdigit() for v in str_vals):
            continue

        # Check if any value appears in the question (always include these)
        has_match = any(sv.lower() in question_lower for sv in str_vals if len(sv) >= 4)

        # Skip large columns unless they have a direct match
        if len(vals) > 15 and not has_match:
            continue

        # Skip if no match and values are too domain-specific (emails, names with dots)
        if not has_match and any("@" in sv or "." in sv for sv in str_vals[:3]):
            continue

        # Include: matched values, or small useful enum columns (≤8 values)
        if has_match or len(vals) <= 8:
            display_vals = str_vals[:10]
            suffix = f" ... ({len(vals)} total)" if len(vals) > 10 else ""
            lines.append(f"  {col_key}: {display_vals}{suffix}")

    return "\n".join(lines) if lines else ""


def _embed_descs(schema_info, column_descs):
    """把欄位描述嵌入 DDL"""
    if not column_descs:
        return schema_info
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
        elif (
            current_table
            and (line.startswith("\t") or line.startswith("    "))
            and not lower.startswith(")")
        ):
            col_match = re.match(r"\s+(\w+)\s+", line)
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


def _parse_fk_edges(schema_info: str):
    """Parse FK relationships from DDL. Returns list of (src_table, src_col, ref_table, ref_col)."""
    edges = []
    current_table = ""
    for line in schema_info.split("\n"):
        lower = line.strip().lower()
        m = re.match(r'\s*create\s+table\s+"?(\w+)"?', lower)
        if m:
            current_table = m.group(1)
            continue
        if current_table and "references" in lower:
            fk_m = re.search(
                r'foreign\s+key\s*\("?(\w+)"?\)\s+references\s+"?(\w+)"?\s*\("?(\w+)"?\)',
                lower,
            )
            if fk_m:
                edges.append(
                    (current_table, fk_m.group(1), fk_m.group(2), fk_m.group(3))
                )
        if lower.startswith(")"):
            current_table = ""
    return edges


def _build_fk_graph(edges):
    """Build undirected adjacency: {table: {neighbor_table: (src_col, ref_col)}}"""
    graph = {}
    for src_t, src_c, ref_t, ref_c in edges:
        graph.setdefault(src_t, {})[ref_t] = (src_c, ref_c)
        graph.setdefault(ref_t, {})[src_t] = (ref_c, src_c)
    return graph


def _find_shortest_path(graph, src, dst):
    """BFS shortest path between two tables. Returns list of table names or None."""
    if src == dst:
        return [src]
    if src not in graph or dst not in graph:
        return None
    visited = {src}
    queue = [(src, [src])]
    while queue:
        node, path = queue.pop(0)
        for neighbor in graph.get(node, {}):
            if neighbor in visited:
                continue
            new_path = path + [neighbor]
            if neighbor == dst:
                return new_path
            visited.add(neighbor)
            queue.append((neighbor, new_path))
    return None


def _complete_fk_path(task_plan: str, schema_info: str) -> str:
    """Check tables_needed connectivity via FK and auto-insert missing bridge tables + join_path."""
    try:
        parsed = json.loads(task_plan)
        tables = parsed.get("tables_needed", [])

        edges = _parse_fk_edges(schema_info)
        if not edges:
            return task_plan

        graph = _build_fk_graph(edges)
        tables_lower = [t.lower() for t in tables]

        # Case 1: Single table — remind about FK neighbors
        if len(tables_lower) == 1:
            t = tables_lower[0]
            neighbors = list(graph.get(t, {}).keys())
            if neighbors:
                sql_task = parsed.get("sql_task", "")
                if sql_task:
                    parsed["sql_task"] = (
                        sql_task
                        + f" (Note: if you need columns not in {t}, consider JOINing with: {', '.join(neighbors)})"
                    )
                    debug_log("fk_path_complete", single_table=t, neighbors=neighbors)
                    return json.dumps(parsed, ensure_ascii=False, indent=2)
            return task_plan

        all_needed = set(tables_lower)
        new_joins = list(parsed.get("join_path", []))

        # Case 2: Multiple tables — find FK path and add missing bridge tables
        for i in range(len(tables_lower)):
            for j in range(i + 1, len(tables_lower)):
                path = _find_shortest_path(graph, tables_lower[i], tables_lower[j])
                if not path:
                    continue
                # Add bridge tables
                for t in path:
                    if t not in all_needed:
                        all_needed.add(t)
                        debug_log(
                            "fk_path_complete",
                            added_table=t,
                            between=(tables_lower[i], tables_lower[j]),
                        )
                # Add join conditions for consecutive pairs in path
                for k in range(len(path) - 1):
                    t1, t2 = path[k], path[k + 1]
                    if t2 in graph.get(t1, {}):
                        c1, c2 = graph[t1][t2]
                        join_str = f"{t1}.{c1} = {t2}.{c2}"
                        reverse_str = f"{t2}.{c2} = {t1}.{c1}"
                        existing = [j.lower() for j in new_joins]
                        if (
                            join_str.lower() not in existing
                            and reverse_str.lower() not in existing
                        ):
                            new_joins.append(join_str)

        if all_needed != set(tables_lower) or len(new_joins) != len(
            parsed.get("join_path", [])
        ):
            parsed["tables_needed"] = sorted(all_needed)
            parsed["join_path"] = new_joins
            added = all_needed - set(tables_lower)
            if added:
                sql_task = parsed.get("sql_task", "")
                if sql_task:
                    parsed["sql_task"] = (
                        sql_task
                        + f" (Note: also JOIN through {', '.join(sorted(added))} for FK connectivity)"
                    )
            debug_log(
                "fk_path_complete",
                original_tables=tables_lower,
                final_tables=sorted(all_needed),
                joins=new_joins,
            )
            return json.dumps(parsed, ensure_ascii=False, indent=2)

        return task_plan
    except Exception as e:
        debug_log("fk_path_complete", error=str(e))
        return task_plan


def _review_plan(question, task_plan, schema_text, profile):
    """用 reviewer LLM 檢查 task_plan，回傳修正後的 plan 或原 plan"""
    try:
        review_prompt = profile.build_qa_review_prompt(question, task_plan, schema_text)
        debug_log("qa_review", prompt=review_prompt[:300])
        res = review_llm.invoke([HumanMessage(content=review_prompt)])
        debug_log("qa_review", response=res.content[:300])

        review = clean_llm_json(res.content)
        if review.get("approved", True):
            debug_log("qa_review", result="approved")
            return task_plan

        # 有修正建議，套用到 plan
        fixes = review.get("fixes", [])
        if not fixes:
            return task_plan

        parsed = json.loads(task_plan)
        for fix in fixes:
            field = fix.get("field", "")
            fix_text = fix.get("fix", "")
            if field and fix_text and field in parsed:
                debug_log("qa_review", fix_field=field, fix=fix_text[:100])
                parsed[field] = fix_text

        return json.dumps(parsed, ensure_ascii=False, indent=2)
    except Exception as e:
        debug_log("qa_review", error=str(e))
        return task_plan


def _classify_question(question: str) -> str:
    """Use LLM to provide specific supplementary advice ONLY for complex questions."""
    prompt = f"""Analyze this data analysis question. ONLY provide advice if the question has one of these specific pitfalls:

1. SEPARATE COUNTS: Question asks "how many X and Y" — need to count X and Y separately, not as a total
2. AGGREGATION NEEDED: Question asks about a total/sum over a time period, but data has one row per month/transaction — need GROUP BY + SUM first
3. HINT EXPLAINS COLUMN NAME: Hint says "X refers to Y" meaning a column name, NOT a filter condition
4. MULTI-STEP COMPUTATION: Question requires computing an intermediate value (e.g., rate, difference) before answering
5. AMBIGUOUS ENTITY: Question mentions an entity that could map to multiple columns (e.g., "district" could be district name or district code)

For SIMPLE questions (direct lookup, single filter, straightforward count), output empty advice.
Be very conservative — most questions do NOT need extra advice.

Question: {question}

Output JSON: {{"advice": "specific advice if needed, or empty string for simple questions"}}
Output ONLY JSON:"""

    try:
        res = llm.invoke([HumanMessage(content=prompt)])
        debug_log("classify_question", response=res.content[:200])
        result = clean_llm_json(res.content)
        advice = result.get("advice", "")
        if advice:
            debug_log("classify_question", advice=advice[:100])
        return advice
    except Exception as e:
        debug_log("classify_question", error=str(e))
        return ""


def question_analysis(state):
    """分解問題，生成結構化的 task_plan 供 SQL 和 Python 使用。"""
    profile = load_profile()
    question = state["question"]

    schema_desc = state.get("schema_desc", "")
    schema_desc_section = (
        f"\nColumn descriptions:\n{schema_desc}\n" if schema_desc else ""
    )
    base_schema = state.get("filtered_schema") or SCHEMA_INFO
    column_descs = state.get("column_descs")
    schema_text = (
        _embed_descs(base_schema, column_descs) if column_descs else base_schema
    )

    # Build enum values summary for QA (helps pick correct columns)
    from db import ENUM_VALUES

    enum_info = _filter_enum_for_question(question, ENUM_VALUES)

    # Dynamic advice for complex questions only
    dynamic_guidelines = _classify_question(question)

    prompt = profile.build_qa_prompt(
        question,
        schema_text,
        schema_desc_section,
        conditions_context=state.get("_conditions_context", ""),
        enum_info=enum_info,
        dynamic_guidelines=dynamic_guidelines,
    )

    debug_log("question_analysis", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    debug_log("question_analysis", llm_response=res.content)

    try:
        parsed = clean_llm_json(res.content)
        task_plan = json.dumps(parsed, ensure_ascii=False, indent=2)
        needs_python = parsed.get("needs_python", True)
    except (json.JSONDecodeError, KeyError):
        debug_log("question_analysis", error="JSON parse failed")
        task_plan = ""
        needs_python = True

    # FK path completion: disabled for now — no significant improvement in eval
    # if task_plan:
    #     task_plan = _complete_fk_path(task_plan, SCHEMA_INFO)

    # Refine: check if aggregation is needed (disabled — no net improvement in eval)
    # if task_plan:
    #     task_plan = _refine_aggregation(question, task_plan)
    #     try:
    #         parsed = json.loads(task_plan)
    #         needs_python = parsed.get("needs_python", True)
    #     except (json.JSONDecodeError, KeyError):
    #         pass

    # Review step（用不同的 LLM 檢查 plan）
    if ENABLE_QA_REVIEW and task_plan:
        task_plan = _review_plan(question, task_plan, schema_text, profile)
        try:
            parsed = json.loads(task_plan)
            needs_python = parsed.get("needs_python", True)
        except (json.JSONDecodeError, KeyError):
            pass

    # Enhance: check sql_task for common pitfalls (disabled — causes more harm than good)
    # if task_plan:
    #     task_plan = _enhance_sql_task(question, task_plan)

    debug_log("question_analysis", task_plan=task_plan[:300], needs_python=needs_python)
    return {"task_plan": task_plan, "qa_needs_python": needs_python}


def _refine_aggregation(question: str, task_plan: str) -> str:
    """Refine sql_task/python_task: detect when aggregation (GROUP BY + SUM) is needed.
    Triggered when a table has composite PK (multiple rows per entity) and the question
    asks about a time period or total that spans multiple rows."""
    try:
        parsed = json.loads(task_plan)
        sql_task = parsed.get("sql_task", "")
        python_task = parsed.get("python_task", "")
        tables = parsed.get("tables_needed", [])
        # filters = parsed.get("filters", [])

        if not sql_task or not tables:
            return task_plan

        # Detect composite PK tables from schema
        composite_pk_tables = _detect_composite_pk_tables()
        used_composite = [t for t in tables if t.lower() in composite_pk_tables]

        if not used_composite:
            return task_plan

        # Build context about table granularity
        granularity_info = []
        for t in used_composite:
            pk_cols = composite_pk_tables[t.lower()]
            granularity_info.append(
                f"Table '{t}' has composite PK ({', '.join(pk_cols)}), meaning each row represents one combination of these columns."
            )

        prompt = f"""Review this query plan. A table used has a composite primary key, meaning multiple rows exist per entity.

Question: {question[:300]}
sql_task: {sql_task[:300]}
python_task: {python_task[:300]}

Table granularity:
{chr(10).join(granularity_info)}

Determine: Does the python_task apply a THRESHOLD or FILTER to a per-row value that should actually be an AGGREGATED value across the time period?

Specifically, check if:
- The question asks about a TOTAL value over a time period (e.g., "consumption for the year", "total spending in 2012")
- BUT the sql_task/python_task filters or compares individual row values instead of first aggregating (GROUP BY entity + SUM) then filtering

Rules:
- If python_task simply finds MAX/MIN of individual rows (e.g., "find maximum consumption") and the question asks for the highest single value → that is CORRECT, do NOT modify
- If python_task counts rows or finds extremes without aggregation, but the question clearly asks about a TOTAL over a period → aggregation IS needed
- If you are NOT CERTAIN that aggregation is missing → output ok=true (be conservative, do not modify)

ONLY output ok=false if ALL of these are true:
1. The question explicitly asks about a TOTAL/SUM over a time period (not just the highest/lowest individual value)
2. The current plan treats per-row values as if they were already totals
3. The sql_task has a WHERE filter on the value column that should only apply AFTER aggregation

Output JSON:
- If no change needed: {{"ok": true}}
- If aggregation missing: {{"ok": false, "fixed_python_task": "description of correct Python logic", "fixed_sql_task": "corrected sql_task removing pre-aggregation filters, or empty string if sql_task is fine", "needs_python": true}}
IMPORTANT: fixed_python_task must be a DESCRIPTION of Python logic, NOT SQL code.
Output ONLY JSON:"""

        debug_log("refine_aggregation", prompt=prompt[:300])
        res = review_llm.invoke([HumanMessage(content=prompt)])
        debug_log("refine_aggregation", response=res.content[:300])

        result = clean_llm_json(res.content)
        if result.get("ok", True):
            return task_plan

        fixed_py = result.get("fixed_python_task", "")
        if fixed_py and fixed_py != python_task:
            parsed["python_task"] = fixed_py
            parsed["needs_python"] = True
            # Also fix sql_task if provided (e.g., remove pre-aggregation filters)
            fixed_sql = result.get("fixed_sql_task", "")
            if fixed_sql and fixed_sql != sql_task:
                parsed["sql_task"] = fixed_sql
                debug_log(
                    "refine_aggregation", action="fixed_sql", fixed=fixed_sql[:80]
                )
            debug_log(
                "refine_aggregation",
                action="fixed",
                original=python_task[:80],
                fixed=fixed_py[:80],
            )
            return json.dumps(parsed, ensure_ascii=False, indent=2)

        return task_plan
    except Exception as e:
        debug_log("refine_aggregation", error=str(e))
        return task_plan


def _detect_composite_pk_tables() -> dict:
    """Detect tables with composite primary keys from schema.
    Returns {table_name: [pk_col1, pk_col2, ...]}"""
    result = {}
    current_table = ""
    pk_cols = []
    in_pk = False

    for line in SCHEMA_INFO.split("\n"):
        lower = line.strip().lower()
        m = re.match(r'\s*create\s+table\s+"?(\w+)"?', lower)
        if m:
            if current_table and len(pk_cols) > 1:
                result[current_table] = pk_cols
            current_table = m.group(1)
            pk_cols = []
            continue

        if current_table:
            # Check for PRIMARY KEY constraint
            if "primary key" in lower:
                # Inline PK: PRIMARY KEY (col1, col2)
                pk_match = re.search(r"primary\s+key\s*\(([^)]+)\)", lower)
                if pk_match:
                    cols = [
                        c.strip().strip('"').lower()
                        for c in pk_match.group(1).split(",")
                    ]
                    if len(cols) > 1:
                        pk_cols = cols

        if lower.startswith(")"):
            if current_table and len(pk_cols) > 1:
                result[current_table] = pk_cols
            current_table = ""
            pk_cols = []

    return result


def _enhance_sql_task(question: str, task_plan: str) -> str:
    """Check sql_task for common issues. Directly fix sql_task/python_task instead of adding notes."""
    try:
        parsed = json.loads(task_plan)
        sql_task = parsed.get("sql_task", "")
        python_task = parsed.get("python_task", "")
        filters = parsed.get("filters", [])
        expected = parsed.get("expected_result", {})

        if not sql_task:
            return task_plan

        prompt = f"""You are reviewing a query plan. Fix any issues you find by rewriting sql_task and python_task.

Question: {question}
sql_task: {sql_task}
python_task: {python_task}
filters: {json.dumps(filters, ensure_ascii=False)[:300]}
expected_result: {json.dumps(expected, ensure_ascii=False)}

Check these issues and FIX them:
1. PERCENTAGE/RATIO: If the question asks for a percentage, sql_task MUST fetch ALL rows (both subset and total), not just the subset. Remove any filter that would limit to only the numerator.
2. DISTINCT: If python_task counts items (districts, accounts, customers), explicitly write "count DISTINCT [column]" in python_task.
3. SORT DIRECTION: youngest = MAX(birth_date), oldest = MIN(birth_date), first = MIN(date), last = MAX(date). Fix if wrong.
4. MISSING COLUMNS: If python_task needs a column not in sql_task, add it to sql_task.

If NO issues found, output: {{"ok": true}}
If issues found, output the FIXED versions:
{{"ok": false, "sql_task": "fixed sql_task text", "python_task": "fixed python_task text"}}
Output ONLY JSON:"""

        debug_log("enhance_sql_task", prompt=prompt[:300])
        res = review_llm.invoke([HumanMessage(content=prompt)])
        debug_log("enhance_sql_task", response=res.content[:200])

        result = clean_llm_json(res.content)
        if result.get("ok", True):
            debug_log("enhance_sql_task", result="no issues")
            return task_plan

        # Apply fixes directly
        new_sql = result.get("sql_task", "")
        new_py = result.get("python_task", "")
        changed = False
        if new_sql and new_sql != sql_task:
            parsed["sql_task"] = new_sql
            debug_log("enhance_sql_task", fixed="sql_task")
            changed = True
        if new_py and new_py != python_task:
            parsed["python_task"] = new_py
            debug_log("enhance_sql_task", fixed="python_task")
            changed = True

        if changed:
            return json.dumps(parsed, ensure_ascii=False, indent=2)
        return task_plan
    except Exception as e:
        debug_log("enhance_sql_task", error=str(e))
        return task_plan
