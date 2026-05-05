"""
LLM-based Schema Filter Node

自底向上策略：
1. 對所有表做欄位過濾（LLM 選出問題相關的欄位）
2. 如果某表沒有被選中任何非 PK/FK 欄位 → 標記為候選移除
3. 候選移除的表如果在 FK 連通路徑上（連接有用的表）→ 保留
4. 其餘候選移除的表 → 從 DDL 和 column_descs 中移除
"""

import re
import os

from langchain_core.messages import HumanMessage

from db import SCHEMA_INFO
from llm import schema_llm as llm
from utils import debug_log, clean_llm_json
from prompts import load_profile

COL_THRESHOLD = 15  # 大表欄位過濾門檻（basic 模式）
TABLE_THRESHOLD = 4  # 表級移除門檻（advanced 模式）
SCHEMA_FILTER_MODE = os.getenv("SCHEMA_FILTER_MODE", "basic")  # basic | advanced


def _parse_table_columns(schema_info: str):
    """解析 DDL，回傳 {table: [columns]} 和 {table: set(pk/fk cols)}"""
    table_cols = {}
    pk_fk = {}
    current_table = ""
    for line in schema_info.split("\n"):
        lower = line.strip().lower()
        m = re.match(r'\s*create\s+table\s+"?(\w+)"?', lower)
        if m:
            current_table = m.group(1)
            table_cols[current_table] = []
            pk_fk.setdefault(current_table, set())
            continue
        if current_table and "primary key" in lower:
            pk_m = re.search(r"primary\s+key\s*\(([^)]+)\)", lower)
            if pk_m:
                for col in pk_m.group(1).split(","):
                    pk_fk[current_table].add(col.strip().strip('"').lower())
        if current_table and "foreign key" in lower:
            fk_m = re.search(r"foreign\s+key\s*\(([^)]+)\)", lower)
            if fk_m:
                for col in fk_m.group(1).split(","):
                    pk_fk[current_table].add(col.strip().strip('"').lower())
        if current_table and (line.startswith("\t") or line.startswith("    ")):
            if not lower.startswith("constraint") and not lower.startswith(")"):
                col_m = re.match(r'\s+"?(\w+)"?\s+', line)
                if col_m:
                    table_cols[current_table].append(col_m.group(1).lower())
        if lower.startswith(")"):
            current_table = ""
    return table_cols, pk_fk


def _parse_fk_graph(schema_info: str):
    """從 DDL 解析 FK 關係，建立無向圖 {table: set(connected_tables)}"""
    graph = {}
    current_table = ""
    for line in schema_info.split("\n"):
        lower = line.strip().lower()
        m = re.match(r'\s*create\s+table\s+"?(\w+)"?', lower)
        if m:
            current_table = m.group(1)
            graph.setdefault(current_table, set())
            continue
        if current_table and "references" in lower:
            ref_m = re.search(r'references\s+"?(\w+)"?', lower)
            if ref_m:
                ref_table = ref_m.group(1)
                graph.setdefault(ref_table, set())
                graph[current_table].add(ref_table)
                graph[ref_table].add(current_table)
        if lower.startswith(")"):
            current_table = ""
    return graph


def _is_on_path(graph: dict, bridge_candidate: str, useful_tables: set) -> bool:
    """檢查 bridge_candidate 是否在任意兩個 useful_tables 之間的 FK 路徑上"""
    useful_list = list(useful_tables)
    for i in range(len(useful_list)):
        for j in range(i + 1, len(useful_list)):
            src, dst = useful_list[i], useful_list[j]
            # BFS from src to dst, check if path goes through bridge_candidate
            if src not in graph or dst not in graph:
                continue
            visited = {src}
            queue = [(src, [src])]
            while queue:
                node, path = queue.pop(0)
                for neighbor in graph.get(node, set()):
                    if neighbor in visited:
                        continue
                    new_path = path + [neighbor]
                    if neighbor == dst:
                        if bridge_candidate in new_path[1:-1]:
                            return True
                        break
                    visited.add(neighbor)
                    queue.append((neighbor, new_path))
    return False


def _build_desc_text(table: str, columns: list, column_descs: dict) -> str:
    lines = []
    for col in columns:
        key = f"{table}.{col}"
        desc = column_descs.get(key, "")
        if desc:
            lines.append(f"  - {col}: {desc}")
        else:
            lines.append(f"  - {col}")
    return "\n".join(lines)


def _filter_ddl(
    schema_info: str, keep_tables: set = None, keep_cols: dict = None
) -> str:
    """過濾 DDL：移除不需要的表，過濾欄位"""
    lines = schema_info.split("\n")
    result = []
    current_table = ""
    skip_table = False
    for line in lines:
        lower = line.strip().lower()
        m = re.match(r'\s*create\s+table\s+"?(\w+)"?', lower)
        if m:
            current_table = m.group(1)
            skip_table = keep_tables is not None and current_table not in keep_tables
            if not skip_table:
                result.append(line)
            continue
        if lower.startswith(")"):
            if not skip_table:
                result.append(line)
            current_table = ""
            skip_table = False
            continue
        if skip_table:
            continue
        if lower.startswith("constraint"):
            result.append(line)
            continue
        if current_table and (line.startswith("\t") or line.startswith("    ")):
            col_m = re.match(r'\s+"?(\w+)"?\s+', line)
            if col_m:
                col_name = col_m.group(1).lower()
                if keep_cols and current_table in keep_cols:
                    if col_name in keep_cols[current_table]:
                        result.append(line)
                else:
                    result.append(line)
                continue
        if not skip_table:
            result.append(line)
    return "\n".join(result)


def _build_unique_columns_hint(
    table_cols: dict, pk_fk: dict, column_descs: dict
) -> str:
    """Auto-detect columns unique to each table (excluding PK/FK). Returns a hint string."""
    if len(table_cols) < 2:
        return ""

    # Collect non-PK/FK columns per table
    table_non_key_cols = {}
    for table, cols in table_cols.items():
        key_cols = pk_fk.get(table, set())
        non_key = [c for c in cols if c not in key_cols]
        table_non_key_cols[table] = set(non_key)

    # Find columns that exist in only one table (by name)
    all_col_names = {}  # col_name -> set of tables
    for table, cols in table_non_key_cols.items():
        for col in cols:
            all_col_names.setdefault(col, set()).add(table)

    # Build per-table unique columns list (skip if too many — not useful)
    lines = []
    for table in sorted(table_cols.keys()):
        unique_cols = []
        for col in sorted(table_non_key_cols.get(table, [])):
            if len(all_col_names.get(col, set())) == 1:
                desc = column_descs.get(f"{table}.{col}", "")
                if desc:
                    unique_cols.append(f"{col} ({desc[:50]})")
                else:
                    unique_cols.append(col)
        if unique_cols and len(unique_cols) <= 15:
            lines.append(f"  {table} ONLY: {', '.join(unique_cols)}")

    if not lines:
        return ""

    return (
        "Unique columns per table (only in this table, not in others):\n"
        + "\n".join(lines)
    )


def schema_filter(state):
    """Schema 過濾：
    - basic（預設）：只對大表（>COL_THRESHOLD 欄）用 LLM 過濾欄位
    - advanced：對所有表排除無關欄位 + 自底向上表級移除
    """
    profile = load_profile()
    column_descs = state.get("column_descs")

    # Pre-compute conditions_context for QA node
    # Include both enum matches and keyword search results (confirmed table.column locations)
    conditions = state.get("conditions", [])
    retrieved = state.get("retrieved_docs", [])
    lines = []
    for c in conditions:
        if c.get("type") == "enum":
            t = c.get("table")
            col = c.get("column")
            val = c.get("value")
            if t and col and val:
                lines.append(f"  {t}.{col} = '{val}'")
        elif c.get("type") == "keyword" and retrieved:
            tbl = c.get("table", "")
            col = c.get("column", "")
            kw = c.get("keyword", "")
            # Use the actual search_table/column from state (may have been corrected by broad search)
            actual_tbl = state.get("search_table", tbl)
            actual_col = state.get("search_column", col)
            lines.append(f"  '{kw}' found in {actual_tbl}.{actual_col}")
    conditions_ctx = "\n".join(lines) if lines else ""

    if not column_descs:
        return {"filtered_schema": "", "_conditions_context": conditions_ctx}

    question = state["question"]
    table_cols, pk_fk = _parse_table_columns(SCHEMA_INFO)
    all_tables = set(table_cols.keys())

    retrieval_cols = set()
    for cond in state.get("conditions", []):
        t = cond.get("table", "").lower()
        c = cond.get("column", "").lower()
        if t and c:
            retrieval_cols.add(f"{t}.{c}")

    if SCHEMA_FILTER_MODE == "advanced":
        result = _filter_advanced(
            state,
            profile,
            question,
            table_cols,
            pk_fk,
            all_tables,
            retrieval_cols,
            column_descs,
        )
    else:
        result = _filter_big_tables(
            state, profile, question, table_cols, pk_fk, retrieval_cols, column_descs
        )
    result["_conditions_context"] = conditions_ctx
    return result


def _filter_big_tables(
    state, profile, question, table_cols, pk_fk, retrieval_cols, column_descs
):
    """v29 行為：只對大表（>COL_THRESHOLD）用 LLM 選相關欄位"""
    big_tables = {
        t: cols for t, cols in table_cols.items() if len(cols) > COL_THRESHOLD
    }
    if not big_tables:
        debug_log("schema_filter", skip="no big tables")
        return {"filtered_schema": ""}

    keep_cols = {}
    for table, cols in big_tables.items():
        desc_text = _build_desc_text(table, cols, column_descs)
        prompt = profile.build_schema_filter_prompt(question, table, desc_text)

        debug_log("schema_filter", table=table, col_count=len(cols))
        try:
            res = llm.invoke([HumanMessage(content=prompt)])
            parsed = clean_llm_json(res.content)
            # 支援兩種格式：exclude（新）或 columns（舊）
            if "exclude" in parsed:
                exclude_cols = set(c.lower() for c in parsed["exclude"])
                llm_cols = set(c.lower() for c in cols) - exclude_cols
            else:
                llm_cols = set(c.lower() for c in parsed.get("columns", []))
        except Exception as e:
            debug_log("schema_filter", error=str(e))
            llm_cols = set()

        final = set(pk_fk.get(table, set()))
        final |= llm_cols
        for rc in retrieval_cols:
            parts = rc.split(".", 1)
            if len(parts) == 2 and parts[0] == table:
                final.add(parts[1])

        keep_cols[table] = final
        debug_log("schema_filter", table=table, original=len(cols), kept=len(final))

    filtered = _filter_ddl(SCHEMA_INFO, keep_cols=keep_cols)

    # 過濾 column_descs
    if keep_cols and column_descs:
        filtered_descs = {}
        for key, desc in column_descs.items():
            table, col = key.split(".", 1) if "." in key else ("", key)
            if table in keep_cols:
                if col in keep_cols[table]:
                    filtered_descs[key] = desc
            else:
                filtered_descs[key] = desc
        debug_log(
            "schema_filter",
            original_descs=len(column_descs),
            filtered_descs=len(filtered_descs),
        )
        return {"filtered_schema": filtered, "column_descs": filtered_descs}

    return {"filtered_schema": filtered}


def _filter_advanced(
    state,
    profile,
    question,
    table_cols,
    pk_fk,
    all_tables,
    retrieval_cols,
    column_descs,
):
    """進階模式：對所有表排除無關欄位 + 自底向上表級移除"""
    keep_cols = {}
    useful_cols = {}

    for table, cols in table_cols.items():
        desc_text = _build_desc_text(table, cols, column_descs)
        prompt = profile.build_schema_filter_prompt(question, table, desc_text)

        debug_log("schema_filter", phase="col_filter", table=table, col_count=len(cols))
        try:
            res = llm.invoke([HumanMessage(content=prompt)])
            parsed = clean_llm_json(res.content)
            if "exclude" in parsed:
                exclude_cols = set(c.lower() for c in parsed["exclude"])
                llm_cols = set(c.lower() for c in cols) - exclude_cols
            else:
                llm_cols = set(c.lower() for c in parsed.get("columns", []))
        except Exception as e:
            debug_log("schema_filter", phase="col_filter", error=str(e))
            llm_cols = set(c.lower() for c in cols)

        final = set(pk_fk.get(table, set()))
        final |= llm_cols
        for rc in retrieval_cols:
            parts = rc.split(".", 1)
            if len(parts) == 2 and parts[0] == table:
                final.add(parts[1])

        keep_cols[table] = final
        non_pkfk = llm_cols - pk_fk.get(table, set())
        for rc in retrieval_cols:
            parts = rc.split(".", 1)
            if len(parts) == 2 and parts[0] == table:
                non_pkfk.add(parts[1])
        useful_cols[table] = non_pkfk

        debug_log(
            "schema_filter",
            phase="col_filter",
            table=table,
            original=len(cols),
            kept=len(final),
            useful=len(non_pkfk),
        )

    # 表級移除
    keep_tables = None
    if len(all_tables) > TABLE_THRESHOLD:
        tables_with_useful_cols = {t for t, cols in useful_cols.items() if cols}
        candidates_to_remove = all_tables - tables_with_useful_cols

        if candidates_to_remove:
            fk_graph = _parse_fk_graph(SCHEMA_INFO)
            keep_tables = set(tables_with_useful_cols)

            for table in candidates_to_remove:
                if _is_on_path(fk_graph, table, tables_with_useful_cols):
                    keep_tables.add(table)
                    debug_log(
                        "schema_filter",
                        phase="table_prune",
                        table=table,
                        action="keep (bridge)",
                    )
                else:
                    debug_log(
                        "schema_filter",
                        phase="table_prune",
                        table=table,
                        action="remove",
                    )

            if keep_tables == all_tables:
                keep_tables = None

    filtered = _filter_ddl(SCHEMA_INFO, keep_tables=keep_tables, keep_cols=keep_cols)

    if column_descs:
        filtered_descs = {}
        for key, desc in column_descs.items():
            table, col = key.split(".", 1) if "." in key else ("", key)
            if keep_tables and table not in keep_tables:
                continue
            if table in keep_cols:
                if col in keep_cols[table]:
                    filtered_descs[key] = desc
            else:
                filtered_descs[key] = desc
        debug_log(
            "schema_filter",
            original_descs=len(column_descs),
            filtered_descs=len(filtered_descs),
        )
        return {"filtered_schema": filtered, "column_descs": filtered_descs}

    return {"filtered_schema": filtered}
