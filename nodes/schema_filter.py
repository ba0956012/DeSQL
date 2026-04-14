"""
LLM-based Schema Filter Node

在 retrieval 之後、QA 之前，用 LLM 根據問題和欄位描述
篩選出相關的欄位，減少大表的 schema noise。

只對大表（>threshold 欄位）啟用過濾。
保留規則：
1. PK / FK 欄位（永遠保留）
2. LLM 判斷為相關的欄位
3. retrieval_subgraph 發現的欄位
"""

import re

from langchain_core.messages import HumanMessage

from db import SCHEMA_INFO
from llm import llm
from utils import debug_log, clean_llm_json

# 欄位數超過此值的表才啟用過濾
COL_THRESHOLD = 15


def _parse_table_columns(schema_info: str):
    """解析 DDL，回傳 {table: [col_name, ...]} 和 {table: set(pk_fk_cols)}"""
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
            pk_m = re.search(r'primary\s+key\s*\(([^)]+)\)', lower)
            if pk_m:
                for col in pk_m.group(1).split(","):
                    pk_fk[current_table].add(col.strip().strip('"').lower())

        if current_table and "foreign key" in lower:
            fk_m = re.search(r'foreign\s+key\s*\(([^)]+)\)', lower)
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


def _build_desc_text(table: str, columns: list, column_descs: dict) -> str:
    """為一個表建立欄位描述文字，供 LLM 判斷"""
    lines = []
    for col in columns:
        key = f"{table}.{col}"
        desc = column_descs.get(key, "")
        if desc:
            lines.append(f"  - {col}: {desc}")
        else:
            lines.append(f"  - {col}")
    return "\n".join(lines)


def _filter_ddl(schema_info: str, keep_cols: dict) -> str:
    """根據 keep_cols {table: set(col)} 過濾 DDL，只保留指定欄位"""
    lines = schema_info.split("\n")
    result = []
    current_table = ""

    for line in lines:
        lower = line.strip().lower()

        m = re.match(r'\s*create\s+table\s+"?(\w+)"?', lower)
        if m:
            current_table = m.group(1)
            result.append(line)
            continue

        # CONSTRAINT 或表結束 → 保留
        if lower.startswith("constraint") or lower.startswith(")"):
            if lower.startswith(")"):
                current_table = ""
            result.append(line)
            continue

        # 欄位定義行
        if current_table and (line.startswith("\t") or line.startswith("    ")):
            col_m = re.match(r'\s+"?(\w+)"?\s+', line)
            if col_m:
                col_name = col_m.group(1).lower()
                if current_table not in keep_cols:
                    # 不在過濾名單中的表，全部保留
                    result.append(line)
                elif col_name in keep_cols[current_table]:
                    result.append(line)
                # else: skip this column
                continue

        result.append(line)

    return "\n".join(result)


def schema_filter(state):
    """用 LLM 篩選大表的相關欄位，產生 filtered_schema"""
    column_descs = state.get("column_descs")
    if not column_descs:
        return {"filtered_schema": ""}

    question = state["question"]
    table_cols, pk_fk = _parse_table_columns(SCHEMA_INFO)

    # 找出需要過濾的大表
    big_tables = {t: cols for t, cols in table_cols.items() if len(cols) > COL_THRESHOLD}
    if not big_tables:
        debug_log("schema_filter", skip="no big tables")
        return {"filtered_schema": ""}

    # 從 retrieval conditions 提取已發現的欄位
    retrieval_cols = set()
    for cond in state.get("conditions", []):
        t = cond.get("table", "").lower()
        c = cond.get("column", "").lower()
        if t and c:
            retrieval_cols.add(f"{t}.{c}")

    # 對每個大表，用 LLM 判斷相關欄位
    keep_cols = {}  # {table: set(col)}

    for table, cols in big_tables.items():
        desc_text = _build_desc_text(table, cols, column_descs)

        prompt = f"""你是一個資料庫專家。根據使用者的問題，從以下表的欄位中選出可能需要用到的欄位。

規則：
- 寧可多選不要漏選（後續還有其他步驟會精確篩選）
- 如果不確定某個欄位是否需要，就選上
- 只需要選出欄位名稱，不需要解釋

使用者問題：{question}

表 {table} 的欄位：
{desc_text}

只輸出純 JSON（不要 markdown）：
{{"columns": ["col1", "col2", ...]}}"""

        debug_log("schema_filter", table=table, col_count=len(cols))
        try:
            res = llm.invoke([HumanMessage(content=prompt)])
            parsed = clean_llm_json(res.content)
            llm_cols = set(c.lower() for c in parsed.get("columns", []))
        except Exception as e:
            debug_log("schema_filter", error=str(e))
            llm_cols = set()

        # 合併：PK/FK + LLM 選的 + retrieval 發現的
        final = set(pk_fk.get(table, set()))
        final |= llm_cols
        for rc in retrieval_cols:
            parts = rc.split(".", 1)
            if len(parts) == 2 and parts[0] == table:
                final.add(parts[1])

        keep_cols[table] = final
        debug_log("schema_filter", table=table,
                  original=len(cols), kept=len(final),
                  llm_selected=len(llm_cols))

    filtered = _filter_ddl(SCHEMA_INFO, keep_cols)
    return {"filtered_schema": filtered}
