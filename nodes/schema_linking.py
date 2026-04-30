"""
Schema Linking 節點

在 SQL 生成之前，根據問題：
1. 篩選出相關的表和欄位（schema linking）
2. 從 BIRD 原始 CSV 動態精煉出該題需要的欄位描述
3. 提供 JOIN 提示和策略建議
"""

import csv
import json
import os
from pathlib import Path

from langchain_core.messages import HumanMessage

from db import SCHEMA_INFO, ENUM_VALUES
from llm import llm
from utils import debug_log, clean_llm_json
from retrieval_subgraph import format_enum_info


# BIRD database_description CSV 目錄
# 透過環境變數 BIRD_DESC_DIR 指定，或自動偵測
_BIRD_DESC_DIR = os.environ.get("BIRD_DESC_DIR", "")


def _load_bird_csv_desc(db_id: str = "") -> str:
    """載入 BIRD 原始 CSV 欄位描述，回傳格式化文字。"""
    # 嘗試多個路徑
    candidates = []
    if _BIRD_DESC_DIR:
        candidates.append(Path(_BIRD_DESC_DIR))
    if db_id:
        candidates.append(Path("eval/databases") / db_id / "database_description")
    
    desc_dir = None
    for p in candidates:
        if p.exists():
            desc_dir = p
            break
    
    if not desc_dir:
        return ""
    
    lines = []
    for csv_file in sorted(desc_dir.glob("*.csv")):
        table_name = csv_file.stem
        table_lines = []
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                col = row.get("original_column_name", "")
                desc = row.get("column_description", "")
                val_desc = row.get("value_description", "")
                if not desc and not val_desc:
                    continue
                parts = [f"  {col}"]
                if desc:
                    parts.append(f": {desc}")
                if val_desc and len(val_desc) < 300:
                    parts.append(f" ({val_desc})")
                table_lines.append("".join(parts))
        if table_lines:
            lines.append(f"Table: {table_name}")
            lines.extend(table_lines)
            lines.append("")
    return "\n".join(lines)


def _get_db_id_from_env() -> str:
    """從 DATABASE_URL 環境變數推斷 db_id。"""
    db_url = os.environ.get("DATABASE_URL", "")
    if "bird_" in db_url:
        return db_url.split("bird_")[-1].split("?")[0]
    return ""


def schema_linking(state):
    """分析問題，同時完成 schema linking 和欄位描述精煉。"""
    question = state["question"]
    enum_info = format_enum_info(ENUM_VALUES)
    
    # 載入 BIRD 原始 CSV 描述
    db_id = _get_db_id_from_env()
    bird_desc = _load_bird_csv_desc(db_id)
    bird_desc_section = f"\n原始欄位描述（來自資料庫文件）：\n{bird_desc}\n" if bird_desc else ""

    prompt = f"""你是一個資料庫專家。根據使用者問題，完成以下兩個任務：

任務 1 - Schema Linking：從完整 schema 中找出回答問題需要的表和欄位。
任務 2 - 欄位描述精煉：從原始欄位描述中，提取出和這個問題相關的關鍵資訊。

完整 Schema：
{SCHEMA_INFO}

{enum_info}
{bird_desc_section}
使用者問題：{question}

只輸出純 JSON：
{{"reasoning": "簡短分析",
  "tables": [
    {{"name": "表名", "columns": ["欄位1", "欄位2"], "role": "主表/關聯表/篩選表"}}
  ],
  "joins": ["表A.col = 表B.col"],
  "strategy": "simple_sql 或 fetch_for_python",
  "column_notes": "和這個問題相關的欄位注意事項（如：哪個欄位代表什麼、容易混淆的欄位、計算公式等）。如果原始描述中沒有有用的資訊，留空字串。"
}}"""

    debug_log("schema_linking", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    debug_log("schema_linking", llm_response=res.content)

    try:
        parsed = clean_llm_json(res.content)
    except (json.JSONDecodeError, KeyError):
        debug_log("schema_linking", error="JSON parse failed, using full schema")
        return {}

    linked_tables = parsed.get("tables", [])
    joins = parsed.get("joins", [])
    strategy = parsed.get("strategy", "")
    reasoning = parsed.get("reasoning", "")
    column_notes = parsed.get("column_notes", "")

    if not linked_tables:
        debug_log("schema_linking", warning="no tables identified, using full schema")
        return {}

    # 構建精簡 schema
    linked_schema = _build_linked_schema(linked_tables)
    # 構建精簡 enum
    table_names = {t["name"].lower() for t in linked_tables}
    linked_enum = _filter_enum_values(table_names)

    # JOIN 提示
    join_hint = ""
    if joins:
        join_hint = "JOIN 關係：\n" + "\n".join(f"  - {j}" for j in joins)

    # 策略提示
    strategy_hint = ""
    if strategy == "simple_sql":
        strategy_hint = "建議策略：SQL 直接用聚合/排序算出答案"
    elif strategy == "fetch_for_python":
        strategy_hint = "建議策略：SQL 取回原始資料，讓 Python 做邏輯運算"

    # 欄位描述注入 schema_desc
    schema_desc = state.get("schema_desc", "")
    if column_notes:
        if schema_desc:
            schema_desc += f"\n\n{column_notes}"
        else:
            schema_desc = column_notes

    debug_log("schema_linking",
              tables=[t["name"] for t in linked_tables],
              strategy=strategy,
              column_notes=column_notes[:200] if column_notes else "")

    return {
        "linked_schema": linked_schema,
        "linked_enum": linked_enum,
        "join_hint": join_hint,
        "strategy_hint": strategy_hint,
        "schema_desc": schema_desc,
    }


def _build_linked_schema(linked_tables: list) -> str:
    """從完整 SCHEMA_INFO 中提取相關表的 DDL。"""
    table_names = {t["name"].lower() for t in linked_tables}
    lines = SCHEMA_INFO.split("\n")
    result = []
    in_relevant_table = False
    current_block = []

    for line in lines:
        lower = line.strip().lower()
        if lower.startswith("create table"):
            parts = lower.replace('"', '').replace('`', '').split()
            if len(parts) >= 3:
                tname = parts[2].rstrip("(").strip()
                in_relevant_table = tname in table_names
            if in_relevant_table:
                current_block = [line]
        elif in_relevant_table:
            current_block.append(line)
            if line.strip().startswith(")") or line.strip() == ")":
                result.extend(current_block)
                result.append("")
                in_relevant_table = False
                current_block = []

    if not result:
        return ""
    return "\n".join(result)


def _filter_enum_values(table_names: set) -> str:
    """只保留相關表的 enum values。"""
    if not ENUM_VALUES:
        return ""
    filtered = {}
    for col_key, vals in ENUM_VALUES.items():
        tname = col_key.split(".")[0].lower()
        if tname in table_names:
            filtered[col_key] = vals
    if not filtered:
        return ""
    lines = ["以下欄位的所有可能值（可直接用於 WHERE 條件）："]
    for col, vals in filtered.items():
        lines.append(f"  {col}: {vals}")
    return "\n".join(lines)
