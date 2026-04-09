"""
問題分解與 Schema Mapping 節點

在 SQL 生成之前，對問題進行結構化分解：
1. 提取目標（要回傳什麼）
2. 提取篩選條件（WHERE）
3. 提取計算邏輯（聚合、排序、比較）
4. 對應到具體的表和欄位
"""

import json
from langchain_core.messages import HumanMessage

from db import SCHEMA_INFO
from llm import llm
from utils import debug_log, clean_llm_json


def _embed_descs(schema_info, column_descs):
    """把欄位描述嵌入 DDL（複用 sql.py 的邏輯）"""
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


def question_analysis(state):
    """分解問題，生成結構化的 task_plan 供 SQL 和 Python 使用。"""
    question = state["question"]
    schema_desc = state.get("schema_desc", "")
    schema_desc_section = f"\n欄位說明：\n{schema_desc}\n" if schema_desc else ""

    # 如果有 column_descs，嵌入 DDL
    column_descs = state.get("column_descs")
    schema_text = _embed_descs(SCHEMA_INFO, column_descs) if column_descs else SCHEMA_INFO

    prompt = f"""你是一個資料分析專家。請仔細分析使用者的問題，將其分解為結構化的查詢計畫。

資料庫 Schema：
{schema_text}
{schema_desc_section}

使用者問題：{question}

請分析並輸出純 JSON（不要 markdown）：
{{
  "target": "問題要求回傳什麼（如：出生年份、帳戶數量、百分比等）",
  "expected_result": {{
    "type": "single_value / list / count / ratio / rank",
    "description": "預期最終答案的樣態（如：一個數字、一個名稱、一組 ID 列表等）"
  }},
  "entities": [
    {{
      "mention": "問題中提到的實體原文",
      "table": "對應的表名",
      "column": "對應的欄位名",
      "value": "如果有具體值，寫出來；否則 null"
    }}
  ],
  "filters": [
    {{
      "description": "篩選條件的自然語言描述",
      "table": "表名",
      "column": "欄位名",
      "operator": "= / > / < / LIKE / BETWEEN / IN",
      "value": "篩選值"
    }}
  ],

篩選條件規則：
- 只列出問題或 Hint 中明確提到的篩選條件和具體值
- 不要根據自然語言推測隱含的篩選條件
- 如果不確定某個條件是否需要，不要加（寧可多取資料讓 Python 篩選）
  "tables_needed": ["需要的表名列表"],
  "join_path": ["表A.col = 表B.col 的 JOIN 路徑"],
  "sql_task": "SQL 的具體任務：描述 SQL 應該取回什麼資料。原則：SQL 寧可多取不要漏取，用簡單的 JOIN + WHERE 取回原始資料，讓 Python 做精確篩選和計算。避免在 SQL 中使用複雜的子查詢、自連接或精確日期匹配。",
  "needs_python": true,
  "python_task": "Python 的具體任務：描述 Python 要從 SQL 結果中做什麼處理（如：篩選特定條件、計算百分比、找最大值等）。Python 也負責整理最終結果的格式。"
}}"""

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

    debug_log("question_analysis", task_plan=task_plan[:300], needs_python=needs_python)
    return {"task_plan": task_plan, "qa_needs_python": needs_python}
