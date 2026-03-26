"""
SQL 生成與執行節點
"""

from langchain_core.messages import HumanMessage
from sqlalchemy import text as sa_text

from db import engine, SCHEMA_INFO, ENUM_VALUES
from llm import llm
from utils import debug_log, strip_code_fences
from retrieval_subgraph import format_enum_info, build_conditions_context
from domain_rules import DOMAIN_SQL_RULES

# 通用 SQL 生成規則（與業務無關）
_BASE_SQL_RULES = [
    "只生成 SELECT 語句，禁止 DML",
    "SQL 要盡量簡單，只取回答問題所需的原始資料",
    "複雜的計算、排名、比較等邏輯不要放在 SQL 裡，後續會用 Python 處理",
    "適當使用 JOIN 取得需要的欄位，但避免複雜的子查詢或 window function",
    "JOIN 時 SELECT 的欄位必須加上表名前綴，避免 ambiguous column 錯誤",
    "不要加 LIMIT，取回完整資料讓 Python 處理",
    "如果問題是「假設性 / what-if」問題，SQL 必須取回完整資料，讓 Python 做 before/after 比較",
    "WHERE 條件中的值，優先使用下方提供的「已確認精確值」和「已知欄位值」",
    "只輸出純 SQL，不要任何解釋或 markdown 格式",
]


def generate_sql(state):
    conditions_context = build_conditions_context(state)
    enum_info = format_enum_info(ENUM_VALUES)
    sql_error_context = ""
    if state.get("error") and state.get("sql"):
        sql_error_context = (
            f"\n上次生成的 SQL 執行失敗，請根據錯誤訊息修正：\n"
            f"失敗的 SQL：\n{state['sql']}\n錯誤訊息：{state['error']}\n"
        )

    all_rules = _BASE_SQL_RULES + DOMAIN_SQL_RULES
    rules_text = "\n".join(f"- {r}" for r in all_rules)

    prompt = f"""你是一個 SQL 生成助手。根據以下資料庫 schema 和使用者問題，生成一個簡單的 PostgreSQL 查詢。

規則：
{rules_text}

資料庫 Schema：
{SCHEMA_INFO}

{enum_info}

{conditions_context}

使用者問題：{state["question"]}
{sql_error_context}"""
    debug_log("generate_sql", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    sql = strip_code_fences(res.content)
    debug_log("generate_sql", final_sql=sql)
    return {"sql": sql}


def execute_sql(state):
    debug_log("execute_sql", sql=state["sql"])
    try:
        with engine.connect() as conn:
            rp = conn.execute(sa_text(state["sql"]))
            cols = list(rp.keys())
            rows = rp.fetchall()
        result = [dict(zip(cols, r)) for r in rows]
        debug_log("execute_sql", row_count=len(result))
        return {"sql_result": result, "sample": result[:5], "error": ""}
    except Exception as e:
        debug_log("execute_sql", error=str(e))
        return {
            "sql_result": [],
            "sample": [],
            "error": f"SQL 執行錯誤: {str(e)}",
            "sql_retry": state.get("sql_retry", 0) + 1,
        }
