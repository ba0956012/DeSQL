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
    "SQL 的職責是「取回原始資料」，Python 的職責是「邏輯運算」",
    "核心原則：SQL 取回的資料必須足夠讓後續 Python 程式碼能計算出最終答案",
    "適當使用 JOIN 取得需要的欄位，但避免為了實現邏輯而疊加複雜的子查詢",
    "JOIN 時 SELECT 的欄位必須加上表名前綴，避免 ambiguous column 錯誤",
    "不要加 LIMIT，取回完整資料讓 Python 處理",
    "如果問題是「假設性 / what-if」問題，SQL 必須取回完整資料，讓 Python 做 before/after 比較",
    "WHERE 條件中的值，優先使用下方提供的「已確認精確值」和「已知欄位值」",
    "只輸出純 SQL，不要任何解釋或 markdown 格式",
]


def generate_plan(state):
    """在 SQL 生成前規劃處理策略"""
    conditions_context = build_conditions_context(state)
    enum_info = format_enum_info(ENUM_VALUES)

    prompt = f"""根據使用者問題和資料庫 schema，規劃回答這個問題的處理策略。

資料庫 Schema：
{SCHEMA_INFO}

{enum_info}

{conditions_context}

使用者問題：{state["question"]}

請簡潔回答以下三點（每點一行）：
1. 需要哪些表和欄位
2. SQL 應該取回什麼資料（只取資料，不做複雜邏輯）
3. Python 應該做什麼處理（排序、篩選、計算等）"""

    debug_log("generate_plan", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    plan = res.content.strip()
    debug_log("generate_plan", plan=plan)
    return {"plan": plan}


def generate_sql(state):
    conditions_context = build_conditions_context(state)
    enum_info = format_enum_info(ENUM_VALUES)
    sql_error_context = ""
    if state.get("error") and state.get("sql"):
        sql_error_context = (
            f"\n上次生成的 SQL 執行失敗，請根據錯誤訊息修正：\n"
            f"失敗的 SQL：\n{state['sql']}\n錯誤訊息：{state['error']}\n"
        )
    elif state.get("sql") and not state.get("sql_result") and state.get("sql_retry", 0) > 0:
        sql_error_context = (
            f"\n上次生成的 SQL 執行成功但回傳 0 筆結果，WHERE 條件可能太嚴格或欄位值不匹配。"
            f"\n上次的 SQL：\n{state['sql']}\n"
            f"\n請嘗試以下放寬策略："
            f"\n- 檢查 WHERE 中的值是否與資料庫實際值一致（如大小寫、全名 vs 縮寫）"
            f"\n- 移除不確定的篩選條件，讓 Python 後續處理"
            f"\n- 如果用了 ILIKE，嘗試更寬鬆的匹配"
            f"\n- 記住：後續有 Python 可以做精確篩選，SQL 寧可多取不要漏取"
        )

    all_rules = _BASE_SQL_RULES + DOMAIN_SQL_RULES
    rules_text = "\n".join(f"- {r}" for r in all_rules)

    schema_desc = state.get("schema_desc", "")
    schema_desc_section = f"\n欄位說明：\n{schema_desc}\n" if schema_desc else ""
    plan = state.get("plan", "")
    plan_section = f"\n處理規劃：\n{plan}\n" if plan else ""

    prompt = f"""你是一個 SQL 生成助手。根據以下資料庫 schema 和使用者問題，生成一個簡單的 PostgreSQL 查詢。

規則：
{rules_text}

資料庫 Schema：
{SCHEMA_INFO}
{schema_desc_section}
{enum_info}

{conditions_context}
{plan_section}
使用者問題：{state["question"]}
{sql_error_context}請先用一行註解簡述：Python 需要哪些欄位和資料才能算出答案？然後再寫 SQL。
寫完後自我檢查：這份 SQL 取回的資料，是否足夠讓 Python 做出完整判斷？如果 WHERE 條件過濾掉了 Python 判斷所需的資料，請修正後再輸出最終 SQL。只輸出最終版本。"""
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
