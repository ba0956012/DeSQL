"""
SQL 生成與執行節點
"""

import json
import os
from langchain_core.messages import HumanMessage
from sqlalchemy import text as sa_text

from db import engine, SCHEMA_INFO, ENUM_VALUES
from llm import llm
from utils import debug_log, strip_code_fences, clean_llm_json
from retrieval_subgraph import format_enum_info, build_conditions_context
from domain_rules import DOMAIN_SQL_RULES

# 通用 SQL 生成規則（與業務無關）
_BASE_SQL_RULES = [
    "只生成 SELECT 語句，禁止 DML",
    "根據問題複雜度選擇 SQL 策略：",
    "  - 簡單查詢（單一數值、排名、TOP N）：SQL 直接用 GROUP BY / ORDER BY / LIMIT / 聚合函數算出答案，不需要 Python 後處理",
    "  - 複雜分析（多步驟計算、條件比較、what-if）：SQL 取回原始資料，讓 Python 做邏輯運算",
    "核心原則：SQL 取回的資料必須足夠讓後續流程能得出最終答案",
    "適當使用 JOIN 取得需要的欄位，但避免為了實現邏輯而疊加複雜的子查詢",
    "JOIN 時 SELECT 的欄位必須加上表名前綴，避免 ambiguous column 錯誤",
    "如果問題問的是排名或 TOP N，SQL 應使用 ORDER BY + LIMIT 直接取出結果",
    "如果問題問的是比例或百分比，SQL 應取回分子和分母所需的完整資料（不要只取其中一邊）",
    "WHERE 條件寧鬆勿嚴：不確定的篩選條件不要加，讓 Python 後續處理",
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
        validation = state.get("sql_validation", "")
        if validation:
            sql_error_context = (
                f"\n上次的 SQL 結果不完整，缺少：{validation}"
                f"\n上次的 SQL：\n{state['sql']}\n"
                f"\n請修正 SQL 加入缺少的欄位/資料。記住：Python 可以後續做篩選，SQL 應取回所有需要的資料。"
            )
        else:
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


_VALIDATE_PROMPT_LEGACY = """You are a SQL result validator. Check if the SQL query result contains sufficient data for Python to answer the question.

Question: {question}
SQL: {sql}
Row count: {row_count}
Result sample:
{sample_str}

Check:
1. Does the result contain all columns needed to answer the question?
2. Are there any important columns missing that Python would need?
3. Does the WHERE clause filter out data that Python needs for comparison/calculation?

Output pure JSON only:
{{"sufficient": true/false, "missing": "what is missing, if any"}}"""

_VALIDATE_PROMPT_V2 = """你是一個基礎資料科學家，這次的任務是負責檢視SQL查詢到的資料是否足夠用於後續分析。

重要原則：資料寧多勿少。如果不確定資料是否足夠，就判定為 sufficient。只有在你非常確定缺少關鍵資料時，才判定為 insufficient。

分析需求（使用者問題）：{question}

資料來源 SQL：
{sql}

取得的資料筆數：{row_count}
資料樣本：
{sample_str}

請從資料分析的角度檢視：
1. 如果問題附帶了 (Hint: ...) 提示，裡面提到的欄位或計算公式，在取回的資料中是否有對應的欄位可以使用？
2. 問題中提到的篩選範圍（如特定地區、時間、類別），SQL 是否有做對應的篩選？如果沒篩選但資料全部取回了，那也沒問題（後續 Python 可以處理）。

以下情況資料是足夠的，不需要重新取資料：
- 資料比需要的多（後續分析可以再篩選）
- 欄位名稱和問題用詞不完全一致（只要能對應就行）
- 資料筆數看起來偏多或偏少（可能是正常的）

只輸出純 JSON：
{{"reasoning": "先說明你的判斷理由。如果資料不足，具體說明缺少什麼", "sufficient": true/false}}"""


def validate_sql_result(state):
    """檢查 SQL 結果是否包含足夠的資料來回答問題。
    
    透過環境變數 VALIDATE_PROMPT 切換版本：
      - "legacy" 或未設定：使用舊版英文 prompt（只檢查欄位）
      - "v2"：使用新版中文 prompt（全面檢查）
    """
    sql_result = state.get("sql_result", [])
    if not sql_result or state.get("error"):
        return {}

    sample_str = json.dumps(state.get("sample", [])[:5], indent=2, ensure_ascii=False, default=str)

    version = os.environ.get("VALIDATE_PROMPT", "legacy")
    template = _VALIDATE_PROMPT_V2 if version == "v2" else _VALIDATE_PROMPT_LEGACY
    prompt = template.format(
        question=state["question"],
        sql=state.get("sql", ""),
        row_count=len(sql_result),
        sample_str=sample_str,
    )

    debug_log("validate_sql_result", prompt=prompt)
    res = llm.invoke([HumanMessage(content=prompt)])
    try:
        parsed = clean_llm_json(res.content)
        sufficient = parsed.get("sufficient", True)
        missing = parsed.get("reasoning", "") or parsed.get("missing", "")
    except (json.JSONDecodeError, KeyError):
        sufficient = True
        missing = ""

    debug_log("validate_sql_result", sufficient=sufficient, missing=missing)

    if not sufficient and missing and state.get("sql_retry", 0) < 2:
        return {
            "error": f"SQL result incomplete: {missing}",
            "sql_retry": state.get("sql_retry", 0) + 1,
            "sql_validation": missing,
        }
    return {"sql_validation": ""}


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
