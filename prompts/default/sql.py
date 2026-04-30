"""SQL node prompt templates — default (中文, gpt-4.1-mini)"""

SQL_RULES = [
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


def build_task_section(sql_task: str, tables_info: str, join_info: str) -> str:
    return f"""
=== SQL 任務（最重要，請嚴格遵循）===
{sql_task}
{tables_info}
{join_info}
核心原則：SQL 只負責取回資料，不要用複雜的子查詢或自連接。後續有 Python 可以做精確篩選和計算。
===
"""


def build_sql_prompt_with_task(task_section, rules_text, schema_text, schema_desc_section, enum_info, conditions_context, sql_error_context) -> str:
    return f"""根據以下 SQL 任務指令和資料庫 schema，生成一個 PostgreSQL SELECT 查詢。
{task_section}
規則：
{rules_text}

資料庫 Schema：
{schema_text}
{schema_desc_section}
{enum_info}

{conditions_context}
{sql_error_context}
只輸出純 SQL，不要任何解釋。"""


def build_sql_prompt_no_task(question, rules_text, schema_text, schema_desc_section, enum_info, conditions_context, sql_error_context) -> str:
    return f"""根據使用者問題和資料庫 schema，生成一個簡單的 PostgreSQL SELECT 查詢。

規則：
{rules_text}

資料庫 Schema：
{schema_text}
{schema_desc_section}
{enum_info}

{conditions_context}

使用者問題：{question}
{sql_error_context}
只輸出 SQL，不要其他格式。"""


def build_error_context_validation(validation, prev_sql) -> str:
    return (
        f"\n上次的 SQL 結果不完整，缺少：{validation}"
        f"\n上次的 SQL：\n{prev_sql}\n"
        f"\n請修正 SQL 加入缺少的欄位/資料。記住：Python 可以後續做篩選，SQL 應取回所有需要的資料。"
    )


def build_error_context_failed(prev_sql, error) -> str:
    return (
        f"\n上次生成的 SQL 執行失敗，請根據錯誤訊息修正：\n"
        f"失敗的 SQL：\n{prev_sql}\n錯誤訊息：{error}\n"
    )


def build_error_context_empty(prev_sql) -> str:
    return (
        f"\n上次生成的 SQL 執行成功但回傳 0 筆結果，WHERE 條件可能太嚴格或欄位值不匹配。"
        f"\n上次的 SQL：\n{prev_sql}\n"
        f"\n請嘗試以下放寬策略："
        f"\n- 檢查 WHERE 中的值是否與資料庫實際值一致（如大小寫、全名 vs 縮寫）"
        f"\n- 移除不確定的篩選條件，讓 Python 後續處理"
        f"\n- 如果用了 ILIKE，嘗試更寬鬆的匹配"
        f"\n- 記住：後續有 Python 可以做精確篩選，SQL 寧可多取不要漏取"
    )
