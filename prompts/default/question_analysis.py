"""QA node prompt templates — default (中文, gpt-4.1-mini)"""

QA_JSON_FORMAT = """\
請分析並輸出純 JSON（不要 markdown）：
{{
  "target": "問題要求回傳什麼",
  "expected_result": {{
    "type": "single_value / list / count / ratio / rank",
    "description": "預期最終答案的樣態"
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
  "python_task": "Python 的具體任務：描述 Python 要從 SQL 結果中做什麼處理。Python 也負責整理最終結果的格式。"
}}"""


def build_qa_prompt(question: str, schema_text: str, schema_desc_section: str, conditions_context: str = "", enum_info: str = "") -> str:
    return f"""你是一個資料分析專家。請分解問題為結構化的查詢計畫。

資料庫 Schema：
{schema_text}
{schema_desc_section}

使用者問題：{question}

{QA_JSON_FORMAT}"""


def build_qa_review_prompt(question: str, task_plan: str, schema_text: str) -> str:
    return f"""你是一個資料庫專家，正在審查一個查詢計畫。請檢查常見錯誤並建議修正。

檢查以下問題：
1. 表選擇：是否選了正確的表？如果兩個表有相似欄位，是否用了正確的那個？
2. Hint 遵循：如果問題有 Hint 提供公式或值對應，計畫是否完全遵循？
3. 篩選值：篩選值是否正確？檢查大小寫和拼寫。
4. JOIN 必要性：所有 JOIN 都需要嗎？有沒有遺漏？
5. SQL/Python 分工：sql_task 是否取回了 python_task 需要的所有欄位？

如果計畫正確，輸出：{{"approved": true}}
如果有問題，輸出：{{"approved": false, "fixes": [{{"field": "sql_task|python_task|filters|tables_needed", "issue": "問題描述", "fix": "修正建議"}}]}}

使用者問題：{question}

查詢計畫：
{task_plan}

資料庫 Schema（參考用）：
{schema_text}

只輸出純 JSON（不要 markdown）："""
