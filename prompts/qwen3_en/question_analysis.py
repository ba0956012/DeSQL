"""QA node prompt templates — qwen3_en (English, Qwen3)"""

QA_JSON_FORMAT = """\
Output pure JSON (no markdown):
{{
  "target": "What the question asks to return",
  "expected_result": {{
    "type": "single_value / list / count / ratio / rank",
    "description": "Expected shape of the final answer"
  }},
  "entities": [
    {{
      "mention": "Entity text from the question",
      "table": "Table name",
      "column": "Column name",
      "value": "Concrete value if any; otherwise null"
    }}
  ],
  "filters": [
    {{
      "description": "Filter condition description",
      "table": "table name",
      "column": "column name",
      "operator": "= / > / < / LIKE / BETWEEN / IN",
      "value": "filter value"
    }}
  ],

Filter rules:
- Only include filters explicitly stated in the question or Hint
- Do not guess or infer implicit conditions
- If unsure whether a filter is needed, do NOT add it (fetch more, let Python filter)
  "tables_needed": ["ONLY tables strictly required — do NOT include extra tables"],
  "join_path": ["TableA.col = TableB.col — ONLY joins that are necessary. If the question asks to list items 'along with' optional info from another table, annotate with (LEFT JOIN) e.g. 'TableA.col = TableB.col (LEFT JOIN)'. Default is INNER JOIN."],
  "sql_task": "Describe what raw data SQL should fetch. IMPORTANT: (1) If the Hint provides a formula, sql_task MUST fetch the raw columns used in that formula, not a pre-computed column with a similar name. (2) If the question mentions a category like 'high schools' or 'continuation schools', include the structured type/category column in WHERE, not just the name column. (3) SELECT all columns that Python will need — do not force Python to guess from names. SQL should fetch ALL rows that might be needed — fetch more rather than less.",
  "needs_python": true,
  "python_task": "Describe what Python should do with the SQL result. If the question or Hint specifies a formula, state it explicitly (e.g., percentage = count_x / count_total * 100). IMPORTANT: Every column you reference here MUST be included in sql_task's SELECT list."
}}"""


def build_qa_prompt(question: str, schema_text: str, schema_desc_section: str, conditions_context: str = "", enum_info: str = "") -> str:
    conditions_section = f"\nConfirmed values from database (use these exact table.column = value in your plan):\n{conditions_context}\n" if conditions_context else ""
    enum_section = f"\nKnown column values (use these to identify the correct column for filtering):\n{enum_info}\n" if enum_info else ""
    return f"""You are a data analysis expert. Analyze the user's question and create a structured query plan.

IMPORTANT guidelines:
- Read the question and Hint VERY carefully before choosing tables and columns.
- If the Hint defines multiple values for a concept (e.g., "status = 'C' means X; status = 'D' means X"), include ALL matching values in your filters, not just the first one.
- If the Hint provides a formula (e.g., X = A / B * 100), your sql_task MUST fetch the raw columns (A, B) so Python can compute X. Do NOT use a pre-computed column just because its name looks similar.
- When multiple tables have columns with similar names, READ the column descriptions carefully to pick the correct one. A column named "district" (text name) is different from "District Code" (numeric code).
- If the question mentions a category (e.g., "high schools", "continuation schools"), find the structured column that represents that category (e.g., School Type, SOCType, Educational Option Type) and use it in sql_task as a WHERE filter. Do NOT leave category filtering to Python via name matching.
- SQL should fetch more data rather than less. Python will handle precise filtering.
- If "Confirmed values" are provided below, use the EXACT table and column specified there. Do NOT use a different table/column for the same value.
- Use "Known column values" below to identify which column contains the values mentioned in the question. If a value appears in a specific column's known values, use THAT column.

Database Schema:
{schema_text}
{schema_desc_section}
{enum_section}
{conditions_section}
User Question: {question}

{QA_JSON_FORMAT}"""


def build_qa_review_prompt(question: str, task_plan: str, schema_text: str) -> str:
    return f"""You are a database expert reviewing a query plan. Check for common mistakes and suggest fixes.

Check these specific issues:
1. TABLE SELECTION: Are the correct tables chosen? If two tables have similar columns (e.g., "district" vs "District Code"), is the right one used?
2. HINT COMPLIANCE: If the question has a Hint with a formula or value mapping, does the plan follow it exactly?
3. FILTER VALUES: Are filter values correct? Check case sensitivity and exact spelling against the schema.
4. JOIN NECESSITY: Are all JOINs actually needed? Are any missing?
5. SQL vs PYTHON split: Does sql_task fetch ALL columns that python_task references?

If the plan looks correct, output exactly: {{"approved": true}}
If there are issues, output: {{"approved": false, "fixes": [{{"field": "sql_task|python_task|filters|tables_needed", "issue": "what is wrong", "fix": "what it should be"}}]}}

User Question: {question}

Query Plan:
{task_plan}

Database Schema (for reference):
{schema_text}

Output pure JSON (no markdown):"""
