"""Schema filter node prompt templates — qwen3_en (English, Qwen3)"""


def build_schema_filter_prompt(question: str, table: str, desc_text: str) -> str:
    return f"""Given the user's question, identify columns from the table below that are DEFINITELY NOT needed.

Rules:
- Only exclude columns you are CERTAIN are irrelevant to the question.
- If there is ANY chance a column might be useful, do NOT exclude it.
- Never exclude ID columns, key columns, or columns that could be used for JOINs.
- Never exclude columns whose names or descriptions have even a loose connection to the question.
- When in doubt, keep the column (do not list it in exclude).

User Question: {question}

Table {table} columns:
{desc_text}

Output pure JSON (no markdown):
{{"exclude": ["col1", "col2", ...]}}
If nothing should be excluded, output: {{"exclude": []}}"""


def build_desc_filter_prompt(question: str, compact_desc: str) -> str:
    return f"""Given the user's question, extract ONLY the lines from the schema notes below that are relevant to answering this question.

Rules:
- Always include JOIN key lines that connect tables needed for the question
- Include column clarifications only if the question touches those columns
- Include value mappings only if the question mentions those values or categories
- Remove lines about unrelated tables or columns
- Keep the -- prefix format, output the selected lines directly (no JSON, no markdown)
- If nothing is relevant, output just the JOIN key lines

User Question: {question}

Schema Notes:
{compact_desc}

Output only the relevant lines:"""
