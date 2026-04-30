"""SQL node prompt templates — qwen3_en (English, Qwen3)"""

SQL_RULES = [
    "Generate SELECT statements only. No DML.",
    "Choose SQL strategy based on complexity:",
    "  - Simple queries (single value, rank, TOP N): use GROUP BY / ORDER BY / LIMIT / aggregates directly",
    "  - Complex analysis (multi-step, comparison, ratio): fetch raw data, let Python compute",
    "CRITICAL: SQL must fetch ALL data needed for the final answer. Fetch more rather than less.",
    "SELECT all columns that Python will need for filtering or calculation. If the task mentions a column, include it in SELECT.",
    "Only JOIN tables that are strictly necessary. Do NOT add extra JOINs for columns not needed.",
    "Always prefix columns with table name in JOINs to avoid ambiguous column errors",
    "For ranking or TOP N questions, use ORDER BY + LIMIT",
    "For ratio or percentage questions, fetch both numerator and denominator data",
    "WHERE conditions: prefer loose over strict. If unsure about a filter, omit it and let Python handle it",
    "For WHERE values, prefer the confirmed exact values and known column values provided below",
    "Output only raw SQL. No explanation, no markdown.",
]


def build_task_section(sql_task: str, tables_info: str, join_info: str) -> str:
    return f"""
=== SQL Task (highest priority, follow strictly) ===
{sql_task}
{tables_info}
{join_info}
IMPORTANT: Only JOIN the tables listed above. Do NOT add extra tables or JOINs.
SQL retrieves data only. No complex subqueries or self-joins. Python handles filtering and calculation.
===
"""


def build_sql_prompt_with_task(task_section, rules_text, schema_text, schema_desc_section, enum_info, conditions_context, sql_error_context) -> str:
    return f"""Generate a PostgreSQL SELECT query based on the SQL task and database schema.
{task_section}
Rules:
{rules_text}

Database Schema:
{schema_text}
{schema_desc_section}
{enum_info}

{conditions_context}
{sql_error_context}
Output only SQL."""


def build_sql_prompt_no_task(question, rules_text, schema_text, schema_desc_section, enum_info, conditions_context, sql_error_context) -> str:
    return f"""Generate a simple PostgreSQL SELECT query based on the user question and database schema.

Rules:
{rules_text}

Database Schema:
{schema_text}
{schema_desc_section}
{enum_info}

{conditions_context}

User Question: {question}
{sql_error_context}
Output only SQL, no other format."""


def build_error_context_validation(validation, prev_sql) -> str:
    return (
        f"\nThe previous SQL result was validated and found insufficient."
        f"\nIssue: {validation}"
        f"\nPrevious SQL:\n{prev_sql}\n"
        f"\nIMPORTANT: Fix ONLY the specific issue above. Do NOT rewrite the entire query."
        f"\n- Keep all existing WHERE conditions unchanged"
        f"\n- Keep all existing JOINs unchanged"
        f"\n- Only ADD the missing column/table, or FIX the specific problem mentioned"
        f"\n- If the issue mentions a missing column, add it to the SELECT list"
        f"\n- If the issue mentions a wrong table, replace only that table"
    )


def build_error_context_failed(prev_sql, error) -> str:
    return (
        f"\nPrevious SQL failed. Fix based on error message:\n"
        f"Failed SQL:\n{prev_sql}\nError: {error}\n"
    )


def build_error_context_empty(prev_sql) -> str:
    return (
        f"\nPrevious SQL returned 0 rows. The WHERE conditions likely don't match any data."
        f"\nPrevious SQL:\n{prev_sql}\n"
        f"\nCommon causes and fixes:"
        f"\n- Value format mismatch: try with/without leading zeros, different casing (ILIKE), or partial match (LIKE '%value%')"
        f"\n- Wrong column: the filter value may belong to a DIFFERENT column or table. Check the schema carefully."
        f"\n- Too many filters: remove the most uncertain filter and let Python handle it"
        f"\n- Date range: the database may not have data for that time period. Try removing the date filter."
        f"\nStrategy: fetch MORE data with fewer WHERE conditions. Python filters later."
    )
