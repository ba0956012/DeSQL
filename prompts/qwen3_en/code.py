"""Code node prompt templates — qwen3_en (English, Qwen3 Coder)"""


def build_code_prompt(question, sql, sql_result_len, sample, result_guidance_section, error_context, chart_instruction, data_profile="", data_notes="") -> str:
    import json
    columns = list(sample[0].keys()) if sample and isinstance(sample[0], dict) else []
    sample_str = json.dumps(sample, indent=2, ensure_ascii=False, default=str)

    profile_section = f"\n{data_profile}\n" if data_profile else ""
    notes_section = f"\nData verification notes:\n{data_notes}\n" if data_notes else ""

    return f"""
Question: {question}

SQL query completed. The data variable contains the full result:
SQL: {sql}

Row count: {sql_result_len}
Columns: {columns}
Sample data:
{sample_str}
{profile_section}
{result_guidance_section}
{notes_section}
{error_context}
Write Python code to process the data variable:
1. Process data as needed (calculate, filter, sort, format)
2. Store the final answer in the result variable
   - If SQL already computed the result (GROUP BY / ORDER BY / LIMIT / aggregates), just extract the answer directly
   - If SQL returned raw data, process according to the Python task description above
   - result should be the complete answer (include all needed values and names)
3. Filter out None values before any math operations
{chart_instruction}

CRITICAL Rules:
- data is a list of dict, keys are SQL column names (lowercase)
- ONLY use columns that exist in the data (check the Columns list above). Do NOT reference columns that SQL did not SELECT.
- The SQL WHERE clause has ALREADY filtered the data. Do NOT re-apply the same filters in Python unless you need additional filtering beyond what SQL did.
- If the data profile shows duplicate rows, consider whether you need to deduplicate before counting or aggregating.
- For "how many" questions, check if you need COUNT(DISTINCT ...) by looking at the unique counts in the data profile.
- For ratio/percentage questions, compute both numerator and denominator from data
- Do NOT use import statements
- Counter, defaultdict, Decimal, datetime, timedelta, date are already available
- Date columns are datetime.date objects, compare directly with < > ==
- Do NOT call .date() on date objects or parse them as strings
- If the question or Hint specifies a formula, follow it exactly
"""


def build_chart_instruction(enable_chart: bool) -> str:
    if not enable_chart:
        return ""
    return """4. Store chart-friendly structured data in chart_data variable (list of dict)
   - If result is not suitable for charting, set chart_data = []"""
