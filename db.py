"""
資料庫連線、Schema、Enum 載入
"""

from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine, text as sa_text

from config import DB_URI


def build_db_context(db_uri: str = None):
    """建立 DB context: (engine, schema_info, enum_values)。可多次呼叫不同 URI。"""
    uri = db_uri or DB_URI
    eng = create_engine(uri)
    db = SQLDatabase.from_uri(uri)
    table_names = db.get_usable_table_names()
    schema_info = db.get_table_info(table_names)
    enum_values = load_enum_values(eng)
    return eng, schema_info, enum_values


def load_enum_values(eng, max_distinct=50):
    """載入低基數文字欄位的所有可能值"""
    enums = {}
    skip_suffixes = ("_id", "_no")
    sql = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND data_type IN ('character varying', 'text')
    ORDER BY table_name, ordinal_position
    """
    with eng.connect() as conn:
        cols = conn.execute(sa_text(sql)).fetchall()
        for table_name, column_name in cols:
            if any(column_name.endswith(s) for s in skip_suffixes):
                continue
            cnt_sql = f'SELECT COUNT(DISTINCT "{column_name}") FROM "{table_name}"'
            cnt = conn.execute(sa_text(cnt_sql)).scalar()
            if cnt is not None and 1 < cnt <= max_distinct:
                val_sql = (
                    f'SELECT DISTINCT "{column_name}" FROM "{table_name}" '
                    f'WHERE "{column_name}" IS NOT NULL ORDER BY 1'
                )
                vals = [r[0] for r in conn.execute(sa_text(val_sql)).fetchall()]
                enums[f"{table_name}.{column_name}"] = vals
    return enums


# 預設 global context（給 app.py / Streamlit 等單 DB 場景用）
engine, SCHEMA_INFO, ENUM_VALUES = build_db_context()


def generate_schema_summary(
    schema_info: str, column_descs: dict, enum_values: dict
) -> str:
    """一次性用 LLM 精煉 schema + column descriptions，產出修正後的欄位描述。

    兩步驟（分析-調整）：
    1. 分析：結合 column_descs + 資料樣本，深度分析每個欄位的真實含義
    2. 調整：從分析結果中萃取出需要修正的欄位描述
    """
    from langchain_core.messages import HumanMessage
    from llm import llm
    from sqlalchemy import text as sa_text

    # 組合 column descriptions
    desc_lines = []
    tables_seen = set()
    for key, desc in sorted(column_descs.items()):
        table, col = key.split(".", 1)
        if table not in tables_seen:
            if tables_seen:
                desc_lines.append("")
            desc_lines.append(f"Table: {table}")
            tables_seen.add(table)
        desc_lines.append(f"  {col}: {desc}")
    desc_text = "\n".join(desc_lines)

    enum_lines = []
    for col, vals in sorted(enum_values.items()):
        enum_lines.append(f"  {col}: {vals}")
    enum_text = "\n".join(enum_lines) if enum_lines else "(none)"

    # 取得每個表的資料樣本（分析用）
    print("  Fetching data samples...", flush=True)
    sample_text = _get_data_samples(engine, schema_info)
    print(f"  Samples ready ({len(sample_text)} chars)", flush=True)

    # Step 1: 分析 — 深度分析每個欄位
    import time

    print("  Step 1/2: Deep analysis (analyze)...", flush=True)
    t0 = time.time()
    analyze_prompt = f"""You are a database expert. Analyze each column in the database by cross-referencing the column descriptions with the actual data samples below.

For each column, determine:
1. What the column actually stores (verify against sample data, not just the description)
2. If the description says "X" but the data suggests otherwise, note the discrepancy
3. For numeric columns: is it a count, an amount, a ratio, an ID, or something else? Look at the value ranges in the samples.
4. For text columns with non-English values: what language and what do the values mean?

Be thorough. Output your analysis for every column, one per line:
Format: table.column: [actual meaning based on data] (note: [any discrepancy with description])

Database Schema (DDL):
{schema_info}

Column Descriptions:
{desc_text}

Data Samples (5 rows per table):
{sample_text}

Known Enum Values:
{enum_text}
"""

    res1 = llm.invoke([HumanMessage(content=analyze_prompt)])
    deep_analysis = res1.content.strip()
    print(f"  Step 1 done ({len(deep_analysis)} chars, {time.time()-t0:.0f}s)")

    # Step 2: 調整 — 只產出需要修正的欄位描述
    print("  Step 2/2: Extract corrections (refine)...", flush=True)
    t1 = time.time()
    refine_prompt = f"""You are a database expert. Based on the deep column analysis below, identify ONLY the columns where the original description is wrong, misleading, or missing critical information.

For each such column, provide a corrected one-line description.

Rules:
- ONLY include columns where the deep analysis found a discrepancy or important missing info
- Do NOT include columns where the original description is basically correct
- Do NOT include columns that are self-explanatory from the name alone
- Do NOT include date format corrections or data type observations
- For non-English values, include translations only if they are not obvious

Output as JSON object where keys are "table.column" and values are the corrected description.
If no corrections are needed, output an empty object {{}}.

Original Column Descriptions:
{desc_text}

Deep Column Analysis:
{deep_analysis}
"""

    res2 = llm.invoke([HumanMessage(content=refine_prompt)])
    refined_text = res2.content.strip()
    print(f"  Step 2 done ({len(refined_text)} chars, {time.time()-t1:.0f}s)")

    # Parse JSON, fallback to original format
    import json as _json

    try:
        # Clean markdown fences
        text = refined_text
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        refined_descs = _json.loads(text.strip())
        return refined_descs  # Return as dict
    except (_json.JSONDecodeError, KeyError):
        print("  ⚠️ Failed to parse refined descs as JSON, returning raw text")
        return refined_text  # Fallback to text


def _get_data_samples(engine, schema_info):
    """從每個表取 5 筆資料樣本"""
    import re
    from sqlalchemy import text as sa_text

    tables = re.findall(r'CREATE\s+TABLE\s+"?(\w+)"?', schema_info, re.IGNORECASE)
    samples = []
    for table in tables:
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    sa_text(f'SELECT * FROM "{table}" LIMIT 5')
                ).fetchall()
                if rows:
                    cols = (
                        rows[0]._fields
                        if hasattr(rows[0], "_fields")
                        else list(range(len(rows[0])))
                    )
                    samples.append(f"Table {table} (sample):")
                    for row in rows:
                        vals = {c: str(v)[:50] for c, v in zip(cols, row)}
                        samples.append(f"  {vals}")
                    samples.append("")
        except Exception:
            pass
    return "\n".join(samples)
