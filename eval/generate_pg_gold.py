"""
Generate PostgreSQL gold answers for BIRD eval questions.

Runs each gold SQL on PostgreSQL (instead of SQLite) and saves the results.
Converts SQLite-specific syntax to PostgreSQL equivalents.

Usage:
    python eval/generate_pg_gold.py                          # all 3 DBs
    python eval/generate_pg_gold.py --db california_schools   # single DB
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

EVAL_DIR = Path(__file__).parent
sys.path.insert(0, str(EVAL_DIR.parent))

from dotenv import load_dotenv

load_dotenv(EVAL_DIR / ".env.eval", override=True)

PG_BASE_URL = os.environ.get(
    "PG_BASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432"
)
DB_PREFIX = "bird_"

BIRD_DBS = ["california_schools", "financial", "debit_card_specializing"]


def sqlite_to_pg(sql: str) -> str:
    """Convert SQLite-specific SQL syntax to PostgreSQL."""
    s = sql

    # 1. Replace backtick quoting with double quotes (lowercase for PG)
    def backtick_to_pg(m):
        return '"' + m.group(1).lower() + '"'

    s = re.sub(r"`([^`]+)`", backtick_to_pg, s)

    # 2. LIMIT offset,count → LIMIT count OFFSET offset
    def fix_limit(m):
        offset = m.group(1)
        count = m.group(2)
        return f"LIMIT {count} OFFSET {offset}"

    s = re.sub(r"LIMIT\s+(\d+)\s*,\s*(\d+)", fix_limit, s, flags=re.IGNORECASE)

    # 3. IIF(cond, true_val, false_val) → CASE WHEN cond THEN true_val ELSE false_val END
    # Handle nested IIF by doing multiple passes
    for _ in range(10):
        # Match IIF with balanced parentheses (simple version)
        pattern = r"\bIIF\s*\(\s*"
        match = re.search(pattern, s, re.IGNORECASE)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1  # position of '('
        # Find matching closing paren
        depth = 1
        pos = paren_start + 1
        args = []
        arg_start = pos
        while pos < len(s) and depth > 0:
            if s[pos] == "(":
                depth += 1
            elif s[pos] == ")":
                depth -= 1
                if depth == 0:
                    args.append(s[arg_start:pos].strip())
                    break
            elif s[pos] == "," and depth == 1:
                args.append(s[arg_start:pos].strip())
                arg_start = pos + 1
            pos += 1
        if len(args) == 3:
            replacement = f"CASE WHEN {args[0]} THEN {args[1]} ELSE {args[2]} END"
            s = s[:start] + replacement + s[pos + 1 :]
        else:
            break  # can't parse, stop

    # 4. strftime('%Y', date_col) → EXTRACT(YEAR FROM date_col)::TEXT
    #    strftime('%m', date_col) → LPAD(EXTRACT(MONTH FROM date_col)::TEXT, 2, '0')
    #    strftime('%Y-%m', date_col) → TO_CHAR(date_col, 'YYYY-MM')
    def fix_strftime(m):
        fmt = m.group(1)
        col = m.group(2)
        if fmt == "%Y":
            return f"EXTRACT(YEAR FROM {col})::TEXT"
        elif fmt == "%m":
            return f"LPAD(EXTRACT(MONTH FROM {col})::TEXT, 2, '0')"
        elif fmt == "%d":
            return f"LPAD(EXTRACT(DAY FROM {col})::TEXT, 2, '0')"
        elif fmt == "%Y-%m":
            return f"TO_CHAR({col}, 'YYYY-MM')"
        elif fmt == "%Y%m":
            return f"TO_CHAR({col}, 'YYYYMM')"
        elif fmt == "%Y-%m-%d":
            return f"TO_CHAR({col}, 'YYYY-MM-DD')"
        else:
            return f"TO_CHAR({col}, '{fmt}')"

    s = re.sub(
        r"strftime\s*\(\s*'([^']+)'\s*,\s*([^)]+)\)",
        fix_strftime,
        s,
        flags=re.IGNORECASE,
    )

    # 5. SUM(condition) → SUM(CASE WHEN condition THEN 1 ELSE 0 END)
    # Only for bare boolean expressions, NOT for already-converted CASE WHEN
    def fix_sum_bool(m):
        inner = m.group(1).strip()
        # Skip if already a CASE WHEN expression
        if inner.upper().startswith("CASE"):
            return m.group(0)
        # Check if inner looks like a boolean expression (contains =, <>, !=, LIKE)
        if re.search(r"\s*(=|<>|!=|LIKE|NOT)\s*", inner, re.IGNORECASE):
            return f"SUM(CASE WHEN {inner} THEN 1 ELSE 0 END)"
        return m.group(0)

    s = re.sub(r"\bSUM\s*\(([^()]+)\)", fix_sum_bool, s, flags=re.IGNORECASE)

    # 6. Fix text = integer comparisons (PG is strict about types)
    # Cast integer literals to text when compared with known text columns
    for col in [
        "DOC",
        "SOC",
        "Charter",
        "Magnet",
        "Virtual",
        "EdOpsCode",
        "NCESDist",
        "NCESSchool",
        '"low grade"',
        '"high grade"',
        '"county code"',
        '"school code"',
        '"district code"',
        '"academic year"',
    ]:
        # col = integer → col = 'integer'
        for op in ["=", ">=", "<=", ">", "<"]:
            s = re.sub(
                rf"{re.escape(col)}\s*{re.escape(op)}\s*(\d+)\b",
                lambda m, c=col, o=op: f"{c} {o} '{m.group(1)}'",
                s,
                flags=re.IGNORECASE,
            )
        # BETWEEN integer AND integer → BETWEEN 'integer' AND 'integer'
        s = re.sub(
            rf"{re.escape(col)}\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)",
            lambda m, c=col: f"{c} BETWEEN '{m.group(1)}' AND '{m.group(2)}'",
            s,
            flags=re.IGNORECASE,
        )

    # 7. Date LIKE pattern: WHERE Date LIKE '2012-01%' → WHERE Date::TEXT LIKE '2012-01%'
    s = re.sub(r"\b(Date)\s+LIKE\s+", r"\1::TEXT LIKE ", s, flags=re.IGNORECASE)

    # 7b. Date BETWEEN integer → Date::TEXT BETWEEN 'integer'
    s = re.sub(
        r"\b(T\d+\.)?Date\s+BETWEEN\s+(\d{6})\s+AND\s+(\d{6})",
        lambda m: f"{m.group(1) or ''}Date::TEXT BETWEEN '{m.group(2)}' AND '{m.group(3)}'",
        s,
        flags=re.IGNORECASE,
    )

    # 8. TEXT - TEXT arithmetic: EXTRACT(...)::TEXT - EXTRACT(...)::TEXT → cast to int
    s = re.sub(
        r"EXTRACT\((\w+)\s+FROM\s+([^)]+)\)::TEXT",
        r"EXTRACT(\1 FROM \2)::INTEGER",
        s,
        flags=re.IGNORECASE,
    )

    # 9. (merged into #6)

    # 10. SUM(compound boolean): handle SUM with AND/OR containing comparison operators
    # Need to handle nested parens (e.g., SUM(type = 'gold' AND EXTRACT(...) < '1998'))
    def fix_sum_deep(s):
        pattern = r"\bSUM\s*\("
        for match in list(re.finditer(pattern, s, re.IGNORECASE)):
            start = match.start()
            paren_start = match.end() - 1
            depth = 1
            pos = paren_start + 1
            while pos < len(s) and depth > 0:
                if s[pos] == "(":
                    depth += 1
                elif s[pos] == ")":
                    depth -= 1
                pos += 1
            inner = s[paren_start + 1 : pos - 1].strip()
            if inner.upper().startswith("CASE"):
                continue
            if re.search(r"\b(AND|OR)\b", inner, re.IGNORECASE) and re.search(
                r"(=|<>|!=|<|>|LIKE)", inner, re.IGNORECASE
            ):
                replacement = f"SUM(CASE WHEN {inner} THEN 1 ELSE 0 END)"
                s = s[:start] + replacement + s[pos:]
                return fix_sum_deep(s)  # recurse for multiple occurrences
        return s

    s = fix_sum_deep(s)

    # 11. Division by zero protection: X / Y → X / NULLIF(Y, 0)
    # SQLite returns NULL for division by zero, PG throws error
    # Only apply to column/column division patterns (not literal denominators)
    s = re.sub(
        r"(\w+\.(?:Price|Amount|Consumption))\s*/\s*(\w+\.(?:Price|Amount|Consumption))",
        r"\1 / NULLIF(\2, 0)",
        s,
        flags=re.IGNORECASE,
    )

    return s


def run_gold_on_pg(db_id: str, gold_sql: str) -> list:
    """Execute gold SQL on PostgreSQL, return list of dicts."""
    from sqlalchemy import create_engine, text as sa_text

    pg_sql = sqlite_to_pg(gold_sql)
    url = f"{PG_BASE_URL}/{DB_PREFIX}{db_id}"
    engine = create_engine(url)
    with engine.connect() as conn:
        result = conn.execute(sa_text(pg_sql))
        cols = list(result.keys())
        rows = result.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db", default=None, help="Single DB to process (default: all)"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Show SQL conversion details"
    )
    args = parser.parse_args()

    dbs = [args.db] if args.db else BIRD_DBS

    with open(EVAL_DIR / "dev.json") as f:
        all_questions = json.load(f)

    total_ok = 0
    total_err = 0

    for db_id in dbs:
        questions = [q for q in all_questions if q["db_id"] == db_id]
        if not questions:
            print(f"⚠️  No questions for {db_id}")
            continue

        print(f"\n{'='*60}")
        print(f"Processing {db_id}: {len(questions)} questions")
        print(f"{'='*60}")

        pg_gold = {}
        errors = []

        for q in questions:
            qid = q["question_id"]
            gold_sql = q.get("SQL", "")
            if not gold_sql:
                errors.append((qid, "no gold SQL"))
                continue

            pg_sql = sqlite_to_pg(gold_sql)
            try:
                result = run_gold_on_pg(db_id, gold_sql)
                pg_gold[str(qid)] = {
                    "question_id": qid,
                    "gold_sql": gold_sql,
                    "pg_sql": pg_sql,
                    "result": result[:50],
                    "row_count": len(result),
                }
                print(f"  ✅ #{qid}: {len(result)} rows")
                total_ok += 1
            except Exception as e:
                err_msg = str(e)[:200]
                pg_gold[str(qid)] = {
                    "question_id": qid,
                    "gold_sql": gold_sql,
                    "pg_sql": pg_sql,
                    "result": None,
                    "error": err_msg,
                }
                errors.append((qid, err_msg))
                if args.verbose:
                    print(f"  ❌ #{qid}: {err_msg[:100]}")
                    print(f"     Original: {gold_sql[:100]}")
                    print(f"     Converted: {pg_sql[:100]}")
                else:
                    print(f"  ❌ #{qid}: {err_msg[:80]}")
                total_err += 1

        # Save
        out_path = EVAL_DIR / "databases" / db_id / "pg_gold_results.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(pg_gold, f, ensure_ascii=False, indent=2, default=str)

        ok = len(questions) - len(errors)
        print(f"\n  Saved: {out_path}")
        print(f"  Success: {ok}/{len(questions)}, Errors: {len(errors)}")

    print(f"\n{'='*60}")
    print(f"TOTAL: {total_ok} success, {total_err} errors")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
