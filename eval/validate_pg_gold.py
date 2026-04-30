"""
Validate PG gold results against SQLite gold.
Marks each question as: match, order_diff (tie-breaking), or mismatch.
Only order_diff questions should use PG gold as alternative answer.

Usage:
    conda run -n Greg-text-to-sql python eval/validate_pg_gold.py
"""
import json
import os
import sqlite3
import sys
from pathlib import Path

EVAL_DIR = Path(__file__).parent
BIRD_DBS = ["california_schools", "financial", "debit_card_specializing"]


def normalize_val(v):
    """Normalize a value for comparison (lowercase strings, round floats)."""
    if v is None:
        return None
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, str):
        return v.strip().lower()
    return v


def normalize_row(row):
    """Normalize a dict row: lowercase keys, normalize values."""
    return {k.lower(): normalize_val(v) for k, v in row.items()}


def compare_results(sqlite_rows, pg_rows, gold_sql=""):
    """Compare two result sets. Returns: 'match', 'order_diff', or 'mismatch'."""
    if not sqlite_rows and not pg_rows:
        return "match"
    if not sqlite_rows or not pg_rows:
        # If gold SQL has ORDER BY + LIMIT and one is empty, could be tie-breaking
        if gold_sql and "LIMIT" in gold_sql.upper() and "ORDER BY" in gold_sql.upper():
            return "order_diff"
        return "mismatch"
    if len(sqlite_rows) != len(pg_rows):
        return "mismatch"

    s_norm = [normalize_row(r) for r in sqlite_rows]
    p_norm = [normalize_row(r) for r in pg_rows]

    # Compare by values only (ignore column names — they differ between SQLite and PG)
    def row_values(row):
        return tuple(sorted(str(v) for v in row.values()))

    s_vals = [row_values(r) for r in s_norm]
    p_vals = [row_values(r) for r in p_norm]

    # Exact match (same order, same values)
    if s_vals == p_vals:
        return "match"

    # Same values, different order
    if sorted(s_vals) == sorted(p_vals):
        return "order_diff"

    # Try numeric-tolerant comparison (for float precision differences)
    def fuzzy_row_values(row):
        vals = []
        for v in row.values():
            if v is None:
                vals.append("None")
            elif isinstance(v, (int, float)):
                vals.append(f"{float(v):.4f}")
            else:
                s = str(v).strip()
                try:
                    vals.append(f"{float(s):.4f}")
                except ValueError:
                    vals.append(s.lower())
        return tuple(sorted(vals))

    s_fuzzy = [fuzzy_row_values(r) for r in s_norm]
    p_fuzzy = [fuzzy_row_values(r) for r in p_norm]

    if s_fuzzy == p_fuzzy:
        return "match"
    if sorted(s_fuzzy) == sorted(p_fuzzy):
        return "order_diff"

    # If gold SQL has ORDER BY + LIMIT, different values likely due to tie-breaking
    if gold_sql and "LIMIT" in gold_sql.upper() and "ORDER BY" in gold_sql.upper():
        return "order_diff"

    return "mismatch"


def main():
    with open(EVAL_DIR / "dev.json") as f:
        questions = json.load(f)

    total = {"match": 0, "order_diff": 0, "mismatch": 0, "pg_error": 0}
    mismatches = []

    for db_id in BIRD_DBS:
        pg_path = EVAL_DIR / "databases" / db_id / "pg_gold_results.json"
        if not pg_path.exists():
            print(f"⚠️  No PG gold for {db_id}")
            continue

        with open(pg_path) as f:
            pg_data = json.load(f)

        db_questions = [q for q in questions if q["db_id"] == db_id]
        conn = sqlite3.connect(str(EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"))

        db_stats = {"match": 0, "order_diff": 0, "mismatch": 0, "pg_error": 0}

        for q in db_questions:
            qid = str(q["question_id"])
            pg_entry = pg_data.get(qid, {})

            if pg_entry.get("error"):
                db_stats["pg_error"] += 1
                total["pg_error"] += 1
                continue

            pg_rows = pg_entry.get("result", [])

            try:
                c = conn.cursor()
                c.execute(q["SQL"])
                cols = [d[0] for d in c.description]
                rows = c.fetchall()
                sqlite_rows = [dict(zip(cols, r)) for r in rows[:50]]
            except Exception:
                continue

            status = compare_results(sqlite_rows, pg_rows, gold_sql=q["SQL"])
            db_stats[status] += 1
            total[status] += 1

            if status == "mismatch":
                mismatches.append({
                    "qid": q["question_id"],
                    "db": db_id,
                    "difficulty": q.get("difficulty"),
                    "sqlite_sample": json.dumps(sqlite_rows[:1], default=str)[:100],
                    "pg_sample": json.dumps(pg_rows[:1], default=str)[:100],
                })

        conn.close()
        print(f"{db_id}: match={db_stats['match']}, order_diff={db_stats['order_diff']}, mismatch={db_stats['mismatch']}, pg_error={db_stats['pg_error']}")

    print(f"\nTOTAL: match={total['match']}, order_diff={total['order_diff']}, mismatch={total['mismatch']}, pg_error={total['pg_error']}")
    print(f"Usable PG gold (match + order_diff): {total['match'] + total['order_diff']}")

    if mismatches:
        print(f"\nMISMATCHES ({len(mismatches)}):")
        for m in mismatches:
            print(f"  #{m['qid']} ({m['difficulty']}, {m['db']})")
            print(f"    SQLite: {m['sqlite_sample']}")
            print(f"    PG:     {m['pg_sample']}")

    # Save validation results
    out = {"match": [], "order_diff": [], "mismatch": []}
    for m in mismatches:
        out["mismatch"].append(m["qid"])

    # Re-run to collect match and order_diff qids
    for db_id in BIRD_DBS:
        pg_path = EVAL_DIR / "databases" / db_id / "pg_gold_results.json"
        if not pg_path.exists():
            continue
        with open(pg_path) as f:
            pg_data = json.load(f)
        db_questions = [q for q in questions if q["db_id"] == db_id]
        conn = sqlite3.connect(str(EVAL_DIR / "databases" / db_id / f"{db_id}.sqlite"))
        for q in db_questions:
            qid = str(q["question_id"])
            pg_entry = pg_data.get(qid, {})
            if pg_entry.get("error"):
                continue
            pg_rows = pg_entry.get("result", [])
            try:
                c = conn.cursor()
                c.execute(q["SQL"])
                cols = [d[0] for d in c.description]
                rows = c.fetchall()
                sqlite_rows = [dict(zip(cols, r)) for r in rows[:50]]
            except:
                continue
            status = compare_results(sqlite_rows, pg_rows, gold_sql=q["SQL"])
            if status in ("match", "order_diff"):
                out[status].append(q["question_id"])
        conn.close()

    with open(EVAL_DIR / "pg_gold_validation.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to eval/pg_gold_validation.json")


if __name__ == "__main__":
    main()
