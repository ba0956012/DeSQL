"""
將 BIRD-SQL 的 SQLite 資料庫匯入本地 PostgreSQL

每個 SQLite 資料庫會建立一個獨立的 PG database（前綴 bird_）。

用法：
    # 匯入全部
    python eval/import_to_pg.py

    # 只匯入指定的資料庫
    python eval/import_to_pg.py california_schools financial
"""

import sys
import os
import sqlite3
from pathlib import Path

from sqlalchemy import create_engine, text

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env.eval", override=True)

# PostgreSQL 連線（從 .env.eval 讀取，或使用預設值）
PG_BASE_URL = os.environ.get("PG_BASE_URL")
if not PG_BASE_URL:
    print("❌ 請在 .env.eval 設定 PG_BASE_URL")
    sys.exit(1)
PG_ADMIN_URL = f"{PG_BASE_URL}/postgres"
DB_PREFIX = "bird_"

EVAL_DIR = Path(__file__).parent
DATABASES_DIR = EVAL_DIR / "databases"

# SQLite 型別 → PostgreSQL 型別
TYPE_MAP = {
    "INTEGER": "BIGINT",
    "INT": "BIGINT",
    "REAL": "DOUBLE PRECISION",
    "FLOAT": "DOUBLE PRECISION",
    "NUMERIC": "NUMERIC",
    "TEXT": "TEXT",
    "BLOB": "BYTEA",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "BOOLEAN": "TEXT",
    "VARCHAR": "TEXT",
}


def map_type(sqlite_type: str) -> str:
    """將 SQLite 欄位型別對應到 PostgreSQL"""
    upper = (sqlite_type or "TEXT").upper().strip()
    # 處理 VARCHAR(N) 等帶括號的型別
    base = upper.split("(")[0].strip()
    return TYPE_MAP.get(base, "TEXT")


def create_pg_database(db_name: str):
    """建立 PG database（如果不存在）"""
    engine = create_engine(PG_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": db_name},
        ).fetchone()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            print(f"  ✅ 建立 database: {db_name}")
        else:
            print(f"  ⏭️  database 已存在: {db_name}")
    engine.dispose()


def import_sqlite_to_pg(sqlite_path: str, pg_db_name: str):
    """將一個 SQLite 資料庫的所有表匯入 PG"""
    # 連接 SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    cursor = sqlite_conn.cursor()

    # 取得所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall() if row[0] != "sqlite_sequence"]

    # 先從 SQLite 收集所有 FK 資訊（建表後統一加）
    all_fks = []
    for table_name in tables:
        pg_table = table_name.lower()
        try:
            cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
            for fk in cursor.fetchall():
                # fk: (id, seq, table, from, to, on_update, on_delete, match)
                ref_table = fk[2].lower()
                from_col = fk[3].lower()
                to_col = fk[4].lower()
                all_fks.append((pg_table, from_col, ref_table, to_col))
        except Exception:
            pass

    # 連接 PG
    pg_engine = create_engine(f"{PG_BASE_URL}/{pg_db_name}")

    for table_name in tables:
        # 取得欄位資訊
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = cursor.fetchall()

        # 表名和欄位名全部轉小寫（PG case-sensitive，統一小寫避免引號問題）
        pg_table_name = table_name.lower()

        # 建表
        col_defs = []
        col_names = []       # SQLite 原始欄位名（用於讀取資料）
        pg_col_names = []    # PG 小寫欄位名
        for col in columns:
            col_name = col[1]
            col_type = map_type(col[2])
            col_names.append(col_name)
            pg_col_names.append(col_name.lower())
            col_defs.append(f'"{col_name.lower()}" {col_type}')

        create_sql = f'DROP TABLE IF EXISTS "{pg_table_name}" CASCADE;\n'
        create_sql += f'CREATE TABLE "{pg_table_name}" ({", ".join(col_defs)});'

        with pg_engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()

        # 匯入資料
        cursor.execute(f'SELECT * FROM "{table_name}"')
        rows = cursor.fetchall()

        # 找出 DATE/TIMESTAMP 欄位的索引，用於格式轉換
        from datetime import datetime as _dt
        date_col_indices = set()
        for i, col in enumerate(columns):
            col_type_upper = (col[2] or "").upper().strip().split("(")[0].strip()
            if col_type_upper in ("DATE", "DATETIME"):
                date_col_indices.add(i)

        def parse_date(val):
            """嘗試多種格式解析日期字串"""
            if val is None:
                return None
            s = str(val).strip()
            if not s:
                return None
            # YYMMDD (6 digits)
            if s.isdigit() and len(s) == 6:
                try:
                    return _dt.strptime(s, "%y%m%d").date().isoformat()
                except ValueError:
                    pass
            # YYYYMMDD (8 digits)
            if s.isdigit() and len(s) == 8:
                try:
                    return _dt.strptime(s, "%Y%m%d").date().isoformat()
                except ValueError:
                    pass
            # YYYY-MM-DD
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
                try:
                    return _dt.strptime(s, fmt).date().isoformat()
                except ValueError:
                    continue
            return s  # 無法解析就原樣存

        if rows:
            escaped_cols = [f'"{c}"'.replace("%", "%%") for c in pg_col_names]
            placeholders = ", ".join(["%s" for _ in pg_col_names])
            insert_sql = f'INSERT INTO "{pg_table_name}" ({", ".join(escaped_cols)}) VALUES ({placeholders})'

            with pg_engine.connect() as conn:
                raw_conn = conn.connection
                pg_cursor = raw_conn.cursor()
                batch = []
                for row in rows:
                    converted = []
                    for i in range(len(col_names)):
                        if i in date_col_indices:
                            converted.append(parse_date(row[i]))
                        else:
                            converted.append(row[i])
                    batch.append(tuple(converted))
                    if len(batch) >= 1000:
                        pg_cursor.executemany(insert_sql, batch)
                        batch = []
                if batch:
                    pg_cursor.executemany(insert_sql, batch)
                raw_conn.commit()

        print(f"    📦 {pg_table_name}: {len(columns)} 欄位, {len(rows)} 筆")

    # 建立 PK 和 FK constraints
    # 先從 SQLite 取得 PK 資訊，建立 PK
    for table_name in tables:
        pg_table = table_name.lower()
        try:
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            pk_cols = [row[1].lower() for row in cursor.fetchall() if row[5] > 0]  # pk flag > 0
            if pk_cols:
                pk_col_str = ", ".join(f'"{c}"' for c in pk_cols)
                with pg_engine.connect() as conn:
                    try:
                        conn.execute(text(
                            f'ALTER TABLE "{pg_table}" ADD PRIMARY KEY ({pk_col_str})'
                        ))
                        conn.commit()
                        print(f"    🔑 PK: {pg_table}({', '.join(pk_cols)})")
                    except Exception as e:
                        conn.rollback()
                        # PK 可能因為重複資料失敗，改用 UNIQUE INDEX
                        try:
                            conn.execute(text(
                                f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{pg_table}_pk" ON "{pg_table}" ({pk_col_str})'
                            ))
                            conn.commit()
                            print(f"    🔑 UNIQUE: {pg_table}({', '.join(pk_cols)})")
                        except Exception:
                            conn.rollback()
        except Exception:
            pass

    # 建立 FK constraints（每個獨立 transaction）
    if all_fks:
        for from_table, from_col, ref_table, to_col in all_fks:
            fk_name = f"fk_{from_table}_{from_col}_{ref_table}"
            with pg_engine.connect() as conn:
                try:
                    conn.execute(text(
                        f'ALTER TABLE "{from_table}" ADD CONSTRAINT "{fk_name}" '
                        f'FOREIGN KEY ("{from_col}") REFERENCES "{ref_table}" ("{to_col}")'
                    ))
                    conn.commit()
                    print(f"    🔗 FK: {from_table}.{from_col} → {ref_table}.{to_col}")
                except Exception as e:
                    conn.rollback()
                    print(f"    ⚠️  FK 跳過: {from_table}.{from_col} → {ref_table}.{to_col}")

    pg_engine.dispose()
    sqlite_conn.close()


def main():
    # 決定要匯入哪些資料庫
    if len(sys.argv) > 1:
        targets = sys.argv[1:]
    else:
        targets = sorted([
            d.name for d in DATABASES_DIR.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])

    print(f"準備匯入 {len(targets)} 個資料庫\n")

    for db_id in targets:
        sqlite_path = DATABASES_DIR / db_id / f"{db_id}.sqlite"
        if not sqlite_path.exists():
            print(f"⚠️  找不到 {sqlite_path}，跳過")
            continue

        pg_db_name = f"{DB_PREFIX}{db_id}"
        print(f"📂 {db_id} → {pg_db_name}")
        create_pg_database(pg_db_name)
        import_sqlite_to_pg(str(sqlite_path), pg_db_name)
        print()

    print("✅ 全部完成")


if __name__ == "__main__":
    main()
