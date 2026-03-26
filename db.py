"""
資料庫連線、Schema、Enum 載入
"""

from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine, text as sa_text

from config import DB_URI

engine = create_engine(DB_URI)
db = SQLDatabase.from_uri(DB_URI)
TABLE_NAMES = db.get_usable_table_names()
SCHEMA_INFO = db.get_table_info(TABLE_NAMES)


def load_enum_values(eng=None, max_distinct=50):
    """載入低基數文字欄位的所有可能值"""
    eng = eng or engine
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


ENUM_VALUES = load_enum_values(engine)
