"""資料載入與查詢：啟動時載入 dev.json，提供索引化的題目與資料庫查詢。"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


class DataStore:
    """In-memory store backed by dev.json with db_id and (db_id, question_id) indexes."""

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)
        dev_path = self._data_dir / "dev.json"
        with open(dev_path, encoding="utf-8") as f:
            data: list[dict] = json.load(f)

        # _by_db: db_id -> list of question dicts
        self._by_db: dict[str, list[dict]] = defaultdict(list)
        # _by_key: (db_id, question_id) -> question dict
        self._by_key: dict[tuple[str, int], dict] = {}

        for item in data:
            db_id = item["db_id"]
            qid = item["question_id"]
            self._by_db[db_id].append(item)
            self._by_key[(db_id, qid)] = item

    # ── 查詢方法 ─────────────────────────────────────────

    def get_db_ids(self) -> list[str]:
        """回傳排序後的所有 DB_ID 列表。"""
        return sorted(self._by_db.keys())

    def db_exists(self, db_id: str) -> bool:
        """檢查 DB_ID 是否存在於索引中。"""
        return db_id in self._by_db

    def get_db_description(self, db_id: str) -> dict:
        """讀取資料庫描述：original_desc 從 CSV 生成完整欄位描述，compact_desc 從精簡檔讀取。"""
        base = self._data_dir / "databases" / db_id
        original_desc = self._load_full_description(base / "database_description")
        compact_desc = self._read_file_or_empty(base / "description_compact.txt")
        return {"original_desc": original_desc, "compact_desc": compact_desc}

    def get_db_question_count(self, db_id: str) -> dict:
        """回傳題目總數與按難度分類的統計。"""
        questions = self._by_db.get(db_id, [])
        by_difficulty: dict[str, int] = {"simple": 0, "moderate": 0, "challenging": 0}
        for q in questions:
            diff = q.get("difficulty", "")
            if diff in by_difficulty:
                by_difficulty[diff] += 1
        return {"total": len(questions), "by_difficulty": by_difficulty}

    def get_question(self, db_id: str, question_id: int) -> dict | None:
        """回傳單題資料，找不到時回傳 None。"""
        return self._by_key.get((db_id, question_id))

    def get_db_hints(self, db_id: str) -> list[dict]:
        """回傳該 DB 所有題目的 question_id、question、evidence。"""
        return [
            {
                "question_id": q["question_id"],
                "question": q["question"],
                "evidence": q.get("evidence", ""),
            }
            for q in self._by_db.get(db_id, [])
        ]

    def get_sqlite_path(self, db_id: str) -> str:
        """回傳 SQLite 檔案路徑字串。"""
        return str(self._data_dir / "databases" / db_id / f"{db_id}.sqlite")

    # ── 內部工具 ─────────────────────────────────────────

    @staticmethod
    def _load_full_description(desc_dir: Path) -> str:
        """從 database_description/ CSV 檔案生成完整欄位描述（欄位名轉小寫）。"""
        if not desc_dir.exists():
            return ""
        lines: list[str] = []
        for csv_file in sorted(desc_dir.glob("*.csv")):
            table_name = csv_file.stem.lower()
            lines.append(f"Table: {table_name}")
            with open(csv_file, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    col = row.get("original_column_name", "").lower()
                    desc = row.get("column_description", "")
                    val_desc = row.get("value_description", "")
                    if desc or val_desc:
                        parts = [f"  {col}"]
                        if desc:
                            parts.append(f": {desc}")
                        if val_desc and len(val_desc) < 300:
                            parts.append(f" ({val_desc})")
                        lines.append("".join(parts))
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _read_file_or_empty(path: Path) -> str:
        """讀取檔案內容，檔案不存在時回傳空字串。"""
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ""
