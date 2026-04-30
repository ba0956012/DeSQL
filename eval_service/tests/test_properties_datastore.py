"""Property-based tests for eval_service.datastore module.

Uses real eval/ data to verify correctness properties across all valid DB_IDs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis.strategies import sampled_from

from eval_service.datastore import DataStore

# ── Shared fixtures & data ──────────────────────────────────────────

DATA_DIR = str(Path(__file__).resolve().parents[2] / "eval")

# Pre-load real dev.json once at module level for ground-truth comparisons
_dev_path = Path(DATA_DIR) / "dev.json"
with open(_dev_path, encoding="utf-8") as _f:
    _DEV_DATA: list[dict] = json.load(_f)

_VALID_DB_IDS = sorted({item["db_id"] for item in _DEV_DATA})

# Build ground-truth index: db_id -> list of question dicts
_BY_DB: dict[str, list[dict]] = {}
for _item in _DEV_DATA:
    _BY_DB.setdefault(_item["db_id"], []).append(_item)


@pytest.fixture(scope="module")
def store() -> DataStore:
    """Module-scoped DataStore backed by real eval/ data."""
    return DataStore(DATA_DIR)


# We need a module-scoped store accessible inside hypothesis tests (which
# cannot use pytest fixtures directly).  Use a module-level singleton.
_STORE = DataStore(DATA_DIR)


# ── Property 2: 題目數量不變量 ──────────────────────────────────────
# Feature: eval-microservice, Property 2: 題目數量不變量
# **Validates: Requirements 2.1**


@settings(max_examples=100)
@given(db_id=sampled_from(_VALID_DB_IDS))
def test_property2_question_count_invariant(db_id: str) -> None:
    """by_difficulty 加總 == total，且 total == dev.json 中該 DB 的實際題目數。"""
    result = _STORE.get_db_question_count(db_id)

    total = result["total"]
    by_difficulty = result["by_difficulty"]

    # Sum of difficulty buckets must equal total
    assert sum(by_difficulty.values()) == total, (
        f"by_difficulty sum {sum(by_difficulty.values())} != total {total} for {db_id}"
    )

    # total must match the actual count from dev.json
    actual_count = len(_BY_DB.get(db_id, []))
    assert total == actual_count, (
        f"total {total} != actual dev.json count {actual_count} for {db_id}"
    )


# ── Property 1: 資料庫描述一致性 ──────────────────────────────────
# Feature: eval-microservice, Property 1: 資料庫描述一致性
# **Validates: Requirements 1.1**


@settings(max_examples=100)
@given(db_id=sampled_from(_VALID_DB_IDS))
def test_property1_db_description_consistency(db_id: str) -> None:
    """API 回傳的描述應與磁碟檔案內容完全一致。"""
    result = _STORE.get_db_description(db_id)

    base = Path(DATA_DIR) / "databases" / db_id

    # Read expected files directly from disk (empty string if missing)
    original_path = base / "dataset_description.md"
    expected_original = original_path.read_text(encoding="utf-8") if original_path.exists() else ""

    compact_path = base / "description_compact.txt"
    expected_compact = compact_path.read_text(encoding="utf-8") if compact_path.exists() else ""

    assert result["original_desc"] == expected_original, (
        f"original_desc mismatch for {db_id}"
    )
    assert result["compact_desc"] == expected_compact, (
        f"compact_desc mismatch for {db_id}"
    )
