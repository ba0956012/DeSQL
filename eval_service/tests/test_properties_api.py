"""Property-based tests for eval_service API endpoints.

Uses FastAPI TestClient with real eval/ data and mocked LLMJudge
to verify correctness properties 3-7 across all valid inputs.
"""

from __future__ import annotations

import json
from pathlib import Path
from string import whitespace as whitespace_characters
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings, assume
from hypothesis.strategies import sampled_from, text, integers, characters

from eval_service.datastore import DataStore
from eval_service.main import app

# ── Shared data & fixtures ──────────────────────────────────────────

DATA_DIR = str(Path(__file__).resolve().parents[2] / "eval")

_dev_path = Path(DATA_DIR) / "dev.json"
with open(_dev_path, encoding="utf-8") as _f:
    _DEV_DATA: list[dict] = json.load(_f)

_VALID_DB_IDS = sorted({item["db_id"] for item in _DEV_DATA})

_BY_DB: dict[str, list[dict]] = {}
for _item in _DEV_DATA:
    _BY_DB.setdefault(_item["db_id"], []).append(_item)

# Build valid (db_id, question_id) pairs
_VALID_PAIRS = [(item["db_id"], item["question_id"]) for item in _DEV_DATA]

# Build set of valid question_ids per db_id for filtering
_VALID_QIDS_BY_DB: dict[str, set[int]] = {}
for _item in _DEV_DATA:
    _VALID_QIDS_BY_DB.setdefault(_item["db_id"], set()).add(_item["question_id"])

# Index by (db_id, question_id) for ground-truth lookups
_BY_KEY: dict[tuple[str, int], dict] = {}
for _item in _DEV_DATA:
    _BY_KEY[(_item["db_id"], _item["question_id"])] = _item


@pytest.fixture(scope="module")
def client():
    """Module-scoped TestClient with real DataStore and mocked LLMJudge."""
    with patch("openai.AzureOpenAI"):
        app.state.datastore = DataStore(DATA_DIR)
        app.state.judge = MagicMock()
        app.state.judge.judge.return_value = {"correct": True, "reason": "mock"}
        with TestClient(app) as c:
            yield c


# Module-level client for hypothesis tests (cannot use pytest fixtures)
_datastore = DataStore(DATA_DIR)
_mock_judge = MagicMock()
_mock_judge.judge.return_value = {"correct": True, "reason": "mock"}
app.state.datastore = _datastore
app.state.judge = _mock_judge
_CLIENT = TestClient(app)


# ── Property 3: 題目資料一致性 ──────────────────────────────────────
# Feature: eval-microservice, Property 3: 題目資料一致性
# **Validates: Requirements 3.1**


@settings(max_examples=100)
@given(pair=sampled_from(_VALID_PAIRS))
def test_property3_question_data_consistency(pair: tuple[str, int]) -> None:
    """API 回傳的 question、evidence、difficulty 應與 dev.json 一致。"""
    db_id, question_id = pair
    resp = _CLIENT.get(f"/databases/{db_id}/questions/{question_id}")
    assert resp.status_code == 200

    data = resp.json()
    expected = _BY_KEY[(db_id, question_id)]

    assert (
        data["question"] == expected["question"]
    ), f"question mismatch for ({db_id}, {question_id})"
    assert data["evidence"] == expected.get(
        "evidence", ""
    ), f"evidence mismatch for ({db_id}, {question_id})"
    assert data["difficulty"] == expected.get(
        "difficulty", ""
    ), f"difficulty mismatch for ({db_id}, {question_id})"


# ── Property 4: 提示列表一致性 ──────────────────────────────────────
# Feature: eval-microservice, Property 4: 提示列表一致性
# **Validates: Requirements 5.1**


@settings(max_examples=100)
@given(db_id=sampled_from(_VALID_DB_IDS))
def test_property4_hints_list_consistency(db_id: str) -> None:
    """hints 列表長度 == 題目總數，且每項資料與 dev.json 一致。"""
    resp = _CLIENT.get(f"/databases/{db_id}/hints")
    assert resp.status_code == 200

    data = resp.json()
    expected_questions = _BY_DB[db_id]

    # Length must match
    assert data["total"] == len(
        expected_questions
    ), f"hints total {data['total']} != expected {len(expected_questions)} for {db_id}"
    assert len(data["hints"]) == len(
        expected_questions
    ), f"hints list length {len(data['hints'])} != expected {len(expected_questions)} for {db_id}"

    # Each hint must match dev.json
    hints_by_qid = {h["question_id"]: h for h in data["hints"]}
    for q in expected_questions:
        qid = q["question_id"]
        assert qid in hints_by_qid, f"question_id {qid} missing from hints for {db_id}"
        hint = hints_by_qid[qid]
        assert (
            hint["question"] == q["question"]
        ), f"question mismatch for ({db_id}, {qid})"
        assert hint["evidence"] == q.get(
            "evidence", ""
        ), f"evidence mismatch for ({db_id}, {qid})"


# ── Property 5: 無效 DB_ID 一致回傳 404 ─────────────────────────────
# Feature: eval-microservice, Property 5: 無效 DB_ID 一致回傳 404
# **Validates: Requirements 1.2, 2.2, 3.3, 4.4, 5.3**

_VALID_DB_ID_SET = set(_VALID_DB_IDS)


@settings(max_examples=100)
@given(
    fake_db=text(
        alphabet=characters(
            whitelist_categories=("L", "N", "P", "S"), blacklist_characters="/"
        ),
        min_size=1,
        max_size=50,
    )
)
def test_property5_invalid_db_id_returns_404(fake_db: str) -> None:
    """所有端點對不存在的 DB_ID 回傳 404。"""
    assume(fake_db not in _VALID_DB_ID_SET)

    endpoints = [
        f"/databases/{fake_db}/description",
        f"/databases/{fake_db}/count",
        f"/databases/{fake_db}/hints",
        f"/databases/{fake_db}/questions/0",
    ]

    for url in endpoints:
        resp = _CLIENT.get(url)
        assert (
            resp.status_code == 404
        ), f"Expected 404 for GET {url}, got {resp.status_code}"

    # POST evaluate endpoint
    resp = _CLIENT.post(
        f"/databases/{fake_db}/questions/0/evaluate",
        json={"answer": "test"},
    )
    assert (
        resp.status_code == 404
    ), f"Expected 404 for POST evaluate with db_id={fake_db!r}, got {resp.status_code}"


# ── Property 6: 無效 Question_ID 回傳 404 ───────────────────────────
# Feature: eval-microservice, Property 6: 無效 Question_ID 回傳 404
# **Validates: Requirements 3.2**


@settings(max_examples=100)
@given(
    db_id=sampled_from(_VALID_DB_IDS),
    question_id=integers(min_value=-10000, max_value=100000),
)
def test_property6_invalid_question_id_returns_404(
    db_id: str, question_id: int
) -> None:
    """有效 DB_ID + 不存在的 Question_ID → 404。"""
    valid_qids = _VALID_QIDS_BY_DB[db_id]
    assume(question_id not in valid_qids)

    # GET question
    resp = _CLIENT.get(f"/databases/{db_id}/questions/{question_id}")
    assert (
        resp.status_code == 404
    ), f"Expected 404 for GET question ({db_id}, {question_id}), got {resp.status_code}"

    # POST evaluate
    resp = _CLIENT.post(
        f"/databases/{db_id}/questions/{question_id}/evaluate",
        json={"answer": "test"},
    )
    assert (
        resp.status_code == 404
    ), f"Expected 404 for POST evaluate ({db_id}, {question_id}), got {resp.status_code}"


# ── Property 7: 空答案拒絕 ──────────────────────────────────────────
# Feature: eval-microservice, Property 7: 空答案拒絕
# **Validates: Requirements 4.5**

# Pick a valid pair for the evaluate endpoint
_SAMPLE_DB_ID = _VALID_PAIRS[0][0]
_SAMPLE_QID = _VALID_PAIRS[0][1]


@settings(max_examples=100)
@given(answer=text(alphabet=whitespace_characters, min_size=0, max_size=20))
def test_property7_empty_answer_rejected(answer: str) -> None:
    """空白字串答案應被拒絕（HTTP 400 或 422）。"""
    resp = _CLIENT.post(
        f"/databases/{_SAMPLE_DB_ID}/questions/{_SAMPLE_QID}/evaluate",
        json={"answer": answer},
    )
    assert resp.status_code in (
        400,
        422,
    ), f"Expected 400/422 for whitespace answer {answer!r}, got {resp.status_code}"
