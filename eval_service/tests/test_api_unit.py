"""Unit tests for eval_service API endpoints.

Tests edge cases and error conditions: health check, missing description files,
Gold SQL failure, and LLM Judge errors.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from fastapi.testclient import TestClient

from eval_service.datastore import DataStore
from eval_service.main import app

DATA_DIR = str(Path(__file__).resolve().parents[2] / "eval")


@pytest.fixture(scope="module")
def client():
    """Module-scoped TestClient with real DataStore and mocked LLMJudge."""
    with patch("openai.AzureOpenAI"):
        ds = DataStore(DATA_DIR)
        mock_judge = MagicMock()
        mock_judge.judge.return_value = {"correct": True, "reason": "mock"}

        @asynccontextmanager
        async def _noop_lifespan(a):
            a.state.datastore = ds
            a.state.judge = mock_judge
            yield

        original_lifespan = app.router.lifespan_context
        app.router.lifespan_context = _noop_lifespan
        with TestClient(app) as c:
            yield c
        app.router.lifespan_context = original_lifespan


# ── 健康檢查 (Requirements 6.1, 6.2) ────────────────────────────────


def test_health_returns_200_with_status_and_version(client):
    """GET /health 回傳 200，包含 status 和 version。"""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "version" in data
    assert isinstance(data["version"], str)
    assert len(data["version"]) > 0


# ── 描述檔案缺失 (Requirement 1.3) ──────────────────────────────────


def test_description_missing_files_returns_empty_strings(client):
    """當描述檔案不存在時，回傳空字串而非錯誤。"""
    ds = app.state.datastore
    # Find a db_id that exists but mock the file reading to return empty
    db_ids = ds.get_db_ids()
    assert len(db_ids) > 0

    # Patch _read_file_or_empty to always return empty string
    with patch.object(DataStore, "_read_file_or_empty", return_value=""):
        resp = client.get(f"/databases/{db_ids[0]}/description")

    assert resp.status_code == 200
    data = resp.json()
    assert data["original_desc"] == ""
    assert data["compact_desc"] == ""


# ── Gold SQL 執行失敗 (Requirement 4.2) ─────────────────────────────


def test_gold_sql_failure_returns_500(client):
    """Gold SQL 執行失敗時回傳 500 錯誤。"""
    ds = app.state.datastore
    db_ids = ds.get_db_ids()
    # Get a valid question
    hints = ds.get_db_hints(db_ids[0])
    qid = hints[0]["question_id"]

    with patch(
        "eval_service.routers.eval.run_gold_sql", side_effect=Exception("sqlite error")
    ):
        resp = client.post(
            f"/databases/{db_ids[0]}/questions/{qid}/evaluate",
            json={"answer": "test answer"},
        )

    assert resp.status_code == 500
    data = resp.json()
    assert "Gold SQL execution failed" in data["detail"]


# ── LLM Judge 異常 (Requirement 4.3) ────────────────────────────────


def test_llm_judge_error_returns_correct_false(client):
    """LLM Judge 回傳異常時，correct 為 false 且附帶錯誤原因。"""
    ds = app.state.datastore
    db_ids = ds.get_db_ids()
    hints = ds.get_db_hints(db_ids[0])
    qid = hints[0]["question_id"]

    # Mock judge to return error result (as the judge.py does internally)
    error_result = {"correct": False, "reason": "LLM judge error: connection timeout"}
    app.state.judge.judge.return_value = error_result

    with patch("eval_service.routers.eval.run_gold_sql", return_value=[{"col": "val"}]):
        resp = client.post(
            f"/databases/{db_ids[0]}/questions/{qid}/evaluate",
            json={"answer": "test answer"},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["correct"] is False
    assert "error" in data["reason"].lower() or "LLM" in data["reason"]

    # Reset mock for other tests
    app.state.judge.judge.return_value = {"correct": True, "reason": "mock"}
