"""API integration tests for chart_service endpoints.

Tests /health, request validation (422), and success response structure.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from chart_service.main import app
from chart_service.chart_generator import ChartGenerator
from chart_service.config import Settings


@pytest.fixture(scope="module")
def client():
    """Module-scoped TestClient with mocked OpenAI client."""
    with patch("openai.AzureOpenAI"):
        settings = Settings(
            azure_openai_api_key="test-key",
            azure_openai_endpoint="https://test.openai.azure.com",
        )
        generator = ChartGenerator(settings)

        @asynccontextmanager
        async def _test_lifespan(a):
            a.state.chart_generator = generator
            yield

        original_lifespan = app.router.lifespan_context
        app.router.lifespan_context = _test_lifespan
        with TestClient(app) as c:
            yield c
        app.router.lifespan_context = original_lifespan


# ── /health endpoint ─────────────────────────────────────


def test_health_returns_200_with_status_and_version(client):
    """GET /health returns 200 with status and version."""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "version" in data
    assert isinstance(data["version"], str)
    assert len(data["version"]) > 0


# ── Request validation (422) ─────────────────────────────


def test_empty_data_returns_422(client):
    """POST /chart/generate with empty data returns 422."""
    resp = client.post("/chart/generate", json={
        "data": [],
        "question": "test question",
        "engine": "echarts",
    })
    assert resp.status_code == 422


def test_blank_question_returns_422(client):
    """POST /chart/generate with blank question returns 422."""
    resp = client.post("/chart/generate", json={
        "data": [{"a": 1}],
        "question": "   ",
        "engine": "echarts",
    })
    assert resp.status_code == 422


def test_invalid_engine_returns_422(client):
    """POST /chart/generate with invalid engine returns 422."""
    resp = client.post("/chart/generate", json={
        "data": [{"a": 1}],
        "question": "test question",
        "engine": "plotly",
    })
    assert resp.status_code == 422


def test_invalid_chart_type_returns_422(client):
    """POST /chart/generate with invalid chart_type returns 422."""
    resp = client.post("/chart/generate", json={
        "data": [{"a": 1}],
        "question": "test question",
        "engine": "echarts",
        "chart_type": "invalid_type",
    })
    assert resp.status_code == 422


# ── Success response structure (table type, mock LLM) ────


def test_echarts_table_returns_success_with_html(client):
    """POST /chart/generate with table type returns ChartResponse with chart_html."""
    resp = client.post("/chart/generate", json={
        "data": [{"name": "Alice", "score": 90}, {"name": "Bob", "score": 85}],
        "question": "Show scores",
        "engine": "echarts",
        "chart_type": "table",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["chart_type"] == "table"
    assert "<table" in data["chart_html"]
    assert "reason" in data


def test_matplotlib_table_returns_success_with_image(client):
    """POST /chart/generate with matplotlib table returns base64 image."""
    resp = client.post("/chart/generate", json={
        "data": [{"name": "Alice", "score": 90}, {"name": "Bob", "score": 85}],
        "question": "Show scores",
        "engine": "matplotlib",
        "chart_type": "table",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["chart_type"] == "table"
    assert len(data["chart_image"]) > 0
    assert "reason" in data
