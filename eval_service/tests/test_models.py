"""Tests for eval_service.models module."""

import pytest
from pydantic import ValidationError

from eval_service.models import (
    DescriptionResponse,
    ErrorResponse,
    EvaluateRequest,
    EvaluateResponse,
    HealthResponse,
    HintItem,
    HintsResponse,
    QuestionCountResponse,
    QuestionResponse,
)


class TestEvaluateRequest:
    def test_valid_answer(self):
        req = EvaluateRequest(answer="SELECT * FROM t")
        assert req.answer == "SELECT * FROM t"

    def test_empty_answer_rejected(self):
        with pytest.raises(ValidationError):
            EvaluateRequest(answer="")

    def test_missing_answer_rejected(self):
        with pytest.raises(ValidationError):
            EvaluateRequest()


class TestHealthResponse:
    def test_default_status(self):
        resp = HealthResponse(version="1.0.0")
        assert resp.status == "ok"
        assert resp.version == "1.0.0"

    def test_custom_status(self):
        resp = HealthResponse(status="degraded", version="1.0.0")
        assert resp.status == "degraded"


class TestDescriptionResponse:
    def test_fields(self):
        resp = DescriptionResponse(
            db_id="financial",
            original_desc="# Financial DB",
            compact_desc="compact info",
        )
        assert resp.db_id == "financial"
        assert resp.original_desc == "# Financial DB"
        assert resp.compact_desc == "compact info"

    def test_empty_descriptions(self):
        resp = DescriptionResponse(db_id="test", original_desc="", compact_desc="")
        assert resp.original_desc == ""
        assert resp.compact_desc == ""


class TestQuestionCountResponse:
    def test_fields(self):
        resp = QuestionCountResponse(
            db_id="financial",
            total=10,
            by_difficulty={"simple": 5, "moderate": 3, "challenging": 2},
        )
        assert resp.total == 10
        assert resp.by_difficulty["simple"] == 5


class TestQuestionResponse:
    def test_fields(self):
        resp = QuestionResponse(
            question_id=1,
            db_id="financial",
            question="How many accounts?",
            evidence="See table accounts",
            difficulty="simple",
        )
        assert resp.question_id == 1
        assert resp.difficulty == "simple"


class TestHintItem:
    def test_fields(self):
        item = HintItem(question_id=1, question="Q?", evidence="hint text")
        assert item.question_id == 1
        assert item.evidence == "hint text"

    def test_empty_evidence(self):
        item = HintItem(question_id=2, question="Q2?", evidence="")
        assert item.evidence == ""


class TestHintsResponse:
    def test_fields(self):
        resp = HintsResponse(
            db_id="financial",
            total=1,
            hints=[HintItem(question_id=1, question="Q?", evidence="e")],
        )
        assert resp.total == 1
        assert len(resp.hints) == 1

    def test_empty_hints(self):
        resp = HintsResponse(db_id="test", total=0, hints=[])
        assert resp.hints == []


class TestEvaluateResponse:
    def test_correct(self):
        resp = EvaluateResponse(
            question_id=1, db_id="financial", correct=True, reason="Match"
        )
        assert resp.correct is True

    def test_incorrect(self):
        resp = EvaluateResponse(
            question_id=1, db_id="financial", correct=False, reason="Mismatch"
        )
        assert resp.correct is False


class TestErrorResponse:
    def test_detail(self):
        resp = ErrorResponse(detail="Not found")
        assert resp.detail == "Not found"
