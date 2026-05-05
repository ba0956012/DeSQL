"""Property-based tests for chart_service.models module.

Uses hypothesis to verify correctness properties 11, 12, 13
across all valid/invalid inputs.
"""

from __future__ import annotations

from string import whitespace as whitespace_chars

import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import (
    dictionaries,
    fixed_dictionaries,
    from_type,
    just,
    lists,
    sampled_from,
    text,
)
from pydantic import ValidationError

from chart_service.models import VALID_CHART_TYPES, ChartRequest

# ── Shared strategies ────────────────────────────────────────────────

_VALID_ENGINES = ["echarts", "matplotlib"]

_valid_data = lists(
    fixed_dictionaries({"col": text(min_size=1, max_size=10)}),
    min_size=1,
    max_size=3,
)

_valid_chart_type = sampled_from(sorted(VALID_CHART_TYPES))


# ── Property 11: 空白問題被拒絕 ─────────────────────────────────────
# Feature: chart-microservice, Property 11: 空白問題被拒絕
# **Validates: Requirements 6.2**


@settings(max_examples=100)
@given(
    question=text(alphabet=whitespace_chars, min_size=0, max_size=20),
    engine=sampled_from(_VALID_ENGINES),
    data=_valid_data,
)
def test_property11_whitespace_only_question_rejected(
    question: str, engine: str, data: list[dict]
) -> None:
    """僅由空白字元組成的 question 應被 ChartRequest 驗證拒絕。"""
    with pytest.raises(ValidationError):
        ChartRequest(data=data, question=question, engine=engine)


# ── Property 12: 無效的 engine 與 chart_type 值被拒絕 ────────────────
# Feature: chart-microservice, Property 12: 無效的 engine 與 chart_type 值被拒絕
# **Validates: Requirements 6.3, 6.4**


@settings(max_examples=100)
@given(
    bad_engine=text(min_size=1, max_size=30),
    data=_valid_data,
)
def test_property12_invalid_engine_rejected(bad_engine: str, data: list[dict]) -> None:
    """不在 {"echarts", "matplotlib"} 中的 engine 值應被拒絕。"""
    assume(bad_engine not in _VALID_ENGINES)
    with pytest.raises(ValidationError):
        ChartRequest(data=data, question="test question", engine=bad_engine)


@settings(max_examples=100)
@given(
    bad_chart_type=text(min_size=1, max_size=30),
    engine=sampled_from(_VALID_ENGINES),
    data=_valid_data,
)
def test_property12_invalid_chart_type_rejected(
    bad_chart_type: str, engine: str, data: list[dict]
) -> None:
    """不在允許清單中的 chart_type 值應被拒絕。"""
    assume(bad_chart_type not in VALID_CHART_TYPES)
    with pytest.raises(ValidationError):
        ChartRequest(
            data=data,
            question="test question",
            engine=engine,
            chart_type=bad_chart_type,
        )


# ── Property 13: ChartRequest JSON 序列化往返一致性 ──────────────────
# Feature: chart-microservice, Property 13: ChartRequest JSON 序列化往返一致性
# **Validates: Requirements 9.3**


@settings(max_examples=100)
@given(
    engine=sampled_from(_VALID_ENGINES),
    chart_type=_valid_chart_type,
    question=text(min_size=1, max_size=50),
    data=_valid_data,
)
def test_property13_json_round_trip_consistency(
    engine: str, chart_type: str, question: str, data: list[dict]
) -> None:
    """合法的 ChartRequest 序列化為 JSON 再反序列化，應產生等價的物件。"""
    assume(question.strip())  # skip whitespace-only questions
    original = ChartRequest(
        data=data, question=question, engine=engine, chart_type=chart_type
    )
    json_str = original.model_dump_json()
    restored = ChartRequest.model_validate_json(json_str)
    assert original == restored
