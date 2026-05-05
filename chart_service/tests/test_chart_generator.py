"""Property-based tests for chart_service.chart_generator module.

Uses hypothesis to verify correctness properties 1, 2, 3, 4, 6
across all valid inputs. All tests use a mocked OpenAI client.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

from hypothesis import given, settings as h_settings
from hypothesis.strategies import (
    dictionaries,
    fixed_dictionaries,
    from_regex,
    just,
    lists,
    sampled_from,
    text,
)

from chart_service.chart_generator import ChartGenerator
from chart_service.config import Settings

# ── Helpers ──────────────────────────────────────────────────────────


def _make_generator(llm_return: str | list[str] | None = None) -> ChartGenerator:
    """Build a ChartGenerator with a mocked AzureOpenAI client.

    *llm_return* controls what ``client.chat.completions.create()`` returns:
    - ``str``: every call returns that string
    - ``list[str]``: successive calls return each element in order
    - ``None``: returns empty string
    """
    settings = Settings(
        azure_openai_api_key="fake-key",
        azure_openai_endpoint="https://fake.openai.azure.com",
    )

    with patch("openai.AzureOpenAI", autospec=False) as MockCls:
        mock_client = MagicMock()
        MockCls.return_value = mock_client

        if isinstance(llm_return, list):
            responses = []
            for text_val in llm_return:
                msg = MagicMock()
                msg.content = text_val
                choice = MagicMock()
                choice.message = msg
                resp = MagicMock()
                resp.choices = [choice]
                responses.append(resp)
            mock_client.chat.completions.create.side_effect = responses
        else:
            msg = MagicMock()
            msg.content = llm_return or ""
            choice = MagicMock()
            choice.message = msg
            resp = MagicMock()
            resp.choices = [choice]
            mock_client.chat.completions.create.return_value = resp

        gen = ChartGenerator(settings)
        # Store mock for assertion access
        gen._mock_client = mock_client
        return gen


# ── Strategies ───────────────────────────────────────────────────────

_non_blank_question = text(min_size=1).filter(lambda s: s.strip())

_simple_row = fixed_dictionaries({"name": text(min_size=1), "value": just(42)})

_non_empty_data = lists(_simple_row, min_size=1, max_size=5)

_non_auto_chart_types = sampled_from(
    ["bar", "pie", "line", "treemap", "scatter", "table"]
)


# ── Property 1: Judge 判斷不需圖表時回傳空結果 ──────────────────────
# Feature: chart-microservice, Property 1: Judge 判斷不需圖表時回傳空結果
# **Validates: Requirements 2.2**


@h_settings(max_examples=100)
@given(data=_non_empty_data, question=_non_blank_question)
def test_property1_judge_no_chart_returns_none(data: list[dict], question: str) -> None:
    """When LLM Judge returns should_chart=False, response has chart_type='none' and empty content."""
    judge_json = json.dumps(
        {
            "insight": "不需要圖表",
            "should_chart": False,
            "chart_type": "none",
        }
    )
    gen = _make_generator(llm_return=judge_json)
    resp = gen.generate(
        data=data, question=question, engine="echarts", chart_type="auto"
    )
    assert resp.chart_type == "none"
    assert resp.chart_html == ""
    assert resp.chart_option == ""
    assert resp.chart_image == ""


# ── Property 2: 指定 chart_type 時跳過 LLM 判斷 ────────────────────
# Feature: chart-microservice, Property 2: 指定 chart_type 時跳過 LLM 判斷
# **Validates: Requirements 2.3**


@h_settings(max_examples=100)
@given(
    data=_non_empty_data,
    question=_non_blank_question,
    chart_type=_non_auto_chart_types,
)
def test_property2_explicit_chart_type_skips_judge(
    data: list[dict], question: str, chart_type: str
) -> None:
    """When chart_type != 'auto', LLM judge is NOT called for judging."""
    # For table types, no LLM call is needed at all.
    # For non-table types, LLM is called for code generation (not judging).
    gen = _make_generator(llm_return="x = 1")  # dummy code for non-table
    resp = gen.generate(
        data=data, question=question, engine="matplotlib", chart_type=chart_type
    )

    # The judge prompt contains "你是資料視覺化顧問" — verify it was NOT used.
    for call in gen._mock_client.chat.completions.create.call_args_list:
        messages = (
            call.kwargs.get("messages") or call.args[0]
            if call.args
            else call.kwargs.get("messages", [])
        )
        if isinstance(messages, list):
            for m in messages:
                content = (
                    m.get("content", "")
                    if isinstance(m, dict)
                    else getattr(m, "content", "")
                )
                assert (
                    "你是資料視覺化顧問" not in content
                ), "Judge was called when chart_type was explicit"


# ── Property 3: 無法解析的 Judge 回應回退為 "none" ──────────────────
# Feature: chart-microservice, Property 3: 無法解析的 Judge 回應回退為 "none"
# **Validates: Requirements 2.4**


_bad_json = sampled_from(
    [
        "this is not json",
        "{ broken",
        "```json\nnot valid\n```",
        "",
        "true",
        "[1, 2, 3]",
        "should_chart: yes",
    ]
)


@h_settings(max_examples=100)
@given(data=_non_empty_data, question=_non_blank_question, bad_response=_bad_json)
def test_property3_unparseable_judge_falls_back_to_none(
    data: list[dict], question: str, bad_response: str
) -> None:
    """When LLM Judge returns unparseable JSON, response has chart_type='none'."""
    gen = _make_generator(llm_return=bad_response)
    resp = gen.generate(
        data=data, question=question, engine="echarts", chart_type="auto"
    )
    assert resp.chart_type == "none"
    assert resp.chart_html == ""
    assert resp.chart_option == ""
    assert resp.chart_image == ""


# ── Property 4: ECharts table 類型生成 HTML 表格 ────────────────────
# Feature: chart-microservice, Property 4: ECharts table 類型生成 HTML 表格
# **Validates: Requirements 3.1**


@h_settings(max_examples=100)
@given(data=_non_empty_data, question=_non_blank_question)
def test_property4_echarts_table_generates_html_table(
    data: list[dict], question: str
) -> None:
    """When engine=echarts and chart_type=table, response chart_html contains '<table'."""
    gen = _make_generator()  # No LLM call needed for table rendering
    resp = gen.generate(
        data=data, question=question, engine="echarts", chart_type="table"
    )
    assert resp.chart_type == "table"
    assert "<table" in resp.chart_html


# ── Property 6: Matplotlib table 類型生成合法 base64 PNG ────────────
# Feature: chart-microservice, Property 6: Matplotlib table 類型生成合法 base64 PNG
# **Validates: Requirements 4.1**

_PNG_HEADER = b"\x89PNG\r\n\x1a\n"


@h_settings(max_examples=20, deadline=None)
@given(data=_non_empty_data, question=_non_blank_question)
def test_property6_matplotlib_table_generates_valid_base64_png(
    data: list[dict], question: str
) -> None:
    """When engine=matplotlib and chart_type=table, response chart_image is valid base64 PNG."""
    gen = _make_generator()  # No LLM call needed for table rendering
    resp = gen.generate(
        data=data, question=question, engine="matplotlib", chart_type="table"
    )
    assert resp.chart_type == "table"
    assert resp.chart_image != ""
    decoded = base64.b64decode(resp.chart_image)
    assert decoded[:8] == _PNG_HEADER, "Decoded image does not have PNG header"
