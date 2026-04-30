"""Tests for eval_service/judge.py — run_gold_sql and LLMJudge."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from eval_service.judge import LLMJudge, run_gold_sql
from eval_service.config import Settings


# ---------------------------------------------------------------------------
# run_gold_sql tests
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_sqlite(tmp_path):
    """Create a temporary SQLite database with sample data."""
    db_path = str(tmp_path / "test.sqlite")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE scores (name TEXT, score INTEGER)")
    conn.execute("INSERT INTO scores VALUES ('Alice', 90)")
    conn.execute("INSERT INTO scores VALUES ('Bob', 85)")
    conn.commit()
    conn.close()
    return db_path


def test_run_gold_sql_returns_list_of_dicts(tmp_sqlite):
    result = run_gold_sql(tmp_sqlite, "SELECT name, score FROM scores ORDER BY name")
    assert result == [
        {"name": "Alice", "score": 90},
        {"name": "Bob", "score": 85},
    ]


def test_run_gold_sql_single_value(tmp_sqlite):
    result = run_gold_sql(tmp_sqlite, "SELECT COUNT(*) AS cnt FROM scores")
    assert result == [{"cnt": 2}]


def test_run_gold_sql_empty_result(tmp_sqlite):
    result = run_gold_sql(tmp_sqlite, "SELECT * FROM scores WHERE score > 100")
    assert result == []


def test_run_gold_sql_invalid_sql_raises(tmp_sqlite):
    with pytest.raises(sqlite3.OperationalError):
        run_gold_sql(tmp_sqlite, "SELECT * FROM nonexistent_table")


# ---------------------------------------------------------------------------
# LLMJudge tests
# ---------------------------------------------------------------------------

def _make_settings(**overrides) -> Settings:
    defaults = {
        "azure_openai_api_key": "test-key",
        "azure_openai_endpoint": "https://test.openai.azure.com",
        "openai_api_version": "2024-12-01-preview",
        "llm_deployment": "gpt-4.1-mini",
        "llm_temperature": 0,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _make_judge_with_mock(llm_content: str = '{"correct": true, "reason": "ok"}',
                          side_effect=None):
    """Create an LLMJudge with a mocked OpenAI client."""
    with patch("openai.AzureOpenAI") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client

        if side_effect:
            mock_client.chat.completions.create.side_effect = side_effect
        else:
            mock_choice = MagicMock()
            mock_choice.message.content = llm_content
            mock_response = MagicMock()
            mock_response.choices = [mock_choice]
            mock_client.chat.completions.create.return_value = mock_response

        judge = LLMJudge(_make_settings())
    return judge, mock_client


def test_judge_correct_response():
    """LLMJudge parses a correct JSON response from the LLM."""
    judge, _ = _make_judge_with_mock('{"correct": true, "reason": "數值一致"}')
    result = judge.judge("問題", [{"val": 42}], "42")
    assert result == {"correct": True, "reason": "數值一致"}


def test_judge_incorrect_response():
    judge, _ = _make_judge_with_mock('{"correct": false, "reason": "答案不符"}')
    result = judge.judge("問題", [{"a": 1}], "wrong answer")
    assert result["correct"] is False


def test_judge_strips_markdown_fences():
    judge, _ = _make_judge_with_mock('```json\n{"correct": true, "reason": "ok"}\n```')
    result = judge.judge("q", [{"x": 1}], "1")
    assert result == {"correct": True, "reason": "ok"}


def test_judge_parse_error():
    """When LLM returns non-JSON, judge returns correct=False with parse error."""
    judge, _ = _make_judge_with_mock("This is not JSON at all")
    result = judge.judge("q", [{"x": 1}], "answer")
    assert result["correct"] is False
    assert "parse error" in result["reason"]


def test_judge_api_error():
    """When the OpenAI API call raises, judge returns correct=False with error."""
    judge, _ = _make_judge_with_mock(side_effect=RuntimeError("connection failed"))
    result = judge.judge("q", [{"x": 1}], "answer")
    assert result["correct"] is False
    assert "LLM judge error" in result["reason"]


def test_judge_expected_str_single_value():
    """When expected has 1 row with 1 column, expected_str is the plain value."""
    judge, mock_client = _make_judge_with_mock()
    judge.judge("q", [{"count": 5}], "5")

    call_args = mock_client.chat.completions.create.call_args
    prompt_content = call_args[1]["messages"][0]["content"]
    assert "標準答案：5" in prompt_content


def test_judge_expected_str_multiple_rows():
    """When expected has <=10 rows, expected_str is full JSON."""
    judge, mock_client = _make_judge_with_mock()
    expected = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    judge.judge("q", expected, "answer")

    call_args = mock_client.chat.completions.create.call_args
    prompt_content = call_args[1]["messages"][0]["content"]
    assert '"id": 1' in prompt_content
    assert '"id": 2' in prompt_content


def test_judge_expected_str_many_rows():
    """When expected has >10 rows, expected_str is truncated with count note."""
    judge, mock_client = _make_judge_with_mock()
    expected = [{"id": i} for i in range(15)]
    judge.judge("q", expected, "answer")

    call_args = mock_client.chat.completions.create.call_args
    prompt_content = call_args[1]["messages"][0]["content"]
    assert "共 15 筆" in prompt_content
    assert "僅顯示前 10 筆" in prompt_content
