"""Tests for eval_service.datastore module."""

import json

import pytest

from eval_service.datastore import DataStore


@pytest.fixture
def sample_data():
    """Minimal dev.json data for testing."""
    return [
        {
            "question_id": 0,
            "db_id": "db_a",
            "question": "Q0?",
            "evidence": "hint for Q0",
            "SQL": "SELECT 1",
            "difficulty": "simple",
        },
        {
            "question_id": 1,
            "db_id": "db_a",
            "question": "Q1?",
            "evidence": "",
            "SQL": "SELECT 2",
            "difficulty": "moderate",
        },
        {
            "question_id": 2,
            "db_id": "db_b",
            "question": "Q2?",
            "evidence": "hint for Q2",
            "SQL": "SELECT 3",
            "difficulty": "challenging",
        },
    ]


@pytest.fixture
def data_dir(tmp_path, sample_data):
    """Create a temporary data directory with dev.json and database description files."""
    # Write dev.json
    (tmp_path / "dev.json").write_text(json.dumps(sample_data), encoding="utf-8")

    # Create database directories with description files
    db_a_dir = tmp_path / "databases" / "db_a"
    db_a_dir.mkdir(parents=True)
    (db_a_dir / "dataset_description.md").write_text(
        "# DB A original", encoding="utf-8"
    )
    (db_a_dir / "description_compact.txt").write_text("DB A compact", encoding="utf-8")
    (db_a_dir / "db_a.sqlite").write_bytes(b"")

    db_b_dir = tmp_path / "databases" / "db_b"
    db_b_dir.mkdir(parents=True)
    # db_b has no description files — tests missing-file fallback
    (db_b_dir / "db_b.sqlite").write_bytes(b"")

    return tmp_path


@pytest.fixture
def store(data_dir):
    return DataStore(str(data_dir))


class TestInit:
    def test_loads_dev_json(self, store):
        assert store.db_exists("db_a")
        assert store.db_exists("db_b")

    def test_raises_on_missing_dev_json(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            DataStore(str(tmp_path))


class TestGetDbIds:
    def test_returns_sorted(self, store):
        assert store.get_db_ids() == ["db_a", "db_b"]


class TestDbExists:
    def test_existing(self, store):
        assert store.db_exists("db_a") is True

    def test_not_existing(self, store):
        assert store.db_exists("no_such_db") is False


class TestGetDbDescription:
    def test_both_files_present(self, store):
        desc = store.get_db_description("db_a")
        assert desc["original_desc"] == "# DB A original"
        assert desc["compact_desc"] == "DB A compact"

    def test_missing_files_return_empty(self, store):
        desc = store.get_db_description("db_b")
        assert desc["original_desc"] == ""
        assert desc["compact_desc"] == ""


class TestGetDbQuestionCount:
    def test_counts_db_a(self, store):
        result = store.get_db_question_count("db_a")
        assert result["total"] == 2
        assert result["by_difficulty"] == {"simple": 1, "moderate": 1, "challenging": 0}

    def test_counts_db_b(self, store):
        result = store.get_db_question_count("db_b")
        assert result["total"] == 1
        assert result["by_difficulty"] == {"simple": 0, "moderate": 0, "challenging": 1}

    def test_nonexistent_db(self, store):
        result = store.get_db_question_count("nope")
        assert result["total"] == 0


class TestGetQuestion:
    def test_found(self, store):
        q = store.get_question("db_a", 0)
        assert q is not None
        assert q["question"] == "Q0?"

    def test_not_found_bad_qid(self, store):
        assert store.get_question("db_a", 999) is None

    def test_not_found_bad_db(self, store):
        assert store.get_question("nope", 0) is None


class TestGetDbHints:
    def test_returns_hints(self, store):
        hints = store.get_db_hints("db_a")
        assert len(hints) == 2
        assert hints[0]["question_id"] == 0
        assert hints[0]["evidence"] == "hint for Q0"
        # Empty evidence stays as empty string
        assert hints[1]["evidence"] == ""

    def test_nonexistent_db(self, store):
        assert store.get_db_hints("nope") == []


class TestGetSqlitePath:
    def test_path_format(self, store, data_dir):
        path = store.get_sqlite_path("db_a")
        assert path == str(data_dir / "databases" / "db_a" / "db_a.sqlite")
