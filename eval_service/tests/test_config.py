"""Tests for eval_service.config module."""

import os

import pytest


class TestSettingsDefaults:
    """Verify default values when no env vars are set."""

    def test_default_values(self, monkeypatch):
        # Clear any env vars that could interfere
        for key in (
            "AZURE_OPENAI_API_KEY",
            "AZURE_OPENAI_ENDPOINT",
            "OPENAI_API_VERSION",
            "LLM_MODEL",
            "LLM_DEPLOYMENT",
            "LLM_TEMPERATURE",
            "DATA_DIR",
            "HOST",
            "PORT",
        ):
            monkeypatch.delenv(key, raising=False)

        from eval_service.config import Settings

        s = Settings(_env_file=None)

        assert s.azure_openai_api_key == ""
        assert s.azure_openai_endpoint == ""
        assert s.openai_api_version == "2024-12-01-preview"
        assert s.llm_model == "gpt-4.1-mini"
        assert s.llm_deployment == "gpt-4.1-mini"
        assert s.llm_temperature == 0
        assert s.data_dir == "./data"
        assert s.host == "0.0.0.0"
        assert s.port == 8080


class TestSettingsFromEnv:
    """Verify settings are read from environment variables."""

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "test-key")
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://test.openai.azure.com")
        monkeypatch.setenv("OPENAI_API_VERSION", "2025-01-01")
        monkeypatch.setenv("LLM_MODEL", "gpt-5")
        monkeypatch.setenv("LLM_DEPLOYMENT", "gpt-5-deploy")
        monkeypatch.setenv("LLM_TEMPERATURE", "0.7")
        monkeypatch.setenv("DATA_DIR", "/custom/data")
        monkeypatch.setenv("HOST", "127.0.0.1")
        monkeypatch.setenv("PORT", "9090")

        from eval_service.config import Settings

        s = Settings(_env_file=None)

        assert s.azure_openai_api_key == "test-key"
        assert s.azure_openai_endpoint == "https://test.openai.azure.com"
        assert s.openai_api_version == "2025-01-01"
        assert s.llm_model == "gpt-5"
        assert s.llm_deployment == "gpt-5-deploy"
        assert s.llm_temperature == 0.7
        assert s.data_dir == "/custom/data"
        assert s.host == "127.0.0.1"
        assert s.port == 9090

    def test_case_insensitive_env(self, monkeypatch):
        """pydantic-settings reads env vars case-insensitively by default."""
        monkeypatch.setenv("azure_openai_api_key", "lower-key")

        from eval_service.config import Settings

        s = Settings(_env_file=None)
        assert s.azure_openai_api_key == "lower-key"


class TestGetSettings:
    """Verify the cached get_settings helper."""

    def test_returns_settings_instance(self):
        from eval_service.config import Settings, get_settings

        get_settings.cache_clear()
        s = get_settings()
        assert isinstance(s, Settings)

    def test_cached(self):
        from eval_service.config import get_settings

        get_settings.cache_clear()
        assert get_settings() is get_settings()
