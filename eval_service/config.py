"""環境變數設定：使用 pydantic-settings 從環境變數讀取所有設定。"""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """應用程式設定，所有欄位均可透過同名環境變數覆寫。"""

    azure_openai_api_key: str = ""
    azure_openai_endpoint: str = ""
    openai_api_version: str = "2024-12-01-preview"
    llm_model: str = "gpt-4.1-mini"
    llm_deployment: str = "gpt-4.1-mini"
    llm_temperature: float = 0
    data_dir: str = "./data"
    host: str = "0.0.0.0"
    port: int = 8080

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    """回傳快取的 Settings 單例。"""
    return Settings()
