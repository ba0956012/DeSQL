"""Pydantic 請求/回應模型定義。"""

from typing import Literal

from pydantic import BaseModel, Field, field_validator

VALID_CHART_TYPES = {
    "auto",
    "bar",
    "pie",
    "line",
    "treemap",
    "scatter",
    "table",
    "none",
    "heatmap",
}


# ── 請求模型 ──────────────────────────────────────────────


class ChartRequest(BaseModel):
    data: list[dict] = Field(..., min_length=1)
    question: str = Field(..., min_length=1)
    engine: Literal["echarts", "matplotlib"]
    chart_type: str = "auto"

    @field_validator("question")
    @classmethod
    def question_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question 不可為空白")
        return v

    @field_validator("chart_type")
    @classmethod
    def valid_chart_type(cls, v: str) -> str:
        if v not in VALID_CHART_TYPES:
            raise ValueError(f"chart_type must be one of {VALID_CHART_TYPES}")
        return v


# ── 回應模型 ──────────────────────────────────────────────


class ChartResponse(BaseModel):
    chart_type: str
    reason: str
    chart_html: str = ""
    chart_option: str = ""
    chart_image: str = ""


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
