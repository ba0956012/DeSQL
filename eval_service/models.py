"""Pydantic 請求/回應模型定義。"""

from pydantic import BaseModel, Field, field_validator


# ── 請求模型 ──────────────────────────────────────────────

class EvaluateRequest(BaseModel):
    answer: str = Field(..., min_length=1, description="使用者的答案文字")

    @field_validator("answer")
    @classmethod
    def answer_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("答案不可為空白")
        return v


# ── 回應模型 ──────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str


class DescriptionResponse(BaseModel):
    db_id: str
    original_desc: str
    compact_desc: str


class QuestionCountResponse(BaseModel):
    db_id: str
    total: int
    by_difficulty: dict[str, int]


class QuestionResponse(BaseModel):
    question_id: int
    db_id: str
    question: str
    evidence: str
    difficulty: str


class HintItem(BaseModel):
    question_id: int
    question: str
    evidence: str


class HintsResponse(BaseModel):
    db_id: str
    total: int
    hints: list[HintItem]


class EvaluateResponse(BaseModel):
    question_id: int
    db_id: str
    correct: bool
    reason: str


class DatabaseListResponse(BaseModel):
    databases: list[str]


class ErrorResponse(BaseModel):
    detail: str
