"""評測端點：單題測試評判。"""

from fastapi import APIRouter, HTTPException, Request

from eval_service.judge import run_gold_sql
from eval_service.models import EvaluateRequest, EvaluateResponse

router = APIRouter()


@router.post(
    "/databases/{db_id}/questions/{question_id}/evaluate",
    response_model=EvaluateResponse,
)
def evaluate(db_id: str, question_id: int, body: EvaluateRequest, request: Request):
    ds = request.app.state.datastore
    judge = request.app.state.judge

    if not ds.db_exists(db_id):
        raise HTTPException(status_code=404, detail=f"Database '{db_id}' not found")

    q = ds.get_question(db_id, question_id)
    if q is None:
        raise HTTPException(
            status_code=404,
            detail=f"Question {question_id} not found in database '{db_id}'",
        )

    # Execute Gold SQL
    sqlite_path = ds.get_sqlite_path(db_id)
    try:
        expected = run_gold_sql(sqlite_path, q["SQL"])
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gold SQL execution failed: {e}",
        )

    # LLM Judge
    result = judge.judge(q["question"], expected, body.answer)

    return EvaluateResponse(
        question_id=question_id,
        db_id=db_id,
        correct=result.get("correct", False),
        reason=result.get("reason", ""),
    )
