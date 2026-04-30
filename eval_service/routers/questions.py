"""題目相關端點：取得單題內容。"""

from fastapi import APIRouter, HTTPException, Request

from eval_service.models import QuestionResponse

router = APIRouter()


@router.get(
    "/databases/{db_id}/questions/{question_id}",
    response_model=QuestionResponse,
)
def get_question(db_id: str, question_id: int, request: Request):
    ds = request.app.state.datastore
    if not ds.db_exists(db_id):
        raise HTTPException(status_code=404, detail=f"Database '{db_id}' not found")

    q = ds.get_question(db_id, question_id)
    if q is None:
        raise HTTPException(
            status_code=404,
            detail=f"Question {question_id} not found in database '{db_id}'",
        )

    return QuestionResponse(
        question_id=q["question_id"],
        db_id=db_id,
        question=q["question"],
        evidence=q.get("evidence", ""),
        difficulty=q.get("difficulty", ""),
    )
