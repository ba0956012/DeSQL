"""資料庫相關端點：描述、題目數量、提示。"""

from fastapi import APIRouter, HTTPException, Request

from eval_service.models import DatabaseListResponse, DescriptionResponse, HintsResponse, QuestionCountResponse

router = APIRouter()


def _get_datastore(request: Request):
    return request.app.state.datastore


def _check_db(datastore, db_id: str):
    if not datastore.db_exists(db_id):
        raise HTTPException(status_code=404, detail=f"Database '{db_id}' not found")


@router.get("/databases", response_model=DatabaseListResponse)
def list_databases(request: Request):
    ds = _get_datastore(request)
    return DatabaseListResponse(databases=ds.get_db_ids())


@router.get("/databases/{db_id}/description", response_model=DescriptionResponse)
def get_description(db_id: str, request: Request):
    ds = _get_datastore(request)
    _check_db(ds, db_id)
    desc = ds.get_db_description(db_id)
    return DescriptionResponse(db_id=db_id, **desc)


@router.get("/databases/{db_id}/count", response_model=QuestionCountResponse)
def get_count(db_id: str, request: Request):
    ds = _get_datastore(request)
    _check_db(ds, db_id)
    counts = ds.get_db_question_count(db_id)
    return QuestionCountResponse(db_id=db_id, **counts)


@router.get("/databases/{db_id}/hints", response_model=HintsResponse)
def get_hints(db_id: str, request: Request):
    ds = _get_datastore(request)
    _check_db(ds, db_id)
    hints = ds.get_db_hints(db_id)
    return HintsResponse(db_id=db_id, total=len(hints), hints=hints)
