"""Chart generation API router."""

from fastapi import APIRouter, Request

from chart_service.models import ChartRequest, ChartResponse

router = APIRouter(prefix="/chart", tags=["chart"])


@router.post("/generate", response_model=ChartResponse)
def generate_chart(req: ChartRequest, request: Request) -> ChartResponse:
    generator = request.app.state.chart_generator
    return generator.generate(
        data=req.data,
        question=req.question,
        engine=req.engine,
        chart_type=req.chart_type,
    )
