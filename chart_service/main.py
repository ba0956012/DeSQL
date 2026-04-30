"""FastAPI application entry point."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from chart_service import __version__
from chart_service.config import get_settings
from chart_service.chart_generator import ChartGenerator
from chart_service.models import HealthResponse
from chart_service.routers import chart


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.chart_generator = ChartGenerator(settings)
    yield


app = FastAPI(title="Chart Generation Service", lifespan=lifespan)
app.include_router(chart.router)


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(version=__version__)
