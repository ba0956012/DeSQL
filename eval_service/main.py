"""FastAPI 應用程式入口：初始化 DataStore 和 LLMJudge，註冊所有 router。"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from eval_service.config import get_settings
from eval_service.datastore import DataStore
from eval_service.judge import LLMJudge
from eval_service.models import HealthResponse
from eval_service.routers import databases, eval, questions

__version__ = "0.1.0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.datastore = DataStore(settings.data_dir)
    app.state.judge = LLMJudge(settings)
    yield


app = FastAPI(title="BIRD-SQL Eval Service", lifespan=lifespan)

app.include_router(databases.router)
app.include_router(questions.router)
app.include_router(eval.router)


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(version=__version__)
