# main.py — исправленная версия (добавлен /health на корневом уровне)

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func

from database import engine, SessionLocal
from models import Base
from router import router
from config import settings
from schemas import HealthResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Запуск word-service...")
    logger.info(
        f"📦 PostgreSQL: {settings.POSTGRES_HOST}:"
        f"{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
    )
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Таблицы проверены/созданы")
    yield
    logger.info("🛑 Остановка word-service")


app = FastAPI(
    title="Word Order Generator Service",
    description="Сервис генерации приказов о переаттестации в формате Word",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API-роутер (все маршруты доступны по /api/...)
app.include_router(router, prefix="/api")


@app.get("/")
def root():
    return {
        "service": "word-order-generator",
        "version": "1.0.0",
        "docs": "/docs",
    }


# ── /health на корневом уровне (для Docker / k8s healthcheck) ──────────
@app.get("/health", response_model=HealthResponse, tags=["system"])
def health_check_root():
    """
    Healthcheck без обращения к БД — для быстрой проверки контейнера.
    Полный healthcheck с проверкой БД: GET /api/health
    """
    return HealthResponse(status="ok", database="unknown", version="1.0.0")