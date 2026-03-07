import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import engine
from models import Base
from router import router
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / Shutdown"""
    logger.info("🚀 Запуск word-service...")
    logger.info(f"📦 PostgreSQL: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")

    # Создаём таблицы, если их нет
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

# CORS — разрешаем фронту обращаться
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутер
app.include_router(router, prefix="/api")


# Корневой эндпоинт
@app.get("/")
def root():
    return {
        "service": "word-order-generator",
        "version": "1.0.0",
        "docs": "/docs",
    }