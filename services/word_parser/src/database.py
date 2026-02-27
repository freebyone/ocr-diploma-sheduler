from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Generator
import logging

# Меняем на абсолютные импорты
from app.config import get_settings
from app.models import Base

settings = get_settings()
logger = logging.getLogger(__name__)

# Создаем движок базы данных
engine = create_engine(settings.DATABASE_URL)

# Создаем фабрику сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Инициализация базы данных: создание таблиц"""
    try:
        # Проверяем подключение
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to database")
        
        # Создаем таблицы
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/verified")
        
    except SQLAlchemyError as e:
        logger.error(f"Database initialization error: {e}")
        raise


def get_db() -> Generator[Session, None, None]:
    """Dependency для получения сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()