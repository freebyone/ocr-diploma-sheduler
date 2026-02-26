from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from config import DATABASE_URL
from models import Base
import logging
import time

logger = logging.getLogger(__name__)

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def wait_for_db(max_retries: int = 30, delay: int = 5):
    """Ждёт пока PostgreSQL станет доступен"""
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.warning(
                f"Database not ready (attempt {attempt}/{max_retries}): {e}"
            )
            if attempt < max_retries:
                time.sleep(delay)

    logger.error("Failed to connect to database after all retries")
    return False


def init_db():
    """Создаёт таблицы если их нет (на случай если init.sql не отработал)"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified/created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise


def get_session() -> Session:
    """Возвращает новую сессию БД"""
    return SessionLocal()