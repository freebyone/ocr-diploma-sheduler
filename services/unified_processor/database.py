import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from models import Base

logger = logging.getLogger(__name__)

DATABASE_URL = (
    f"postgresql://"
    f"{os.getenv('DB_USER', 'counter_user')}:"
    f"{os.getenv('DB_PASSWORD', 'norma_password')}@"
    f"{os.getenv('DB_HOST', 'postgres')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'counter_db')}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # проверяет соединение перед использованием
    pool_size=5,
    max_overflow=10,
    echo=False,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)


def init_db() -> None:
    """Создаёт все таблицы если они не существуют."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created / verified successfully.")
    except OperationalError as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


def get_db() -> Session:
    """Dependency для FastAPI — возвращает сессию и закрывает её после запроса."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection() -> bool:
    """Проверка доступности БД (используется в /health)."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"DB connection check failed: {e}")
        return False