"""
Точка входа.

Режимы работы:
  1. --source minio   → скачать xlsx из MinIO бакета и обработать
  2. --source local   → обработать файлы из локальной папки (--dir)
  3. без аргументов   → сначала MinIO, потом локальная папка (если указана)

Переменные окружения (или .env):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
  MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
"""

import argparse
import sys
import time

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import settings
from models import Base
from service import (
    process_all_files,
    process_all_files_from_minio,
)


def wait_for_db(engine, retries: int = 30, delay: float = 2.0):
    """Ожидание доступности PostgreSQL"""
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL доступен")
            return
        except Exception as e:
            print(
                f"⏳ Ожидание PostgreSQL "
                f"({attempt}/{retries})... {e}"
            )
            time.sleep(delay)
    print("❌ PostgreSQL недоступен, выход")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Обработка xlsx-файлов → PostgreSQL"
    )
    parser.add_argument(
        "--source",
        choices=["minio", "local", "both"],
        default="minio",
        help="Источник файлов (default: minio)",
    )
    parser.add_argument(
        "--dir",
        default="./inp/",
        help="Локальная папка с xlsx (для --source local/both)",
    )
    args = parser.parse_args()

    # ── Подключение к PostgreSQL ──
    print(f"🔌 Подключение к БД: {settings.database_url}")
    engine = create_engine(
        settings.database_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )

    wait_for_db(engine)

    # ── Создание таблиц ──
    Base.metadata.create_all(engine)
    print("✅ Таблицы созданы/проверены\n")

    SessionLocal = sessionmaker(bind=engine)

    # ── Обработка ──
    with SessionLocal() as session:
        if args.source in ("minio", "both"):
            print("\n🗂️  Обработка файлов из MinIO...")
            process_all_files_from_minio(session)

        if args.source in ("local", "both"):
            print(f"\n📁 Обработка файлов из папки '{args.dir}'...")
            process_all_files(args.dir, session)


if __name__ == "__main__":
    main()