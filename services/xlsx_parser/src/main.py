import sys
import time
import signal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import settings
from models import Base
from service import process_all_files_from_minio

# Флаг для корректной остановки
running = True


def handle_signal(signum, frame):
    global running
    print(f"\n🛑 Получен сигнал {signum}, завершаем...")
    running = False


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


def run_minio_watcher(session_factory, poll_interval: int = 30):
    """
    Бесконечный цикл: каждые poll_interval секунд
    проверяет MinIO бакет и обрабатывает новые файлы.
    """
    global running

    print(f"\n{'=' * 60}")
    print(f"👁️  Мониторинг MinIO бакета '{settings.MINIO_BUCKET}'")
    print(f"   Интервал опроса: {poll_interval} сек")
    print(f"   Для остановки: Ctrl+C")
    print(f"{'=' * 60}\n")

    cycle = 0
    while running:
        cycle += 1
        try:
            with session_factory() as session:
                process_all_files_from_minio(session)
        except KeyboardInterrupt:
            print("\n🛑 Остановлено пользователем")
            break
        except Exception as e:
            print(f"❌ Ошибка в цикле #{cycle}: {e}")

        for _ in range(poll_interval):
            if not running:
                break
            time.sleep(1)

    print("👋 Мониторинг MinIO завершён")


def main():
    global running

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

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
    print("✅ Таблицы созданы/проверены")

    SessionLocal = sessionmaker(bind=engine)

    # ── Мониторинг MinIO (бесконечный цикл) ──
    run_minio_watcher(SessionLocal, settings.POLL_INTERVAL)


if __name__ == "__main__":
    main()