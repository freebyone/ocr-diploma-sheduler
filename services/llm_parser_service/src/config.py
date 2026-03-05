import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes")
RESULTS_BUCKET = os.getenv("RESULTS_BUCKET", "results")
ERRORS_BUCKET = os.getenv("ERRORS_BUCKET", "errors")

GIGACHAT_MODEL = os.getenv("GIGACHAT_MODEL", "GigaChat-2")
GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_CREDENTIALS", "")

if not GIGACHAT_CREDENTIALS:
    import logging
    logging.getLogger(__name__).warning(
        "⚠️ GIGACHAT_CREDENTIALS не задан! "
        "Установите его в .env файле или переменных окружения."
    )

MODEL_KEY = GIGACHAT_CREDENTIALS

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ocr_user:ocr_password@postgres:5432/ocr_db"
)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))