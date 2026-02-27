import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456")
MINIO_SECURE = False

MODEL_KEY = ""

RESULTS_BUCKET = "results"
ERRORS_BUCKET = "errors"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ocr_user:ocr_password@postgres:5432/ocr_db"
)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))