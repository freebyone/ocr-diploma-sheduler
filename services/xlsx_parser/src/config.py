import os


class Settings:
    # PostgreSQL
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "xlsx_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "xlsx_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "xlsx_password")

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # MinIO
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "admin123456")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "xlsx-documents")
    MINIO_RESULTS_BUCKET: str = os.getenv("MINIO_RESULTS_BUCKET", "xlsx-results")
    MINIO_ERRORS_BUCKET: str = os.getenv("MINIO_ERRORS_BUCKET", "xlsx-errors")

    # Интервал опроса MinIO (секунды)
    POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL", "30"))

    # Локальная папка для временных файлов
    TEMP_DIR: str = os.getenv("TEMP_DIR", "/tmp/xlsx_processing")


settings = Settings()