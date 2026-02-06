import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # MinIO
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456")
    MINIO_SECURE = os.getenv("MINIO_SECRET_KEY", "false").lower() == "true"
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "pdf-images")
    MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
    
    # Ollama
    OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "deepseek-ocr")
    OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "600"))
    
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "ocr_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "12345678")
    
    # Processing
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
    PROCESSING_INTERVAL = int(os.getenv("PROCESSING_INTERVAL", "10"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Database URL
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

config = Config()