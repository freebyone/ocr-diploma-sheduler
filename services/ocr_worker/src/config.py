import os

class Config:
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456")
    MINIO_SECURE = False
    
    BUCKET_NAME = os.getenv("BUCKET_NAME", "documents")
    RESULTS_BUCKET = os.getenv("RESULTS_BUCKET", "results")
    
    OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
    OCR_MODEL = os.getenv("OCR_MODEL", "deepseek-ocr")
    
    OLLAMA_TIMEOUT = 600

config = Config()