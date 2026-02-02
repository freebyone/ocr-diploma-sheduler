# app/config.py
import os
from typing import List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # WebSocket сервер
    ws_host: str = "0.0.0.0"
    ws_port: int = 8000
    ws_max_connections: int = 10000
    
    # gRPC микросервисы
    microservices: List[str] = [
        "ocr-service:50051"
    ]
    
    # Балансировка
    load_balancer: str = "round-robin"  # или "least-connections"
    
    # Настройки повторных попыток
    max_retries: int = 3
    retry_delay: float = 0.1
    
    # Circuit breaker
    circuit_breaker_failures: int = 5
    circuit_breaker_timeout: int = 30
    
    # Метрики
    metrics_port: int = 9090
    metrics_enabled: bool = True
    
    # Логирование
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"

settings = Settings()