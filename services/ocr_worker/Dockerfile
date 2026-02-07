FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости системы
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создаем структуру директорий
RUN mkdir -p /app/logs /app/results

# Копируем исходный код
COPY src/ /app/

# Переменные окружения по умолчанию
ENV MINIO_ENDPOINT=localhost:9000
ENV MINIO_ACCESS_KEY=ocrminio
ENV MINIO_SECRET_KEY=admin123456
ENV MINIO_BUCKET=document
ENV MINIO_SECURE=false
ENV OLLAMA_URL=http://localhost:11434
ENV OLLAMA_MODEL=deepseek-ocr
ENV PROMPT_TYPE=markdown
ENV PROCESS_LIMIT=2
ENV MOVE_PROCESSED=true
ENV PROCESS_INTERVAL=60
ENV PROCESS_MODE=once

# Команда запуска
CMD ["python", "main.py"]