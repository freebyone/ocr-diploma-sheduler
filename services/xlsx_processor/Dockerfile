FROM python:3.11-slim

WORKDIR /app

# Зависимости для psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq-dev gcc curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src .

# По умолчанию — обработка из MinIO
CMD ["python", "main.py", "--source", "minio"]