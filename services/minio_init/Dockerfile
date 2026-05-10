FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir minio

COPY minio_init.py .

RUN chmod +x minio_init.py

ENTRYPOINT ["python", "-u", "minio_init.py"]