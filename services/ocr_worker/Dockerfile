FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements_simple.txt

COPY ./src/*.py .

RUN mkdir -p /app/results

CMD ["python", "ocr_worker.py"]