FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libjpeg62-turbo \
    libfreetype6 \
    libpng16-16 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/logs

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8888

CMD ["python", "-m", "app.main"]