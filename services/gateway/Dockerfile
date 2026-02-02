# Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Установка зависимостей
COPY req.txt .
RUN pip install --no-cache-dir -r req.txt

# Копирование кода
COPY . .

# Копирование .proto файлов (если есть)
COPY protos/ ./protos/

# Генерация Python кода из .proto (если нужно)
# RUN python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/*.proto

EXPOSE 8000 9090

CMD ["python", "-m", "app.main"]