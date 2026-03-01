FROM python:3.11-slim

WORKDIR /app

# Сначала копируем только requirements.txt
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь исходный код
COPY ./src ./src

# Устанавливаем PYTHONPATH
ENV PYTHONPATH=/app

# Запускаем приложение
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]