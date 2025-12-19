# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем системные зависимости для MySQL и сборки
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY . .

# Создаем директории для логов
RUN mkdir -p /app/logs

# Запускаем оба скрипта
CMD ["sh", "-c", "python3 main.py & python3 insert_price_wb.py & wait"]