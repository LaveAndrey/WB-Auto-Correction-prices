# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем системные зависимости для MySQL
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    gcc \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY . .

# Создаем директорию для логов
RUN mkdir -p /app/logs

# Указываем команду по умолчанию - запускаем оба скрипта параллельно
CMD ["sh", "-c", "python insert_price_wb.py & python price_updater_main.py & wait"]