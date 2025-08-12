# Используем официальный образ Python в качестве базового
FROM python:3.10-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Скопируем файл с зависимостями и установим их
COPY app/requirements.txt .

# Установим зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Скопируем файлы приложения в контейнер
COPY app/ .

# Запускаем основной файл приложения
ENTRYPOINT ["python", "main.py"]
CMD []