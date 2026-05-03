FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

# Создаём пустые __init__.py, чтобы Python распознал пакеты
RUN touch /app/src/__init__.py && \
    touch /app/src/etl/__init__.py && \
    touch /app/src/ml/__init__.py && \
    mkdir -p /app/data /app/logs

ENV PYTHONPATH=/app

CMD ["python", "src/main.py"]