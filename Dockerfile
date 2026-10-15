# Image for the Python parts: producer (CSV -> Kafka), consumer (Kafka -> HDFS) and dashboard.
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ .
