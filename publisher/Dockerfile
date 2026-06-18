# ============================================================
# Dockerfile - Publisher Service
# Base image: python:3.11-slim
# Non-root user untuk keamanan
# ============================================================

FROM python:3.11-slim

LABEL maintainer="UAS Sistem Terdistribusi"
LABEL description="Pub-Sub Log Aggregator - Publisher/Simulator Service"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Buat user non-root
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --no-create-home appuser

# Install dependensi
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chown -R appuser:appgroup /app

USER appuser

CMD ["python", "main.py"]
