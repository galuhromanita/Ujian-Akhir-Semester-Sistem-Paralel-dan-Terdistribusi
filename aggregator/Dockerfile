# ============================================================
# Dockerfile - Aggregator Service
# Base image: python:3.11-slim
# Non-root user untuk keamanan
# ============================================================

FROM python:3.11-slim AS base

# Metadata image
LABEL maintainer="UAS Sistem Terdistribusi"
LABEL description="Pub-Sub Log Aggregator - FastAPI Service"

# Variabel lingkungan Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Buat direktori kerja
WORKDIR /app

# Install dependensi sistem minimal
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Buat user non-root untuk keamanan
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --no-create-home appuser

# Copy dan install dependensi Python terlebih dahulu (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code aplikasi
COPY . .

# Ubah kepemilikan file ke user non-root
RUN chown -R appuser:appgroup /app

# Gunakan user non-root
USER appuser

# Expose port aplikasi
EXPOSE 8080

# Perintah menjalankan aplikasi
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
