"""
config.py - Konfigurasi Aggregator dari Environment Variables
Menggunakan pydantic-settings untuk validasi tipe otomatis.
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Pengaturan aplikasi yang dibaca dari environment variables.
    Nilai default digunakan saat berjalan di luar Docker (development lokal).
    """

    # Koneksi database PostgreSQL
    database_url: str = "postgresql://loguser:logpassword@localhost:5432/logdb"

    # Koneksi broker Redis
    broker_url: str = "redis://localhost:6379"

    # Jumlah worker consumer yang berjalan paralel
    worker_count: int = 3

    # Level log aplikasi
    log_level: str = "INFO"

    # Nama antrian di Redis
    redis_queue_key: str = "events:queue"

    # Timeout BRPOP Redis (detik); 0 = blokir selamanya
    redis_brpop_timeout: int = 2

    # Maksimum panjang antrian Redis (pencegahan overflow memori)
    redis_max_queue_length: int = 100_000

    # Nama aplikasi (untuk logging)
    app_name: str = "UAS-Aggregator"

    # Lingkungan aplikasi
    app_env: str = "development"

    class Config:
        env_file = ".env"
        case_sensitive = False


# Instance singleton untuk digunakan di seluruh aplikasi
settings = Settings()
