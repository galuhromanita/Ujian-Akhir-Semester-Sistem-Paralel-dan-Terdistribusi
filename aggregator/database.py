"""
database.py - Manajemen Koneksi & Skema Database PostgreSQL
Menggunakan asyncpg untuk koneksi async non-blocking.

Fitur:
- Connection pool untuk efisiensi
- Inisialisasi skema otomatis saat startup
- UNIQUE constraint (topic, event_id) untuk deduplication atomik
- Tabel statistik transaksional
- Audit log untuk observabilitas
"""

import asyncpg
import logging
from config import settings

logger = logging.getLogger(__name__)

# Pool koneksi global - dibuat saat startup dan ditutup saat shutdown
_pool: asyncpg.Pool | None = None


# ============================================================
# DDL - Definisi Skema Database
# ============================================================

SCHEMA_SQL = """
-- --------------------------------------------------------
-- Tabel utama: menyimpan event yang telah diproses
-- UNIQUE(topic, event_id) adalah kunci utama deduplication
-- Constraint ini memastikan atomisitas di level database,
-- sehingga dua worker paralel tidak bisa insert event yang sama dua kali.
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_events (
    id          BIGSERIAL PRIMARY KEY,
    topic       VARCHAR(255) NOT NULL,
    event_id    VARCHAR(255) NOT NULL,
    source      VARCHAR(255) NOT NULL DEFAULT '',
    timestamp   TIMESTAMPTZ NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}',
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraint unik: jantung dari mekanisme idempotency
    CONSTRAINT uq_topic_event UNIQUE (topic, event_id)
);

-- Indeks untuk query GET /events?topic=...
CREATE INDEX IF NOT EXISTS idx_processed_events_topic
    ON processed_events (topic);

-- Indeks untuk query berdasarkan waktu
CREATE INDEX IF NOT EXISTS idx_processed_events_received_at
    ON processed_events (received_at DESC);

-- --------------------------------------------------------
-- Tabel statistik per topik - diupdate transaksional
-- Menggunakan UPDATE ... SET count = count + 1 untuk mencegah lost-update
-- saat banyak worker paralel mengupdate baris yang sama.
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_stats (
    topic               VARCHAR(255) PRIMARY KEY,
    received            BIGINT NOT NULL DEFAULT 0,
    unique_processed    BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped   BIGINT NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- --------------------------------------------------------
-- Audit log: rekam setiap keputusan pemrosesan
-- Berguna untuk debugging dan observabilitas
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    event_id    VARCHAR(255) NOT NULL,
    topic       VARCHAR(255) NOT NULL,
    action      VARCHAR(50) NOT NULL CHECK (action IN ('processed', 'duplicate_dropped')),
    worker_id   VARCHAR(50) NOT NULL,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indeks audit log untuk query berdasarkan event_id
CREATE INDEX IF NOT EXISTS idx_audit_log_event_id
    ON audit_log (event_id);
"""


# ============================================================
# Manajemen Pool Koneksi
# ============================================================

async def init_pool() -> asyncpg.Pool:
    """
    Inisialisasi connection pool PostgreSQL.
    Dipanggil sekali saat aplikasi startup.
    """
    global _pool
    logger.info("Menginisialisasi connection pool database...")

    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,          # Minimal 2 koneksi selalu tersedia
        max_size=20,         # Maksimal 20 koneksi (cukup untuk 3 worker + API)
        command_timeout=30,  # Timeout per query
        max_inactive_connection_lifetime=300,  # Tutup koneksi idle > 5 menit
    )

    logger.info("Connection pool berhasil dibuat.")
    return _pool


async def close_pool() -> None:
    """
    Tutup semua koneksi di pool saat aplikasi shutdown.
    Memastikan graceful shutdown tanpa koneksi menggantung.
    """
    global _pool
    if _pool:
        await _pool.close()
        logger.info("Connection pool database ditutup.")
        _pool = None


def get_pool() -> asyncpg.Pool:
    """
    Ambil pool koneksi yang sudah diinisialisasi.
    Raise error jika pool belum diinisialisasi.
    """
    if _pool is None:
        raise RuntimeError("Database pool belum diinisialisasi. Panggil init_pool() terlebih dahulu.")
    return _pool


# ============================================================
# Inisialisasi Skema
# ============================================================

async def init_schema() -> None:
    """
    Buat tabel-tabel yang diperlukan jika belum ada.
    Menggunakan IF NOT EXISTS sehingga aman dipanggil berkali-kali (idempotent).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    logger.info("Skema database berhasil diinisialisasi.")


# ============================================================
# Health Check
# ============================================================

async def check_db_health() -> bool:
    """
    Periksa apakah koneksi database masih berfungsi.
    Digunakan oleh endpoint GET /health.
    """
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database health check gagal: {e}")
        return False
