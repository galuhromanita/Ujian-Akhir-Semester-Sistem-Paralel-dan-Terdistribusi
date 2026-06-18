"""
main.py - FastAPI Application - Pub-Sub Log Aggregator

Endpoint:
  POST /publish      : Terima single/batch event, push ke Redis queue
  GET  /events       : Daftar event unik yang telah diproses
  GET  /stats        : Statistik agregat (received, unique, duplicate, uptime)
  GET  /health       : Health check (DB + Redis + workers)

Startup/Shutdown:
  - Inisialisasi DB pool dan skema
  - Start N consumer worker tasks
  - Graceful shutdown: drain workers, tutup pool
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional, Union

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import settings
from consumer import get_active_worker_count, start_workers, stop_workers
from database import check_db_health, close_pool, get_pool, init_pool, init_schema
from models import (
    Event,
    EventResponse,
    HealthResponse,
    PublishResponse,
    StatsResponse,
)

# ============================================================
# Setup Logging Terstruktur
# ============================================================
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================
# State Global Aplikasi
# ============================================================
_startup_time: float = 0.0
_redis_client: Optional[aioredis.Redis] = None
_worker_tasks: List[asyncio.Task] = []


# ============================================================
# Lifespan: Startup & Shutdown
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manajemen lifecycle aplikasi:
    - Startup: inisialisasi DB pool, skema, Redis, workers
    - Shutdown: hentikan workers, tutup semua koneksi
    """
    global _startup_time, _redis_client, _worker_tasks

    # ---- STARTUP ----
    logger.info("=" * 60)
    logger.info(f"Memulai {settings.app_name} ...")
    logger.info("=" * 60)

    # Catat waktu startup
    _startup_time = time.time()

    # Inisialisasi koneksi database
    await init_pool()
    await init_schema()

    # Inisialisasi koneksi Redis
    _redis_client = aioredis.from_url(
        settings.broker_url,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )
    logger.info(f"Terhubung ke Redis: {settings.broker_url}")

    # Mulai consumer workers
    _worker_tasks = await start_workers(_redis_client, settings.worker_count)

    logger.info(f"{settings.app_name} siap melayani permintaan.")
    logger.info(f"Workers aktif: {get_active_worker_count()}")

    yield  # Aplikasi berjalan

    # ---- SHUTDOWN ----
    logger.info("Memulai proses shutdown graceful...")
    await stop_workers(_worker_tasks)

    if _redis_client:
        await _redis_client.aclose()
        logger.info("Koneksi Redis ditutup.")

    await close_pool()
    logger.info(f"{settings.app_name} berhasil dihentikan.")


# ============================================================
# Inisialisasi FastAPI
# ============================================================
app = FastAPI(
    title="UAS Pub-Sub Log Aggregator",
    description=(
        "Sistem Pub-Sub Log Aggregator Terdistribusi dengan "
        "Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS untuk akses demo lokal
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Helper
# ============================================================

def get_redis() -> aioredis.Redis:
    """Kembalikan Redis client yang sudah diinisialisasi."""
    if _redis_client is None:
        raise HTTPException(status_code=503, detail="Redis tidak tersedia")
    return _redis_client


def get_uptime() -> float:
    """Hitung uptime aplikasi dalam detik."""
    return time.time() - _startup_time if _startup_time > 0 else 0.0


# ============================================================
# Endpoint: POST /publish
# ============================================================
@app.post(
    "/publish",
    response_model=PublishResponse,
    summary="Publikasikan event (single atau batch)",
    tags=["Events"],
)
async def publish_events(
    body: Union[Event, List[Event]],
):
    """
    Terima satu event atau batch event, validasi skema, lalu push ke Redis queue.

    - Single event: `{ "topic": "...", "event_id": "...", ... }`
    - Batch event: `[ { ... }, { ... }, ... ]`

    Event di-push ke Redis dan akan diproses oleh consumer workers secara async.
    Deduplication terjadi saat consumer memproses (bukan di sini).
    """
    redis = get_redis()

    # Normalisasi input: single event → list
    if isinstance(body, Event):
        events = [body]
    else:
        events = body

    if not events:
        raise HTTPException(status_code=400, detail="Daftar event kosong.")

    # Periksa panjang antrian Redis (circuit breaker sederhana)
    queue_length = await redis.llen(settings.redis_queue_key)
    if queue_length >= settings.redis_max_queue_length:
        raise HTTPException(
            status_code=503,
            detail=f"Antrian penuh ({queue_length} items). Coba lagi nanti."
        )

    # Push semua event ke Redis queue menggunakan pipeline (efisien)
    queued_count = 0
    async with redis.pipeline(transaction=False) as pipe:
        for event in events:
            event_dict = event.model_dump()
            pipe.lpush(settings.redis_queue_key, json.dumps(event_dict, default=str))
            queued_count += 1
        await pipe.execute()

    logger.info(f"Diterima dan di-queue {queued_count} event(s).")

    return PublishResponse(
        status="queued",
        received=len(events),
        queued=queued_count,
        message=f"{queued_count} event berhasil diterima dan masuk antrian pemrosesan."
    )


# ============================================================
# Endpoint: GET /events
# ============================================================
@app.get(
    "/events",
    response_model=List[EventResponse],
    summary="Dapatkan daftar event unik yang telah diproses",
    tags=["Events"],
)
async def get_events(
    topic: Optional[str] = Query(None, description="Filter berdasarkan topic"),
    limit: int = Query(100, ge=1, le=1000, description="Jumlah maksimum event"),
    offset: int = Query(0, ge=0, description="Offset untuk pagination"),
):
    """
    Kembalikan daftar event yang sudah diproses dan tersimpan di database.
    Setiap event dijamin unik berdasarkan (topic, event_id).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                """
                SELECT id, topic, event_id, source,
                       to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                       payload,
                       to_char(received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
                FROM processed_events
                WHERE topic = $1
                ORDER BY received_at DESC
                LIMIT $2 OFFSET $3
                """,
                topic, limit, offset
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, topic, event_id, source,
                       to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                       payload,
                       to_char(received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
                FROM processed_events
                ORDER BY received_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit, offset
            )

    def _parse_payload(raw) -> dict:
        """asyncpg dapat mengembalikan JSONB sebagai str atau dict."""
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        # Fallback: coba konversi
        return json.loads(str(raw))

    return [
        EventResponse(
            id=row["id"],
            topic=row["topic"],
            event_id=row["event_id"],
            source=row["source"],
            timestamp=row["timestamp"],
            payload=_parse_payload(row["payload"]),
            received_at=row["received_at"],
        )
        for row in rows
    ]


# ============================================================
# Endpoint: GET /stats
# ============================================================
@app.get(
    "/stats",
    response_model=StatsResponse,
    summary="Statistik agregat sistem",
    tags=["Monitoring"],
)
async def get_stats():
    """
    Kembalikan statistik keseluruhan sistem:
    - received: total event yang diterima (termasuk duplikat)
    - unique_processed: event unik yang berhasil diproses
    - duplicate_dropped: event duplikat yang diabaikan
    - topics: breakdown per topik
    - uptime_seconds: waktu sejak startup
    - queue_length: jumlah event di antrian Redis
    - worker_count: jumlah consumer workers aktif
    """
    pool = get_pool()
    redis = get_redis()

    async with pool.acquire() as conn:
        # Agregasi total dari semua topik
        totals = await conn.fetchrow(
            """
            SELECT
                COALESCE(SUM(received), 0)          AS total_received,
                COALESCE(SUM(unique_processed), 0)  AS total_unique,
                COALESCE(SUM(duplicate_dropped), 0) AS total_duplicate
            FROM event_stats
            """
        )

        # Detail per topik
        topic_rows = await conn.fetch(
            """
            SELECT topic, received, unique_processed, duplicate_dropped,
                   to_char(last_updated AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_updated
            FROM event_stats
            ORDER BY received DESC
            """
        )

    # Panjang antrian Redis saat ini
    queue_length = await redis.llen(settings.redis_queue_key)

    return StatsResponse(
        received=totals["total_received"],
        unique_processed=totals["total_unique"],
        duplicate_dropped=totals["total_duplicate"],
        topics=[
            {
                "topic": row["topic"],
                "received": row["received"],
                "unique_processed": row["unique_processed"],
                "duplicate_dropped": row["duplicate_dropped"],
                "last_updated": row["last_updated"],
            }
            for row in topic_rows
        ],
        uptime_seconds=round(get_uptime(), 2),
        queue_length=queue_length,
        worker_count=get_active_worker_count(),
        service=settings.app_name,
    )


# ============================================================
# Endpoint: GET /health
# ============================================================
@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check (readiness + liveness)",
    tags=["Monitoring"],
)
async def health_check():
    """
    Periksa status kesehatan semua komponen:
    - database: koneksi PostgreSQL
    - broker: koneksi Redis
    - workers: jumlah consumer workers aktif
    """
    db_ok = await check_db_health()

    # Periksa Redis
    redis_ok = False
    try:
        redis = get_redis()
        pong = await redis.ping()
        redis_ok = (pong is True or pong == "PONG")
    except Exception as e:
        logger.warning(f"Redis health check gagal: {e}")

    status = "healthy" if (db_ok and redis_ok) else "degraded"

    if not db_ok or not redis_ok:
        return JSONResponse(
            status_code=503,
            content=HealthResponse(
                status=status,
                database="ok" if db_ok else "error",
                broker="ok" if redis_ok else "error",
                workers_running=get_active_worker_count(),
                uptime_seconds=round(get_uptime(), 2),
            ).model_dump()
        )

    return HealthResponse(
        status=status,
        database="ok",
        broker="ok",
        workers_running=get_active_worker_count(),
        uptime_seconds=round(get_uptime(), 2),
    )


# ============================================================
# Root
# ============================================================
@app.get("/", tags=["Root"])
async def root():
    """Informasi dasar service."""
    return {
        "service": settings.app_name,
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "endpoints": ["/publish", "/events", "/stats", "/health"],
    }
