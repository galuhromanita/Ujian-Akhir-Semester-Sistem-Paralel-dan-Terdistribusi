"""
consumer.py - Consumer Worker untuk Memproses Event dari Redis Queue

Arsitektur:
- Beberapa worker berjalan secara concurrent sebagai asyncio tasks
- Setiap worker BRPOP dari Redis queue 'events:queue'
- Pemrosesan dilakukan dalam SATU TRANSAKSI PostgreSQL:
  1. INSERT ke processed_events dengan ON CONFLICT DO NOTHING (idempotent)
  2. UPDATE event_stats secara atomik (mencegah lost-update)
  3. INSERT ke audit_log untuk observabilitas

Jaminan Idempotency:
- UNIQUE(topic, event_id) constraint di Postgres mencegah duplikat di level DB
- INSERT ... ON CONFLICT DO NOTHING: atomik, aman untuk multi-worker paralel
- Dua worker yang proses event sama pada waktu bersamaan:
  Worker A: INSERT sukses → unique_processed + 1
  Worker B: INSERT konflik (DO NOTHING) → duplicate_dropped + 1
  Hasil: hanya 1 baris di database ✓

Isolation Level: READ COMMITTED (default Postgres)
- Cukup karena conflict resolution sudah di-handle unique constraint
- Tidak perlu SERIALIZABLE (overhead tinggi tidak sebanding manfaatnya)
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis
from asyncpg import Pool

from config import settings
from database import get_pool

logger = logging.getLogger(__name__)

# ============================================================
# Counter untuk monitoring worker yang aktif
# ============================================================
_active_workers: int = 0
_shutdown_event: asyncio.Event = asyncio.Event()


def get_active_worker_count() -> int:
    """Kembalikan jumlah consumer worker yang sedang aktif."""
    return _active_workers


def signal_shutdown() -> None:
    """Kirim sinyal untuk menghentikan semua worker secara graceful."""
    _shutdown_event.set()


# ============================================================
# Fungsi Pemrosesan Event (Inti Logika Idempotency)
# ============================================================

async def process_single_event(
    conn,
    event_data: dict,
    worker_id: str
) -> tuple[bool, str]:
    """
    Proses satu event dalam satu transaksi database.

    Mengembalikan:
    - (True, 'processed')          : event baru berhasil diproses
    - (False, 'duplicate_dropped') : event duplikat, diabaikan

    Strategi transaksi:
    1. Mulai transaksi dengan isolation READ COMMITTED
    2. INSERT ke processed_events dengan ON CONFLICT DO NOTHING
    3. Periksa apakah insert berhasil (baris diinsert atau tidak)
    4. Update event_stats sesuai hasil
    5. Catat ke audit_log
    6. COMMIT transaksi
    """
    topic = event_data.get("topic", "")
    event_id = event_data.get("event_id", "")
    source = event_data.get("source", "")
    timestamp_str = event_data.get("timestamp", "")
    payload = event_data.get("payload", {})

    # Parse timestamp dari string ISO8601
    try:
        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        timestamp = datetime.now(timezone.utc)
        logger.warning(f"[Worker {worker_id}] Timestamp tidak valid untuk event {event_id}, menggunakan waktu sekarang.")

    # Serialisasi payload ke JSON string untuk asyncpg
    payload_json = json.dumps(payload)

    # --------------------------------------------------------
    # TRANSAKSI ATOMIK - Inti idempotency
    # --------------------------------------------------------
    async with conn.transaction(isolation="read_committed"):

        # Langkah 1: Coba insert event ke tabel utama
        # ON CONFLICT DO NOTHING: jika (topic, event_id) sudah ada → abaikan
        result = await conn.execute(
            """
            INSERT INTO processed_events (topic, event_id, source, timestamp, payload)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT ON CONSTRAINT uq_topic_event DO NOTHING
            """,
            topic, event_id, source, timestamp, payload_json
        )

        # Periksa apakah insert berhasil: "INSERT 0 1" = berhasil, "INSERT 0 0" = konflik
        was_inserted = result == "INSERT 0 1"

        # Langkah 2: Update statistik secara atomik dalam transaksi yang sama
        # Menggunakan INSERT ... ON CONFLICT DO UPDATE untuk upsert atomik
        # UPDATE SET count = count + 1 mencegah lost-update saat multi-worker
        if was_inserted:
            await conn.execute(
                """
                INSERT INTO event_stats (topic, received, unique_processed, duplicate_dropped, last_updated)
                VALUES ($1, 1, 1, 0, NOW())
                ON CONFLICT (topic) DO UPDATE SET
                    received          = event_stats.received + 1,
                    unique_processed  = event_stats.unique_processed + 1,
                    last_updated      = NOW()
                """,
                topic
            )
            action = "processed"
        else:
            await conn.execute(
                """
                INSERT INTO event_stats (topic, received, unique_processed, duplicate_dropped, last_updated)
                VALUES ($1, 1, 0, 1, NOW())
                ON CONFLICT (topic) DO UPDATE SET
                    received          = event_stats.received + 1,
                    duplicate_dropped = event_stats.duplicate_dropped + 1,
                    last_updated      = NOW()
                """,
                topic
            )
            action = "duplicate_dropped"

        # Langkah 3: Catat ke audit_log untuk observabilitas
        await conn.execute(
            """
            INSERT INTO audit_log (event_id, topic, action, worker_id, logged_at)
            VALUES ($1, $2, $3, $4, NOW())
            """,
            event_id, topic, action, worker_id
        )
    # --------------------------------------------------------
    # COMMIT otomatis di akhir blok 'async with conn.transaction()'
    # --------------------------------------------------------

    return was_inserted, action


# ============================================================
# Worker Loop Utama
# ============================================================

async def consumer_worker(worker_id: str, redis_client: aioredis.Redis) -> None:
    """
    Worker loop yang terus-menerus mengambil dan memproses event dari Redis.

    - BRPOP memblokir selama {REDIS_BRPOP_TIMEOUT} detik, lalu cek shutdown signal
    - Setiap event diproses dalam transaksi terpisah
    - Error handling: event yang gagal di-push kembali ke antrian (retry)
    """
    global _active_workers
    _active_workers += 1

    pool: Pool = get_pool()
    logger.info(f"[Worker {worker_id}] Mulai berjalan. Mendengarkan antrian '{settings.redis_queue_key}'...")

    processed_count = 0
    duplicate_count = 0
    error_count = 0

    try:
        while not _shutdown_event.is_set():
            try:
                # BRPOP: Ambil item dari antrian Redis dengan timeout
                # Jika antrian kosong, blokir selama REDIS_BRPOP_TIMEOUT detik
                item = await redis_client.brpop(
                    settings.redis_queue_key,
                    timeout=settings.redis_brpop_timeout
                )

                if item is None:
                    # Timeout tercapai, tidak ada item - lanjut loop (cek shutdown)
                    continue

                # item adalah tuple (key, value) dari Redis
                _, raw_data = item

                # Deserialisasi JSON dari Redis
                try:
                    event_data = json.loads(raw_data)
                except json.JSONDecodeError as e:
                    logger.error(f"[Worker {worker_id}] Gagal parse JSON: {e}. Data: {raw_data[:100]}")
                    error_count += 1
                    continue

                # Ambil koneksi dari pool dan proses event
                async with pool.acquire() as conn:
                    was_inserted, action = await process_single_event(conn, event_data, worker_id)

                if was_inserted:
                    processed_count += 1
                    logger.debug(
                        f"[Worker {worker_id}] DIPROSES event_id={event_data.get('event_id')} "
                        f"topic={event_data.get('topic')} | total_diproses={processed_count}"
                    )
                else:
                    duplicate_count += 1
                    logger.info(
                        f"[Worker {worker_id}] DUPLIKAT DIABAIKAN event_id={event_data.get('event_id')} "
                        f"topic={event_data.get('topic')} | total_duplikat={duplicate_count}"
                    )

            except asyncio.CancelledError:
                # Task dibatalkan (shutdown)
                logger.info(f"[Worker {worker_id}] Mendapat sinyal CancelledError, berhenti...")
                break

            except Exception as e:
                error_count += 1
                logger.error(
                    f"[Worker {worker_id}] Error saat memproses event: {e}",
                    exc_info=True
                )
                # Tunggu sebentar sebelum retry untuk mencegah busy loop
                await asyncio.sleep(0.5)

    finally:
        _active_workers -= 1
        logger.info(
            f"[Worker {worker_id}] Berhenti. "
            f"Statistik: processed={processed_count}, "
            f"duplicate={duplicate_count}, error={error_count}"
        )


# ============================================================
# Manajemen Worker Tasks
# ============================================================

async def start_workers(redis_client: aioredis.Redis, count: Optional[int] = None) -> list[asyncio.Task]:
    """
    Mulai N consumer worker tasks secara concurrent.

    Args:
        redis_client: Koneksi Redis yang sudah diinisialisasi
        count: Jumlah worker (default dari settings.worker_count)

    Returns:
        List dari asyncio.Task yang berjalan
    """
    worker_count = count or settings.worker_count
    tasks = []

    for i in range(worker_count):
        worker_id = f"worker-{i+1:02d}-{uuid.uuid4().hex[:6]}"
        task = asyncio.create_task(
            consumer_worker(worker_id, redis_client),
            name=f"consumer_{worker_id}"
        )
        tasks.append(task)
        logger.info(f"Consumer worker '{worker_id}' berhasil dimulai.")

    logger.info(f"Total {worker_count} consumer workers aktif.")
    return tasks


async def stop_workers(worker_tasks: list[asyncio.Task]) -> None:
    """
    Hentikan semua worker secara graceful.
    Menunggu worker selesai memproses event yang sedang berjalan.
    """
    logger.info("Menghentikan consumer workers...")
    signal_shutdown()

    # Beri waktu worker selesai memproses event yang sedang berjalan
    await asyncio.sleep(settings.redis_brpop_timeout + 1)

    # Cancel task yang masih berjalan
    for task in worker_tasks:
        if not task.done():
            task.cancel()

    # Tunggu semua task selesai
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    logger.info("Semua consumer workers berhasil dihentikan.")
