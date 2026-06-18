"""
main.py - Publisher / Event Simulator

Fungsi:
1. Generate sejumlah event unik dengan UUID v4 sebagai event_id
2. Tambahkan duplikat intentional (sesuai DUPLICATE_RATIO)
   → Mensimulasikan kondisi real: at-least-once delivery, retry, network timeout
3. Kirim event dalam batch ke POST /publish pada aggregator
4. Tampilkan statistik pengiriman di akhir

Konfigurasi via Environment Variables:
- TARGET_URL       : URL endpoint POST /publish (default: http://aggregator:8080/publish)
- EVENT_COUNT      : Jumlah event unik yang di-generate (default: 1000)
- DUPLICATE_RATIO  : Rasio duplikat (0.0–1.0, default: 0.3 = 30% duplikat)
- BATCH_SIZE       : Jumlah event per batch POST request (default: 50)
- DELAY_MS         : Jeda antar batch dalam milidetik (default: 100)
- TOPICS           : Daftar topik dipisah koma
"""

import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any

import httpx

# ============================================================
# Konfigurasi dari Environment Variables
# ============================================================
TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
EVENT_COUNT = int(os.getenv("EVENT_COUNT", "1000"))
DUPLICATE_RATIO = float(os.getenv("DUPLICATE_RATIO", "0.3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
DELAY_MS = int(os.getenv("DELAY_MS", "100"))
TOPICS_STR = os.getenv(
    "TOPICS",
    "sensor.temperature,sensor.humidity,system.cpu,system.memory,app.login,app.logout,network.packet,db.query"
)
TOPICS = [t.strip() for t in TOPICS_STR.split(",") if t.strip()]

# ============================================================
# Setup Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | Publisher | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# Generator Event
# ============================================================

def generate_event(topic: str, event_id: str = None) -> Dict[str, Any]:
    """
    Buat satu event dengan struktur sesuai spesifikasi UAS.

    Args:
        topic: topik event
        event_id: jika None, generate UUID v4 baru

    Returns:
        Dictionary event siap dikirim
    """
    if event_id is None:
        event_id = f"evt-{uuid.uuid4()}"

    # Payload bervariasi sesuai topik untuk realisme
    payload = generate_payload(topic)

    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": f"simulator-{random.randint(1, 10):02d}",
        "payload": payload,
    }


def generate_payload(topic: str) -> Dict[str, Any]:
    """Generate payload yang relevan dengan topik."""
    if "temperature" in topic:
        return {
            "value": round(random.uniform(15.0, 45.0), 2),
            "unit": "celsius",
            "sensor_id": f"temp-{random.randint(1, 20)}",
        }
    elif "humidity" in topic:
        return {
            "value": round(random.uniform(20.0, 90.0), 2),
            "unit": "percent",
            "sensor_id": f"hum-{random.randint(1, 20)}",
        }
    elif "cpu" in topic:
        return {
            "usage_percent": round(random.uniform(0.0, 100.0), 2),
            "core_count": random.choice([2, 4, 8, 16]),
            "hostname": f"host-{random.randint(1, 5)}",
        }
    elif "memory" in topic:
        total = random.choice([8192, 16384, 32768])
        used = random.randint(1024, total - 512)
        return {
            "total_mb": total,
            "used_mb": used,
            "free_mb": total - used,
            "hostname": f"host-{random.randint(1, 5)}",
        }
    elif "login" in topic or "logout" in topic:
        return {
            "user_id": f"user-{random.randint(1000, 9999)}",
            "ip_address": f"192.168.{random.randint(1, 10)}.{random.randint(1, 254)}",
            "user_agent": random.choice(["Chrome/120", "Firefox/121", "Safari/17"]),
        }
    elif "network" in topic:
        return {
            "bytes_sent": random.randint(100, 100000),
            "bytes_recv": random.randint(100, 500000),
            "interface": random.choice(["eth0", "wlan0", "lo"]),
        }
    elif "db" in topic:
        return {
            "query_ms": round(random.uniform(0.5, 500.0), 2),
            "table": random.choice(["users", "orders", "products", "logs"]),
            "operation": random.choice(["SELECT", "INSERT", "UPDATE", "DELETE"]),
        }
    else:
        return {
            "value": random.randint(1, 1000),
            "random_key": uuid.uuid4().hex[:8],
        }


def build_event_list() -> List[Dict[str, Any]]:
    """
    Buat daftar event dengan duplikat intentional.

    Strategi:
    1. Generate EVENT_COUNT event unik
    2. Tambahkan DUPLICATE_RATIO * EVENT_COUNT event duplikat
       (pilih acak dari event yang sudah ada)
    3. Acak urutan semua event (shuffle)
    """
    logger.info(f"Generating {EVENT_COUNT} event unik dengan {int(DUPLICATE_RATIO * 100)}% duplikat...")

    # Buat event unik
    unique_events = []
    for _ in range(EVENT_COUNT):
        topic = random.choice(TOPICS)
        event = generate_event(topic)
        unique_events.append(event)

    # Hitung jumlah duplikat
    dup_count = int(EVENT_COUNT * DUPLICATE_RATIO)
    duplicate_events = []
    for _ in range(dup_count):
        # Pilih event yang sudah ada secara acak dan duplikasi
        original = random.choice(unique_events)
        duplicate = dict(original)  # copy
        # Timestamp boleh berbeda (simulasi retry yang terjadi kemudian)
        duplicate["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        duplicate_events.append(duplicate)

    # Gabungkan dan acak urutan
    all_events = unique_events + duplicate_events
    random.shuffle(all_events)

    logger.info(
        f"Total event akan dikirim: {len(all_events)} "
        f"(unik: {len(unique_events)}, duplikat: {len(duplicate_events)})"
    )
    return all_events, len(unique_events), len(duplicate_events)


# ============================================================
# Pengiriman Event
# ============================================================

async def send_batch(
    client: httpx.AsyncClient,
    batch: List[Dict[str, Any]],
    batch_num: int,
    max_retries: int = 3,
) -> int:
    """
    Kirim satu batch event ke aggregator dengan retry dan exponential backoff.

    Returns:
        Jumlah event yang berhasil dikirim dalam batch ini
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.post(
                TARGET_URL,
                json=batch,
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            logger.debug(
                f"Batch {batch_num}: {data.get('queued', 0)} event di-queue "
                f"(attempt {attempt})"
            )
            return len(batch)

        except httpx.HTTPStatusError as e:
            logger.warning(
                f"Batch {batch_num} gagal (HTTP {e.response.status_code}): "
                f"{e.response.text[:100]}. Attempt {attempt}/{max_retries}"
            )
        except httpx.RequestError as e:
            logger.warning(
                f"Batch {batch_num} gagal (network error): {e}. "
                f"Attempt {attempt}/{max_retries}"
            )

        if attempt < max_retries:
            # Exponential backoff: 1s, 2s, 4s
            backoff = 2 ** (attempt - 1)
            logger.info(f"Menunggu {backoff}s sebelum retry...")
            await asyncio.sleep(backoff)

    logger.error(f"Batch {batch_num} gagal setelah {max_retries} percobaan.")
    return 0


async def run_publisher() -> None:
    """
    Fungsi utama publisher:
    1. Tunggu aggregator ready
    2. Generate event dengan duplikat
    3. Kirim dalam batch
    4. Tampilkan statistik akhir
    """
    logger.info("=" * 60)
    logger.info("UAS Publisher / Event Simulator Dimulai")
    logger.info("=" * 60)
    logger.info(f"Target URL  : {TARGET_URL}")
    logger.info(f"Event Count : {EVENT_COUNT}")
    logger.info(f"Dup Ratio   : {DUPLICATE_RATIO} ({int(DUPLICATE_RATIO*100)}%)")
    logger.info(f"Batch Size  : {BATCH_SIZE}")
    logger.info(f"Delay       : {DELAY_MS}ms")
    logger.info(f"Topics      : {', '.join(TOPICS)}")
    logger.info("=" * 60)

    # Tunggu aggregator siap (readiness check)
    health_url = TARGET_URL.replace("/publish", "/health")
    logger.info(f"Menunggu aggregator siap di {health_url}...")

    async with httpx.AsyncClient() as check_client:
        for i in range(30):  # Maksimal 30 detik
            try:
                resp = await check_client.get(health_url, timeout=3.0)
                if resp.status_code == 200:
                    logger.info("Aggregator siap!")
                    break
            except Exception:
                pass
            logger.info(f"Aggregator belum siap, menunggu... ({i+1}/30)")
            await asyncio.sleep(1)
        else:
            logger.error("Aggregator tidak tersedia setelah 30 detik. Keluar.")
            return

    # Generate event
    all_events, unique_count, dup_count = build_event_list()

    # Kirim dalam batch
    start_time = time.time()
    total_sent = 0
    batch_number = 0

    async with httpx.AsyncClient() as client:
        for i in range(0, len(all_events), BATCH_SIZE):
            batch = all_events[i:i + BATCH_SIZE]
            batch_number += 1

            sent = await send_batch(client, batch, batch_number)
            total_sent += sent

            if batch_number % 10 == 0:
                elapsed = time.time() - start_time
                throughput = total_sent / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Progress: {total_sent}/{len(all_events)} event terkirim "
                    f"| Throughput: {throughput:.1f} evt/s"
                )

            # Jeda antar batch
            if DELAY_MS > 0:
                await asyncio.sleep(DELAY_MS / 1000.0)

    # Statistik akhir
    elapsed = time.time() - start_time
    throughput = total_sent / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("SELESAI - Statistik Pengiriman:")
    logger.info(f"  Total event dikirim  : {total_sent}")
    logger.info(f"  Event unik           : {unique_count}")
    logger.info(f"  Event duplikat       : {dup_count}")
    logger.info(f"  Total waktu          : {elapsed:.2f} detik")
    logger.info(f"  Throughput           : {throughput:.1f} event/detik")
    logger.info(f"  Batches terkirim     : {batch_number}")
    logger.info("=" * 60)
    logger.info("Publisher selesai. Silakan periksa /stats pada aggregator.")


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    asyncio.run(run_publisher())
