"""
test_persistence.py - Pengujian Persistensi Data

Test ini membuktikan bahwa:
1. Data event tetap ada setelah container direstartd (named volumes)
2. Dedup store (processed_events) tetap mencegah reprocessing setelah restart

PENTING: Test ini tidak bisa otomatis restart Docker container.
Sebagai pengganti, tes mensimulasikan kondisi "setelah restart" dengan cara:
- Kirim event, pastikan tersimpan
- Verifikasi data ada di database melalui GET /events
- Kirim event yang sama lagi → harus ditolak sebagai duplikat

Untuk demo video: tunjukkan docker compose restart storage → data masih ada.

Bab yang diuji: 6 (Crash Tolerance), 10-11 (Persistent Storage)
"""

import asyncio
import uuid
from typing import Callable

import httpx
import pytest


# ============================================================
# Test 13: Data Tersimpan Setelah Event Diproses
# ============================================================
@pytest.mark.asyncio
@pytest.mark.persistence
async def test_persistence_event_saved_to_database(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario: Kirim event dan verifikasi tersimpan di database.
    Ekspektasi: Event muncul di GET /events dengan semua field yang benar.
    Persistensi: Data disimpan di PostgreSQL dengan named volume (pg_data).
    """
    unique_topic = f"persist.test.{uuid.uuid4().hex[:8]}"
    event = make_event(
        topic=unique_topic,
        source="persistence-test",
        payload={"test_run": True, "data": "persistent"},
    )

    # Publish event
    r = await client.post("/publish", json=event)
    assert r.status_code == 200

    # Tunggu diproses consumer
    await asyncio.sleep(2)

    # Verifikasi ada di database
    response = await client.get(f"/events?topic={unique_topic}")
    assert response.status_code == 200

    events_list = response.json()
    assert len(events_list) >= 1, "Event tidak ditemukan di database!"

    # Cari event spesifik kita
    found = next((e for e in events_list if e["event_id"] == event["event_id"]), None)
    assert found is not None, f"Event {event['event_id']} tidak tersimpan di database"

    # Verifikasi data payload tersimpan dengan benar
    assert found["payload"]["test_run"] == True
    assert found["payload"]["data"] == "persistent"
    assert found["source"] == "persistence-test"


# ============================================================
# Test 14: Dedup Tetap Berlaku Setelah Data Tersimpan
# ============================================================
@pytest.mark.asyncio
@pytest.mark.persistence
async def test_persistence_dedup_survives_after_stored(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario (simulasi "setelah restart"):
    1. Kirim event dan tunggu diproses
    2. Catat statistik
    3. Kirim event yang sama LAGI (simulasi publisher retry setelah restart)
    4. Verifikasi: event tetap ditolak sebagai duplikat

    Ini membuktikan bahwa dedup store (UNIQUE constraint di Postgres)
    bersifat PERSISTEN: tidak hilang saat container restart karena
    data disimpan di named volume (pg_data).

    Untuk demo video nyata: hapus container lalu recreate, data tetap ada.
    """
    unique_topic = f"persist.dedup.{uuid.uuid4().hex[:8]}"
    event = make_event(topic=unique_topic)

    # Langkah 1: Kirim event pertama kali
    r1 = await client.post("/publish", json=event)
    assert r1.status_code == 200

    await asyncio.sleep(2)

    # Langkah 2: Verifikasi tersimpan
    events_response = await client.get(f"/events?topic={unique_topic}")
    events_list = events_response.json()
    assert any(e["event_id"] == event["event_id"] for e in events_list), (
        "Event pertama tidak tersimpan di database"
    )

    # Ambil stats sebelum retry
    stats_before = (await client.get("/stats")).json()
    dup_before = stats_before["duplicate_dropped"]

    # Langkah 3: Simulasi "publisher restart dan retry" - kirim event yang sama
    r2 = await client.post("/publish", json=event)
    assert r2.status_code == 200

    await asyncio.sleep(2)

    # Langkah 4: Verifikasi duplikat ditolak
    stats_after = (await client.get("/stats")).json()
    dup_after = stats_after["duplicate_dropped"]

    assert dup_after == dup_before + 1, (
        f"Event duplikat setelah 'restart' tidak terdeteksi. "
        f"duplicate_dropped hanya bertambah {dup_after - dup_before} (seharusnya 1). "
        f"Dedup store mungkin tidak persisten!"
    )

    # Verifikasi tidak ada event baru yang ditambahkan (masih 1 event, bukan 2)
    events_response2 = await client.get(f"/events?topic={unique_topic}")
    events_list2 = events_response2.json()
    matching = [e for e in events_list2 if e["event_id"] == event["event_id"]]
    assert len(matching) == 1, (
        f"Seharusnya hanya 1 event dengan event_id ini, tapi ada {len(matching)}"
    )
