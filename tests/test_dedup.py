"""
test_dedup.py - Pengujian Deduplication dan Idempotency

Test ini membuktikan bahwa:
1. Event yang sama (topic + event_id) hanya diproses SATU kali
2. Pengiriman berulang menghasilkan duplicate_dropped yang bertambah
3. Event dengan topic berbeda tapi event_id sama = BUKAN duplikat
4. Batch berisi campuran event baru dan duplikat ditangani dengan benar

Bab yang diuji: 6 (Failure Tolerance), 7 (Consistency), 8-9 (Transaction/Concurrency)
"""

import asyncio
import time
import uuid
from typing import Callable, Dict, Any

import httpx
import pytest


# ============================================================
# Test 1: Deduplication Dasar - Event Sama Dikirim 2 Kali
# ============================================================
@pytest.mark.asyncio
async def test_dedup_basic_duplicate_is_dropped(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario: Kirim event yang sama 2 kali.
    Ekspektasi:
    - Setelah event pertama diproses: unique_processed bertambah 1
    - Setelah event kedua (duplikat): duplicate_dropped bertambah 1, unique_processed TIDAK bertambah
    """
    # Ambil stats awal
    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]
    dup_before = stats_before["duplicate_dropped"]

    # Buat event unik
    event = make_event(topic="test.dedup.basic")

    # Kirim pertama kali
    r1 = await client.post("/publish", json=event)
    assert r1.status_code == 200, f"Publish pertama gagal: {r1.text}"

    # Kirim duplikat (event_id sama, topic sama)
    r2 = await client.post("/publish", json=event)
    assert r2.status_code == 200, f"Publish duplikat gagal: {r2.text}"

    # Tunggu consumer memproses
    await asyncio.sleep(2)

    # Periksa stats
    stats_after = (await client.get("/stats")).json()
    unique_after = stats_after["unique_processed"]
    dup_after = stats_after["duplicate_dropped"]

    # unique_processed harus bertambah tepat 1 (bukan 2)
    assert unique_after == unique_before + 1, (
        f"unique_processed seharusnya +1, tapi +{unique_after - unique_before}. "
        f"Event mungkin diproses lebih dari sekali!"
    )

    # duplicate_dropped harus bertambah 1
    assert dup_after == dup_before + 1, (
        f"duplicate_dropped seharusnya +1, tapi +{dup_after - dup_before}."
    )


# ============================================================
# Test 2: Deduplication Intensif - Event Sama Dikirim 5 Kali
# ============================================================
@pytest.mark.asyncio
async def test_dedup_multiple_duplicates(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario: Kirim event yang sama 5 kali (mensimulasikan retry agresif).
    Ekspektasi:
    - unique_processed bertambah tepat 1
    - duplicate_dropped bertambah tepat 4
    """
    REPEAT_COUNT = 5

    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]
    dup_before = stats_before["duplicate_dropped"]

    event = make_event(topic="test.dedup.multi")

    # Kirim event yang sama 5 kali
    for i in range(REPEAT_COUNT):
        r = await client.post("/publish", json=event)
        assert r.status_code == 200, f"Publish ke-{i+1} gagal"

    # Tunggu consumer memproses semua
    await asyncio.sleep(3)

    stats_after = (await client.get("/stats")).json()
    unique_after = stats_after["unique_processed"]
    dup_after = stats_after["duplicate_dropped"]

    assert unique_after == unique_before + 1, (
        f"Harusnya +1 unique, tapi dapat +{unique_after - unique_before}"
    )
    assert dup_after == dup_before + (REPEAT_COUNT - 1), (
        f"Harusnya +{REPEAT_COUNT-1} duplikat, tapi dapat +{dup_after - dup_before}"
    )


# ============================================================
# Test 3: Event ID Sama, Topic Berbeda = BUKAN Duplikat
# ============================================================
@pytest.mark.asyncio
async def test_dedup_same_event_id_different_topic_not_duplicate(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario: Dua event dengan event_id sama tapi topic BERBEDA.
    Ekspektasi: Keduanya diproses (bukan duplikat).
    Dedup berbasis (topic, event_id) - bukan event_id saja.
    """
    shared_event_id = f"cross-topic-{uuid.uuid4()}"

    event_a = make_event(topic="test.topic.alpha", event_id=shared_event_id)
    event_b = make_event(topic="test.topic.beta", event_id=shared_event_id)

    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]

    await client.post("/publish", json=event_a)
    await client.post("/publish", json=event_b)

    await asyncio.sleep(2)

    stats_after = (await client.get("/stats")).json()
    unique_after = stats_after["unique_processed"]

    # Keduanya harus diproses: unique +2
    assert unique_after >= unique_before + 2, (
        f"Dua event dari topic berbeda seharusnya keduanya diproses. "
        f"unique_processed hanya +{unique_after - unique_before}"
    )


# ============================================================
# Test 4: Batch dengan Campuran Event Baru dan Duplikat
# ============================================================
@pytest.mark.asyncio
async def test_dedup_batch_with_mixed_events(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario: Kirim batch berisi 5 event unik + 3 duplikat (total 8).
    Ekspektasi: unique_processed bertambah 5, duplicate_dropped bertambah 3.
    """
    unique_events = [make_event(topic="test.dedup.batch") for _ in range(5)]
    duplicate_events = unique_events[:3]  # 3 event pertama sebagai duplikat

    all_events = unique_events + duplicate_events  # total 8

    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]
    dup_before = stats_before["duplicate_dropped"]

    # Kirim batch sekaligus
    r = await client.post("/publish", json=all_events)
    assert r.status_code == 200

    await asyncio.sleep(3)

    stats_after = (await client.get("/stats")).json()
    unique_after = stats_after["unique_processed"]
    dup_after = stats_after["duplicate_dropped"]

    assert unique_after == unique_before + 5, (
        f"Harusnya +5 unique, dapat +{unique_after - unique_before}"
    )
    assert dup_after == dup_before + 3, (
        f"Harusnya +3 duplikat, dapat +{dup_after - dup_before}"
    )
