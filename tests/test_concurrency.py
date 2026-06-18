"""
test_concurrency.py - Pengujian Race Condition dan Konkurensi

Test ini membuktikan bahwa:
1. Banyak request paralel dengan event_id yang sama → hanya 1 yang diproses
2. Statistik tetap konsisten di bawah beban paralel (tidak ada lost-update)
3. Multi-thread publish tidak menghasilkan data duplikat

Ini adalah pengujian inti dari Bab 8-9 (Transaksi & Kontrol Konkurensi):
- UNIQUE(topic, event_id) + ON CONFLICT DO NOTHING mencegah double-insert
- UPDATE SET count = count + 1 dalam transaksi mencegah lost-update pada stats

Bab yang diuji: 8 (Transaksi), 9 (Kontrol Konkurensi)
"""

import asyncio
import uuid
from typing import Callable, List, Dict, Any

import httpx
import pytest

BASE_URL = "http://localhost:8080"


async def get_stats_with_retry(client: httpx.AsyncClient, retries: int = 3) -> dict:
    """
    Ambil /stats dengan retry menggunakan fresh connection.
    Diperlukan karena setelah banyak concurrent request, uvicorn
    menutup koneksi keep-alive sehingga reuse koneksi lama bisa
    gagal dengan httpx.ReadError atau RemoteProtocolError.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            if attempt == 0:
                return (await client.get("/stats")).json()
            # Jika gagal, buat fresh client dengan koneksi baru
            await asyncio.sleep(1)
            async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as fresh:
                return (await fresh.get("/stats")).json()
        except (httpx.ReadError, httpx.RemoteProtocolError, httpx.ConnectError) as exc:
            last_exc = exc
            await asyncio.sleep(1)
    raise last_exc


# ============================================================
# Test 10: Race Condition - Banyak Request Paralel, Event ID Sama
# ============================================================
@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrency_parallel_duplicate_event_ids(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario:
    - Kirim event yang SAMA (topic + event_id sama) sebanyak 20 kali secara PARALEL
    - Mensimulasikan kondisi race condition paling ekstrem

    Ekspektasi:
    - unique_processed bertambah tepat 1 (bukan 20)
    - duplicate_dropped bertambah tepat 19
    - Tidak ada exception atau error 5xx

    Ini membuktikan bahwa UNIQUE constraint + ON CONFLICT DO NOTHING
    bekerja dengan benar untuk menangani race condition.
    """
    PARALLEL_COUNT = 20

    shared_event = make_event(topic="test.concurrency.race")
    shared_event_id = shared_event["event_id"]

    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]
    dup_before = stats_before["duplicate_dropped"]

    # Kirim semua event secara paralel menggunakan asyncio.gather
    async def send_one():
        r = await client.post("/publish", json=shared_event)
        return r.status_code

    results = await asyncio.gather(*[send_one() for _ in range(PARALLEL_COUNT)])

    # Semua request harus berhasil (200) - tidak ada yang crash
    for i, status in enumerate(results):
        assert status == 200, f"Request ke-{i+1} gagal dengan status {status}"

    # Tunggu consumer memproses semua event dari antrian
    await asyncio.sleep(4)

    stats_after = (await client.get("/stats")).json()
    unique_after = stats_after["unique_processed"]
    dup_after = stats_after["duplicate_dropped"]

    # Hanya 1 event yang boleh diproses
    assert unique_after == unique_before + 1, (
        f"Race condition terdeteksi! "
        f"unique_processed +{unique_after - unique_before} (seharusnya +1). "
        f"UNIQUE constraint + ON CONFLICT DO NOTHING tidak bekerja dengan benar."
    )

    # 19 lainnya harus jadi duplikat
    assert dup_after == dup_before + (PARALLEL_COUNT - 1), (
        f"duplicate_dropped +{dup_after - dup_before} (seharusnya +{PARALLEL_COUNT - 1})"
    )


# ============================================================
# Test 11: Konsistensi Statistik di Bawah Beban Paralel
# ============================================================
@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrency_stats_consistency_under_load(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario:
    - Kirim 50 event UNIK secara paralel dalam beberapa batch
    - Setiap event berbeda (event_id unik)

    Ekspektasi:
    - unique_processed bertambah tepat 50
    - Tidak ada lost-update pada stats (UPDATE SET count = count + 1)
    - Konsistensi terjaga meski multi-worker memproses bersamaan
    """
    UNIQUE_EVENT_COUNT = 50
    topic = f"test.concurrency.stats.{uuid.uuid4().hex[:8]}"

    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]

    # Generate 50 event unik
    events = [make_event(topic=topic) for _ in range(UNIQUE_EVENT_COUNT)]

    # Kirim semua paralel
    async def send_event(ev):
        r = await client.post("/publish", json=ev)
        return r.status_code

    results = await asyncio.gather(*[send_event(e) for e in events])

    assert all(s == 200 for s in results), "Ada request yang gagal"

    # Tunggu consumer memproses semua
    await asyncio.sleep(5)

    stats_after = await get_stats_with_retry(client)
    unique_after = stats_after["unique_processed"]

    added = unique_after - unique_before
    assert added == UNIQUE_EVENT_COUNT, (
        f"Konsistensi statistik rusak! "
        f"Dikirim {UNIQUE_EVENT_COUNT} event unik, tapi hanya {added} yang tercatat. "
        f"Lost-update terdeteksi dalam update stats."
    )


# ============================================================
# Test 12: Multi-Batch Bersamaan Tidak Menghasilkan Duplikat
# ============================================================
@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrency_no_double_processing(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Skenario:
    - Buat 10 event unik
    - Kirim setiap event dalam 3 batch berbeda (jadi 30 event total, dengan 20 duplikat)
    - Semua batch dikirim secara paralel

    Ekspektasi:
    - unique_processed bertambah tepat 10
    - duplicate_dropped bertambah tepat 20
    - Tidak ada double-processing meskipun concurrency tinggi
    """
    UNIQUE_COUNT = 10
    SEND_TIMES = 3
    topic = f"test.concurrency.nodbl.{uuid.uuid4().hex[:8]}"

    # Buat event unik
    unique_events = [make_event(topic=topic) for _ in range(UNIQUE_COUNT)]

    stats_before = (await client.get("/stats")).json()
    unique_before = stats_before["unique_processed"]
    dup_before = stats_before["duplicate_dropped"]

    # Buat daftar semua kiriman: 3 kali kirim event yang sama
    all_sends = []
    for _ in range(SEND_TIMES):
        for event in unique_events:
            all_sends.append(event)

    # Kirim semua secara paralel
    async def send_ev(ev):
        return (await client.post("/publish", json=ev)).status_code

    results = await asyncio.gather(*[send_ev(e) for e in all_sends])
    assert all(s == 200 for s in results), "Ada request yang gagal"

    await asyncio.sleep(5)

    stats_after = await get_stats_with_retry(client)
    unique_after = stats_after["unique_processed"]
    dup_after = stats_after["duplicate_dropped"]

    expected_unique_added = UNIQUE_COUNT
    expected_dup_added = UNIQUE_COUNT * (SEND_TIMES - 1)

    assert unique_after == unique_before + expected_unique_added, (
        f"unique_processed: expected +{expected_unique_added}, got +{unique_after - unique_before}"
    )
    assert dup_after == dup_before + expected_dup_added, (
        f"duplicate_dropped: expected +{expected_dup_added}, got +{dup_after - dup_before}"
    )
