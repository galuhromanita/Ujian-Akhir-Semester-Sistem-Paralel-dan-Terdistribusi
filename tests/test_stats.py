"""
test_stats.py - Pengujian Konsistensi Statistik

Test ini memverifikasi:
1. Stats bertambah dengan benar setelah publish
2. Breakdown per-topic akurat
3. Hubungan: received = unique_processed + duplicate_dropped

Bab yang diuji: 12-13 (Observabilitas, Koordinasi)
"""

import asyncio
import uuid
from typing import Callable

import httpx
import pytest


# ============================================================
# Test 20: Stats Bertambah Benar Setelah Publish
# ============================================================
@pytest.mark.asyncio
async def test_stats_increment_after_publish(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Setelah publish N event unik, received dan unique_processed
    harus bertambah tepat N.
    """
    N = 5
    topic = f"stats.test.increment.{uuid.uuid4().hex[:6]}"
    events = [make_event(topic=topic) for _ in range(N)]

    stats_before = (await client.get("/stats")).json()

    # Kirim semua event
    for event in events:
        await client.post("/publish", json=event)

    await asyncio.sleep(3)

    stats_after = (await client.get("/stats")).json()

    # Verifikasi increment
    added_unique = stats_after["unique_processed"] - stats_before["unique_processed"]
    assert added_unique == N, (
        f"unique_processed seharusnya +{N}, dapat +{added_unique}"
    )


# ============================================================
# Test 21: Stats per Topic Akurat
# ============================================================
@pytest.mark.asyncio
async def test_stats_per_topic_accuracy(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Verifikasi bahwa stats per-topic dalam response GET /stats
    mencerminkan jumlah event yang dikirim ke topic tersebut.
    """
    # Gunakan topic unik untuk isolasi test ini
    unique_topic = f"stats.topic.accuracy.{uuid.uuid4().hex[:8]}"

    # Kirim 3 event unik + 2 duplikat ke topic ini
    events = [make_event(topic=unique_topic) for _ in range(3)]
    duplicates = events[:2]  # Duplikat 2 event pertama

    for e in events + duplicates:
        await client.post("/publish", json=e)

    await asyncio.sleep(3)

    # Periksa stats
    stats_response = (await client.get("/stats")).json()

    # Cari topic kita
    topic_stats = next(
        (t for t in stats_response["topics"] if t["topic"] == unique_topic),
        None
    )

    assert topic_stats is not None, f"Topic '{unique_topic}' tidak ditemukan di /stats"
    assert topic_stats["unique_processed"] == 3, (
        f"unique_processed untuk topic ini seharusnya 3, dapat {topic_stats['unique_processed']}"
    )
    assert topic_stats["duplicate_dropped"] == 2, (
        f"duplicate_dropped untuk topic ini seharusnya 2, dapat {topic_stats['duplicate_dropped']}"
    )
    assert topic_stats["received"] == 5, (
        f"received untuk topic ini seharusnya 5 (3 unik + 2 duplikat), dapat {topic_stats['received']}"
    )


# ============================================================
# Test 22: Invariant: received = unique_processed + duplicate_dropped
# ============================================================
@pytest.mark.asyncio
async def test_stats_invariant_received_equals_sum(
    client: httpx.AsyncClient,
    wait_for_aggregator,
):
    """
    Invariant statistik yang harus selalu berlaku:
    received = unique_processed + duplicate_dropped

    Jika invariant ini rusak, ada bug di logika update stats
    (kemungkinan race condition atau lost-update).
    """
    # Tunggu antrian kosong (semua event diproses)
    await asyncio.sleep(2)

    stats = (await client.get("/stats")).json()

    total_received = stats["received"]
    total_unique = stats["unique_processed"]
    total_dup = stats["duplicate_dropped"]

    assert total_received == total_unique + total_dup, (
        f"Invariant statistik rusak! "
        f"received={total_received} ≠ "
        f"unique_processed({total_unique}) + duplicate_dropped({total_dup}) = {total_unique + total_dup}. "
        f"Ada kemungkinan race condition atau lost-update dalam update statistik."
    )

    # Per-topic invariant
    for topic_stat in stats["topics"]:
        t = topic_stat["topic"]
        r = topic_stat["received"]
        u = topic_stat["unique_processed"]
        d = topic_stat["duplicate_dropped"]
        assert r == u + d, (
            f"Invariant rusak untuk topic '{t}': "
            f"received={r} ≠ unique_processed({u}) + duplicate_dropped({d})"
        )
