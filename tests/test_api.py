"""
test_api.py - Pengujian Endpoint API

Test ini memverifikasi:
1. POST /publish menerima single event dengan benar
2. POST /publish menerima batch event
3. GET /events mengembalikan event yang sudah diproses
4. GET /stats mengembalikan struktur yang benar
5. GET /health mengembalikan status komponen
"""

import asyncio
import uuid
from typing import Callable, Dict, Any

import httpx
import pytest


# ============================================================
# Test 5: POST /publish - Single Event
# ============================================================
@pytest.mark.asyncio
async def test_api_publish_single_event(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    POST /publish dengan satu event harus mengembalikan 200
    dengan body yang berisi received=1 dan queued=1.
    """
    event = make_event(topic="api.test.single")

    response = await client.post("/publish", json=event)

    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    body = response.json()
    assert body["received"] == 1
    assert body["queued"] == 1
    assert body["status"] == "queued"


# ============================================================
# Test 6: POST /publish - Batch Event
# ============================================================
@pytest.mark.asyncio
async def test_api_publish_batch_events(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    POST /publish dengan list event (batch) harus berhasil.
    Semua event di-queue sekaligus.
    """
    batch_size = 10
    events = [make_event(topic="api.test.batch") for _ in range(batch_size)]

    response = await client.post("/publish", json=events)

    assert response.status_code == 200, f"Batch publish gagal: {response.text}"
    body = response.json()
    assert body["received"] == batch_size
    assert body["queued"] == batch_size


# ============================================================
# Test 7: GET /events - Verifikasi Event Tersimpan
# ============================================================
@pytest.mark.asyncio
async def test_api_get_events_after_publish(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Setelah publish, event harus muncul di GET /events.
    Verifikasi struktur response dan konten event.
    """
    unique_topic = f"api.test.events.{uuid.uuid4().hex[:8]}"
    event = make_event(topic=unique_topic)
    event_id = event["event_id"]

    # Publish event
    await client.post("/publish", json=event)

    # Tunggu diproses
    await asyncio.sleep(2)

    # Query events
    response = await client.get(f"/events?topic={unique_topic}")
    assert response.status_code == 200

    events_list = response.json()
    assert len(events_list) >= 1, "Event tidak ditemukan setelah diproses"

    # Cari event kita
    found = next((e for e in events_list if e["event_id"] == event_id), None)
    assert found is not None, f"Event dengan event_id={event_id} tidak ditemukan"

    # Verifikasi struktur response
    required_fields = ["id", "topic", "event_id", "source", "timestamp", "payload", "received_at"]
    for field in required_fields:
        assert field in found, f"Field '{field}' tidak ada dalam response event"

    assert found["topic"] == unique_topic
    assert found["source"] == event["source"]


# ============================================================
# Test 8: GET /stats - Struktur Response
# ============================================================
@pytest.mark.asyncio
async def test_api_stats_structure(
    client: httpx.AsyncClient,
    wait_for_aggregator,
):
    """
    GET /stats harus mengembalikan semua field yang diperlukan
    dengan tipe data yang benar.
    """
    response = await client.get("/stats")
    assert response.status_code == 200, f"GET /stats gagal: {response.text}"

    body = response.json()

    # Verifikasi semua field wajib ada
    required_fields = [
        "received", "unique_processed", "duplicate_dropped",
        "topics", "uptime_seconds", "queue_length", "worker_count", "service"
    ]
    for field in required_fields:
        assert field in body, f"Field '{field}' tidak ada dalam /stats response"

    # Verifikasi tipe data
    assert isinstance(body["received"], int), "received harus integer"
    assert isinstance(body["unique_processed"], int), "unique_processed harus integer"
    assert isinstance(body["duplicate_dropped"], int), "duplicate_dropped harus integer"
    assert isinstance(body["topics"], list), "topics harus list"
    assert isinstance(body["uptime_seconds"], (int, float)), "uptime_seconds harus number"
    assert isinstance(body["queue_length"], int), "queue_length harus integer"
    assert isinstance(body["worker_count"], int), "worker_count harus integer"
    assert body["worker_count"] > 0, "Harus ada minimal 1 worker aktif"


# ============================================================
# Test 9: GET /health - Status Komponen
# ============================================================
@pytest.mark.asyncio
async def test_api_health_check(
    client: httpx.AsyncClient,
    wait_for_aggregator,
):
    """
    GET /health harus mengembalikan status semua komponen.
    Dalam kondisi normal semua harus 'ok'.
    """
    response = await client.get("/health")
    assert response.status_code in (200, 503), f"Unexpected status: {response.status_code}"

    body = response.json()
    assert "status" in body
    assert "database" in body
    assert "broker" in body
    assert "workers_running" in body
    assert "uptime_seconds" in body

    # Dalam kondisi normal semua harus healthy
    assert body["database"] == "ok", f"Database tidak healthy: {body['database']}"
    assert body["broker"] == "ok", f"Broker tidak healthy: {body['broker']}"
    assert body["status"] == "healthy"
