"""
test_schema.py - Pengujian Validasi Skema Event

Test ini memverifikasi:
1. Field wajib yang hilang → 422 Unprocessable Entity
2. Timestamp dengan format tidak valid → 422
3. Topic dengan spasi → 422
4. Event valid dengan berbagai tipe payload → 200
5. Event_id kosong → 422

Bab yang diuji: 3 (Komunikasi), 4 (Penamaan - naming, identifikasi unik)
"""

import asyncio
import uuid
from typing import Callable, Dict, Any

import httpx
import pytest


# ============================================================
# Test 15: Field Wajib Hilang - topic
# ============================================================
@pytest.mark.asyncio
async def test_schema_missing_required_field_topic(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Event tanpa field 'topic' harus ditolak dengan 422 Unprocessable Entity.
    """
    event = make_event()
    del event["topic"]  # Hapus field wajib

    response = await client.post("/publish", json=event)
    assert response.status_code == 422, (
        f"Event tanpa 'topic' seharusnya 422, dapat {response.status_code}"
    )


# ============================================================
# Test 16: Field Wajib Hilang - event_id
# ============================================================
@pytest.mark.asyncio
async def test_schema_missing_required_field_event_id(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Event tanpa field 'event_id' harus ditolak dengan 422.
    """
    event = make_event()
    del event["event_id"]

    response = await client.post("/publish", json=event)
    assert response.status_code == 422, (
        f"Event tanpa 'event_id' seharusnya 422, dapat {response.status_code}"
    )


# ============================================================
# Test 17: Timestamp Format Tidak Valid
# ============================================================
@pytest.mark.asyncio
async def test_schema_invalid_timestamp_format(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Event dengan timestamp yang bukan ISO8601 harus ditolak dengan 422.
    """
    event = make_event(timestamp="bukan-tanggal-yang-valid")

    response = await client.post("/publish", json=event)
    assert response.status_code == 422, (
        f"Event dengan timestamp tidak valid seharusnya 422, dapat {response.status_code}"
    )


# ============================================================
# Test 18: Topic dengan Spasi Tidak Valid
# ============================================================
@pytest.mark.asyncio
async def test_schema_topic_with_spaces_rejected(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Topic yang mengandung spasi harus ditolak.
    Konvensi penamaan: gunakan titik sebagai separator (mis. 'sensor.temperature').
    """
    event = make_event(topic="topic dengan spasi")

    response = await client.post("/publish", json=event)
    assert response.status_code == 422, (
        f"Event dengan topic berisi spasi seharusnya 422, dapat {response.status_code}"
    )


# ============================================================
# Test 19: Event Valid dengan Payload Kompleks
# ============================================================
@pytest.mark.asyncio
async def test_schema_valid_event_with_complex_payload(
    client: httpx.AsyncClient,
    make_event: Callable,
    wait_for_aggregator,
):
    """
    Event dengan payload JSON kompleks (nested, array, berbagai tipe) harus diterima.
    """
    complex_payload = {
        "metrics": [1.5, 2.7, 3.14],
        "tags": {"region": "asia", "env": "prod"},
        "active": True,
        "count": 42,
        "nullable": None,
        "nested": {
            "deep": {
                "value": "test"
            }
        }
    }

    event = make_event(
        topic="schema.test.complex",
        payload=complex_payload
    )

    response = await client.post("/publish", json=event)
    assert response.status_code == 200, (
        f"Event valid dengan payload kompleks seharusnya 200, dapat {response.status_code}: {response.text}"
    )

    body = response.json()
    assert body["received"] == 1
    assert body["queued"] == 1
