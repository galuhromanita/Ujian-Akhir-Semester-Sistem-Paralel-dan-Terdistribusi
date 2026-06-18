"""
conftest.py - Konfigurasi dan Fixtures Bersama untuk Semua Test

Fixtures:
- base_url       : URL dasar aggregator (dari env atau default localhost:8080)
- client         : httpx.AsyncClient yang sudah dikonfigurasi
- unique_event   : Factory function untuk membuat event unik per test
- cleanup_stats  : Fixture opsional untuk bersihkan state (tidak hapus data, hanya tunggu)

Catatan:
- Test berjalan terhadap aggregator yang SUDAH berjalan (via Docker Compose atau lokal)
- Tidak menggunakan mock/stub; ini adalah integration tests nyata
- Setiap test menggunakan event_id unik (UUID) agar tidak bentrok antar test
"""

import asyncio
import os
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, Any

import httpx
import pytest
import pytest_asyncio

# ============================================================
# Konfigurasi
# ============================================================

BASE_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(scope="session")
def event_loop_policy():
    """Gunakan default event loop policy."""
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture
def base_url() -> str:
    """URL dasar aggregator."""
    return BASE_URL


@pytest_asyncio.fixture
async def client() -> httpx.AsyncClient:
    """
    httpx.AsyncClient yang sudah dikonfigurasi untuk test.
    Timeout 30 detik untuk operasi yang mungkin lambat.
    """
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as c:
        yield c


@pytest.fixture
def make_event() -> Callable[..., Dict[str, Any]]:
    """
    Factory function untuk membuat event dengan event_id unik per panggilan.
    Memastikan tidak ada bentrok antar test case.

    Penggunaan:
        event = make_event(topic="sensor.test")
        event = make_event(topic="sensor.test", event_id="custom-id")
    """
    def _make_event(
        topic: str = "test.topic",
        event_id: str = None,
        source: str = "test-simulator",
        payload: Dict[str, Any] = None,
        timestamp: str = None,
    ) -> Dict[str, Any]:
        return {
            "topic": topic,
            "event_id": event_id or f"test-evt-{uuid.uuid4()}",
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": source,
            "payload": payload or {"test": True, "value": 42},
        }
    return _make_event


@pytest_asyncio.fixture(autouse=False)
async def wait_for_aggregator(client: httpx.AsyncClient):
    """
    Pastikan aggregator berjalan sebelum test.
    Mencoba ping /health maksimal 10 kali.
    """
    for i in range(10):
        try:
            resp = await client.get("/health")
            if resp.status_code in (200, 503):
                return
        except Exception:
            pass
        await asyncio.sleep(1)

    pytest.skip("Aggregator tidak tersedia - pastikan docker compose up sudah berjalan")


# ============================================================
# Marker konfigurasi pytest
# ============================================================

def pytest_configure(config):
    config.addinivalue_line("markers", "slow: test yang memerlukan waktu lebih lama")
    config.addinivalue_line("markers", "concurrency: test yang menguji race condition")
    config.addinivalue_line("markers", "persistence: test yang menguji persistensi data")
