"""
models.py - Model Data Pydantic untuk Validasi Input/Output
Mendefinisikan struktur event JSON yang diterima dan dikembalikan API.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Any, Optional, List
from datetime import datetime, timezone
import uuid


class EventPayload(BaseModel):
    """
    Payload fleksibel - menerima objek JSON apa pun.
    """
    model_config = {"extra": "allow"}


class Event(BaseModel):
    """
    Model event utama sesuai spesifikasi UAS.

    Field wajib:
    - topic: kategori event (mis. 'sensor.temperature')
    - event_id: identifier unik per event (UUID atau ULID)
    - timestamp: waktu event dalam format ISO8601
    - source: sumber pengirim event
    - payload: data tambahan bebas format
    """

    topic: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Kategori/topik event",
        examples=["sensor.temperature"]
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Identifier unik event (UUID/ULID)",
        examples=["evt-550e8400-e29b-41d4-a716-446655440000"]
    )
    timestamp: str = Field(
        ...,
        description="Waktu event dalam format ISO8601",
        examples=["2024-01-15T10:30:00Z"]
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Sumber pengirim event",
        examples=["sensor-node-01"]
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Data payload event (bebas format JSON)"
    )

    @field_validator("topic")
    @classmethod
    def topic_must_not_have_spaces(cls, v: str) -> str:
        """Topic tidak boleh mengandung spasi - gunakan titik sebagai separator."""
        if " " in v:
            raise ValueError("Topic tidak boleh mengandung spasi. Gunakan titik (.) sebagai separator.")
        return v.strip()

    @field_validator("event_id")
    @classmethod
    def event_id_strip(cls, v: str) -> str:
        """Hapus whitespace di awal/akhir event_id."""
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_valid_iso8601(cls, v: str) -> str:
        """Validasi format timestamp ISO8601."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Timestamp '{v}' bukan format ISO8601 yang valid.")
        return v


class EventBatch(BaseModel):
    """
    Model untuk menerima batch event sekaligus.
    Mendukung pengiriman 1 atau banyak event dalam satu request.
    """

    events: List[Event] = Field(
        ...,
        min_length=1,
        description="Daftar event yang akan dipublikasikan"
    )


class PublishResponse(BaseModel):
    """Response dari endpoint POST /publish."""
    status: str
    received: int
    queued: int
    message: str


class EventResponse(BaseModel):
    """Representasi event untuk dikembalikan di GET /events."""
    id: int
    topic: str
    event_id: str
    source: str
    timestamp: str
    payload: dict[str, Any]
    received_at: str


class StatsResponse(BaseModel):
    """Response dari endpoint GET /stats."""
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[dict]
    uptime_seconds: float
    queue_length: int
    worker_count: int
    service: str


class HealthResponse(BaseModel):
    """Response dari endpoint GET /health."""
    status: str
    database: str
    broker: str
    workers_running: int
    uptime_seconds: float
