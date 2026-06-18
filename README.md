# UAS Sistem Terdistribusi — Pub-Sub Log Aggregator Terdistribusi

> **Idempotent Consumer · Deduplication · Transaksi & Kontrol Konkurensi · Docker Compose**

---

### Komponen

| Service | Image | Port | Fungsi |
|---|---|---|---|
| `aggregator` | `uas-aggregator:latest` | `8080` (expose) | API FastAPI + 3 consumer workers |
| `publisher` | `uas-publisher:latest` | — | Simulator event + 30% duplikat |
| `broker` | `redis:7-alpine` | internal | Message queue RPUSH/BRPOP |
| `storage` | `postgres:16-alpine` | internal | Penyimpanan persisten + dedup |

---

## Cara Build & Jalankan

### Prasyarat
- Docker Desktop (Windows) atau Docker Engine (Linux/Mac)
- Docker Compose v2

### 1. Jalankan Semua Service
```bash
docker compose up --build
```

Tunggu hingga semua service healthy (biasanya 30–60 detik).

### 2. Akses Aggregator
```
http://localhost:8080          → Info service
http://localhost:8080/docs     → Swagger UI / dokumentasi interaktif
http://localhost:8080/health   → Health check
http://localhost:8080/stats    → Statistik real-time
http://localhost:8080/events   → Daftar event terproses
```

### 3. Jalankan Publisher Secara Manual (Opsional)
```bash
# Jalankan publisher dengan konfigurasi berbeda
docker compose run --rm publisher \
  -e EVENT_COUNT=5000 \
  -e DUPLICATE_RATIO=0.4
```

### 4. Hentikan Semua Service
```bash
docker compose down
```

### 5. Hapus Volume (Reset Data)
```bash
docker compose down -v
```

---

## Endpoint API

### POST /publish
Publish single event atau batch event.

**Single Event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "sensor.temperature",
    "event_id": "evt-001",
    "timestamp": "2024-01-15T10:30:00Z",
    "source": "sensor-node-01",
    "payload": {"value": 25.4, "unit": "celsius"}
  }'
```

**Batch Event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '[
    {"topic": "sensor.temp", "event_id": "evt-001", "timestamp": "2024-01-15T10:30:00Z", "source": "node-01", "payload": {}},
    {"topic": "sensor.temp", "event_id": "evt-002", "timestamp": "2024-01-15T10:30:01Z", "source": "node-01", "payload": {}}
  ]'
```

**Response:**
```json
{
  "status": "queued",
  "received": 2,
  "queued": 2,
  "message": "2 event berhasil diterima dan masuk antrian pemrosesan."
}
```

### GET /events
```bash
# Semua event
curl http://localhost:8080/events

# Filter per topic
curl "http://localhost:8080/events?topic=sensor.temperature"

# Dengan pagination
curl "http://localhost:8080/events?limit=50&offset=0"
```

### GET /stats
```bash
curl http://localhost:8080/stats
```

```json
{
  "received": 1300,
  "unique_processed": 1000,
  "duplicate_dropped": 300,
  "topics": [
    {
      "topic": "sensor.temperature",
      "received": 500,
      "unique_processed": 350,
      "duplicate_dropped": 150,
      "last_updated": "2024-01-15T10:35:00Z"
    }
  ],
  "uptime_seconds": 245.7,
  "queue_length": 0,
  "worker_count": 3,
  "service": "UAS-Aggregator"
}
```

### GET /health
```bash
curl http://localhost:8080/health
```

---

## Menjalankan Tests

### Prasyarat
```bash
pip install -r tests/requirements.txt
```

### Jalankan Semua Tests
```bash
# Pastikan sistem berjalan: docker compose up --build

# Jalankan tests
pytest tests/ -v

# Jalankan dengan output detail
pytest tests/ -v --tb=long

# Jalankan kategori tertentu
pytest tests/ -v -m concurrency    # Hanya test konkurensi
pytest tests/ -v -m persistence    # Hanya test persistensi

# Dengan URL aggregator berbeda (jika bukan localhost:8080)
AGGREGATOR_URL=http://localhost:8080 pytest tests/ -v
```

### Daftar Tests

| File | Tests | Jumlah |
|---|---|---|
| `test_dedup.py` | Dedup dasar, multi-duplikat, cross-topic, batch campuran | 4 |
| `test_api.py` | POST single, POST batch, GET /events, GET /stats, GET /health | 5 |
| `test_concurrency.py` | Race condition 20 paralel, stats consistency 50 events, no double-processing | 3 |
| `test_persistence.py` | Data tersimpan di DB, dedup setelah data ada | 2 |
| `test_schema.py` | Missing topic, missing event_id, invalid timestamp, spasi di topic, payload kompleks | 5 |
| `test_stats.py` | Stats increment, per-topic accuracy, invariant received=unique+dup | 3 |
| **Total** | | **22 tests** |

---

## Load Test dengan k6

```bash
# Install k6 (Windows via winget)
winget install k6

# Jalankan load test
k6 run k6/load_test.js

# Target minimal: ≥ 20.000 event dengan ≥ 30% duplikat
k6 run -e TOTAL_EVENTS=20000 -e DUPLICATE_RATIO=0.3 k6/load_test.js

# Via Docker (gunakan jaringan Compose)
docker run --rm -i \
  --network uas_internal_network \
  grafana/k6 run \
  -e TARGET_URL=http://aggregator:8080 \
  - < k6/load_test.js
```

---

## Persistensi Data (Named Volumes)

Data disimpan di Docker named volumes yang **tidak terhapus** saat container dihapus:

| Volume | Lokasi di Container | Isi |
|---|---|---|
| `uas_pg_data` | `/var/lib/postgresql/data` | Database PostgreSQL (event, stats, audit) |
| `uas_broker_data` | `/data` | Redis AOF log (queue state) |

**Demonstrasi Persistensi:**
```bash
# 1. Jalankan sistem dan kirim beberapa event
docker compose up -d
# ... kirim event ...

# 2. Hapus container storage (tapi BUKAN volume)
docker compose stop storage
docker compose rm -f storage

# 3. Restart storage → data masih ada!
docker compose up -d storage
curl http://localhost:8080/stats  # Data tetap ada

# 4. Event yang sama → tetap ditolak sebagai duplikat
```

---

## Desain Idempotency & Deduplication

### Mengapa Idempotent?

Sistem menggunakan **at-least-once delivery**: publisher boleh mengirim event yang sama lebih dari satu kali (retry setelah timeout, restart, dll). Sistem harus tetap menghasilkan state yang konsisten.

### Implementasi

```sql
-- UNIQUE constraint di PostgreSQL
CONSTRAINT uq_topic_event UNIQUE (topic, event_id)

-- INSERT atomik dengan penanganan konflik
INSERT INTO processed_events (topic, event_id, source, timestamp, payload)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT uq_topic_event DO NOTHING;
```

### Race Condition Safety

Dua worker yang proses event yang sama **secara bersamaan**:
- **Worker A**: `INSERT` → sukses (baris baru dibuat)
- **Worker B**: `INSERT` → `ON CONFLICT DO NOTHING` (constraint melindungi)

PostgreSQL menjamin atomisitas operasi ini di level storage engine, sehingga **tidak mungkin** terjadi double-insert meskipun concurrent.

---

## Referensi

- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.
- FastAPI Documentation: https://fastapi.tiangolo.com
- asyncpg Documentation: https://magicstack.github.io/asyncpg
- Redis Documentation: https://redis.io/docs
- k6 Load Testing: https://k6.io/docs
