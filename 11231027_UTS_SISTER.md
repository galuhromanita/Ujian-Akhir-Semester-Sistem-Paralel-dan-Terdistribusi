# LAPORAN UJIAN AKHIR SEMESTER
## SISTEM TERDISTRIBUSI

### Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi

---

## IDENTITAS

| Atribut            | Keterangan                                                                |
|--------------------|---------------------------------------------------------------------------|
| **Nama**           | Galuh Juliviana Romanita                                                  |
| **NIM**            | 11231027                                                                  |
| **Mata Kuliah**    | Sistem Terdistribusi                                                      |
| **Jenis Ujian**    | Ujian Akhir Semester (UAS)                                                |
| **Tema Proyek**    | Pub-Sub Log Aggregator Terdistribusi                                      |
| **Bahasa**         | Python 3.11                                                               |
| **Stack Teknologi**| FastAPI · Redis 7 · PostgreSQL 16 · Docker Compose v2                     |

---

## 1. Ringkasan Sistem dan Arsitektur

Sistem yang dibangun adalah **Pub-Sub Log Aggregator** multi-service yang berjalan sepenuhnya di dalam jaringan Docker Compose internal yang terisolasi. Sistem terdiri dari empat layanan utama:

- `aggregator` — API FastAPI dengan tiga consumer worker asinkron
- `publisher` — simulator event dengan duplikasi intentional 30%
- `broker` — Redis 7 sebagai message queue dengan persistensi AOF
- `storage` — PostgreSQL 16 sebagai deduplication store dan penyimpanan persisten

**Alur Kerja Utama:**

```
Publisher → POST /publish → Aggregator (FastAPI) → LPUSH → Redis Queue
                                                               ↓ BRPOP
                                              Consumer Worker 1, 2, 3
                                                               ↓ INSERT ON CONFLICT DO NOTHING
                                                         PostgreSQL 16
                                                    (processed_events, event_stats, audit_log)
```

Publisher mengirim event (termasuk duplikat intentional) ke endpoint `POST /publish` pada aggregator. Aggregator memvalidasi skema event menggunakan model Pydantic, kemudian mendorong event ke antrian Redis menggunakan operasi `LPUSH`. Tiga consumer worker yang berjalan secara konkuren melakukan `BRPOP` dari Redis, lalu memproses setiap event dalam **satu transaksi PostgreSQL atomik** menggunakan `INSERT ... ON CONFLICT DO NOTHING` dengan constraint `UNIQUE(topic, event_id)`.

Mekanisme ini menjamin bahwa event yang sama hanya diproses satu kali meskipun diterima berulang kali (*idempotency*) dan meskipun beberapa worker memproses secara bersamaan (*concurrency-safe*). Invariant statistik yang selalu dijaga adalah:

> **received = unique\_processed + duplicate\_dropped**

---

## 2. Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator

Sistem terdistribusi didefinisikan sebagai sekumpulan komputer independen yang tampak bagi pengguna sebagai satu sistem yang kohesif (Coulouris, Dollimore, Kindberg, & Blair, 2012). Sistem Pub-Sub Log Aggregator yang dibangun mencerminkan empat karakteristik utama sistem terdistribusi secara nyata.

**Pertama, concurrency:** tiga consumer worker berjalan paralel sebagai `asyncio.Task` dalam satu proses Python, masing-masing mengambil dan memproses event dari antrian Redis secara bersamaan tanpa koordinasi eksplisit antar worker.

**Kedua, tidak ada jam global (*no global clock*):** event dikirim dari berbagai sumber dengan timestamp masing-masing yang tidak terjamin sinkron; sistem menggunakan timestamp lokal sebagai panduan dan kolom `received_at` sebagai *tie-breaker* deterministik.

**Ketiga, kegagalan independen (*independent failures*):** container Redis atau PostgreSQL dapat crash dan di-restart oleh Docker Compose tanpa menghentikan keseluruhan sistem — publisher tetap dapat mengirim event yang akan menumpuk di antrian atau di-retry.

**Keempat, heterogenitas:** setiap service berjalan dalam image Docker terpisah dengan dependensi dan lingkungan runtime masing-masing.

Trade-off utama desain Pub-Sub adalah antara **kesederhanaan implementasi** dan **keandalan pengiriman**. Pola Pub-Sub memungkinkan *decoupling* antara publisher dan consumer — publisher tidak perlu mengetahui siapa atau berapa banyak consumer yang akan memproses event-nya (Coulouris et al., 2012). Namun, desentralisasi ini memperkenalkan kompleksitas dalam hal *ordering* dan *exactly-once semantics*. Sistem ini memilih **at-least-once delivery dengan idempotent consumer** sebagai kompromi pragmatis: lebih mudah diimplementasikan dibanding exactly-once murni, tetapi tetap menghasilkan state akhir yang konsisten dan dapat diverifikasi.

---

## 3. Kapan Memilih Arsitektur Publish-Subscribe Dibanding Client-Server?

Pemilihan antara arsitektur publish-subscribe dan client-server bergantung pada karakteristik kebutuhan komunikasi antar komponen (Coulouris et al., 2012). Arsitektur **publish-subscribe** lebih tepat dipilih dalam tiga situasi utama.

**Pertama**, ketika **banyak consumer independen** perlu menerima informasi yang sama tanpa publisher harus mengetahui identitas masing-masing consumer — ini disebut *space decoupling*.

**Kedua**, ketika **temporal decoupling** diperlukan: publisher dan consumer tidak harus aktif pada saat bersamaan; publisher dapat mengirim event saat consumer sedang offline dan consumer akan memproses event tersebut saat kembali aktif.

**Ketiga**, ketika **skalabilitas horizontal** adalah prioritas utama: menambah consumer baru tidak memerlukan perubahan konfigurasi pada sisi publisher sama sekali.

Sebaliknya, arsitektur **client-server** lebih tepat untuk interaksi sinkron satu-satu yang memerlukan respons langsung, seperti autentikasi pengguna, query basis data, atau layanan yang memerlukan *acknowledgment* segera dari penerima.

Dalam sistem ini, Pub-Sub dipilih karena tiga alasan teknis konkret:

1. Publisher simulator perlu mengirim ribuan event dengan throughput tinggi tanpa menunggu konfirmasi pemrosesan dari setiap consumer;
2. Pemisahan concern yang jelas antara pengumpulan data (publisher) dan pemrosesan serta agregasi (consumer workers) menjadikan sistem lebih modular dan mudah di-*scale*;
3. Redis sebagai broker memungkinkan *temporal decoupling* — jika aggregator sementara tidak responsif, event tetap tersimpan dalam antrian dan akan diproses saat aggregator pulih.

Jika menggunakan client-server biasa, publisher harus menunggu respons setiap event dan menjadi *bottleneck* kritis saat volume tinggi.

---

## 4. At-Least-Once vs. Exactly-Once Delivery; Peran Idempotent Consumer

**At-least-once delivery** menjamin bahwa setiap pesan akan dikirimkan minimal satu kali kepada consumer, tetapi memungkinkan pengiriman lebih dari satu kali akibat retry, timeout koneksi, atau kegagalan jaringan sementara. **Exactly-once delivery** menjamin setiap pesan diproses tepat satu kali, yang secara teori lebih ideal namun secara praktik memerlukan koordinasi dua fase (*two-phase commit* / 2PC) atau mekanisme transaksional terdistribusi yang sangat mahal secara performa dan kompleksitas (Coulouris et al., 2012).

Sistem ini secara eksplisit menggunakan **at-least-once delivery**: publisher simulator menggunakan exponential backoff retry dan diperbolehkan mengirim event yang sama lebih dari sekali untuk mensimulasikan kondisi dunia nyata (restart setelah timeout, packet loss, dll.). Untuk mempertahankan konsistensi data meskipun event duplikat diterima, digunakan **idempotent consumer**: consumer dirancang sedemikian rupa sehingga memproses event yang sama berkali-kali menghasilkan efek yang identik dengan memprosesnya satu kali.

Implementasinya memanfaatkan operasi database yang atomik:

```sql
INSERT INTO processed_events (topic, event_id, source, timestamp, payload)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT ON CONSTRAINT uq_topic_event DO NOTHING;
```

Jika event yang sama dikirim ulang, database secara otomatis menolak insert duplikat tanpa error, counter `duplicate_dropped` bertambah satu, dan `unique_processed` tidak berubah. Hasilnya: *at-least-once* di level transport + *idempotent consumer* di level pemrosesan = *effectively exactly-once* di level state database.

---

## 5. Skema Penamaan Topic dan Event_ID; Uniqueness dan Collision Resistance

Penamaan dalam sistem terdistribusi harus memenuhi tiga syarat utama: **unik** secara global dalam namespace-nya, **konsisten** (mengikuti konvensi yang dapat diparsing secara efisien), dan **tidak bergantung pada koordinasi terpusat** untuk pembuatannya (Coulouris et al., 2012).

**Skema Topic** menggunakan notasi hierarki *dot-separated* seperti `sensor.temperature`, `system.cpu`, `app.login`. Notasi ini memiliki beberapa keunggulan teknis:

1. Memungkinkan *filtering*, *routing*, dan *grouping* yang semantik berdasarkan prefix;
2. Mudah diparsing dengan operasi string standar;
3. Dapat dikembangkan menjadi hierarki lebih dalam tanpa perubahan skema.

Validasi di level Pydantic model memastikan topic tidak mengandung spasi dan memiliki panjang antara 1–255 karakter. Constraint dedup beroperasi pada kombinasi `(topic, event_id)`, bukan hanya `event_id`, karena event_id yang sama bisa sah muncul di topic yang berbeda.

**Skema Event_ID** menggunakan format `evt-{uuid4}` di mana UUID v4 memiliki 122 bit entropi acak, memberikan probabilitas collision yang mendekati nol (1 dari 2^122 ≈ 5.3 × 10^-36 per pasangan). UUID v4 dipilih karena dapat di-generate secara lokal oleh setiap node tanpa koordinasi terpusat (krusial untuk sistem terdistribusi), tidak dapat diprediksi urutannya (keamanan), dan didukung natively oleh semua bahasa pemrograman modern.

Constraint `UNIQUE(topic, event_id)` di PostgreSQL menjadi **durable dedup store** yang collision-resistant: meskipun dua worker mengirim event yang sama secara bersamaan, PostgreSQL menjamin hanya satu insert yang berhasil melalui implicit row-level locking pada constraint index.

---

## 6. Ordering Praktis: Timestamp + Monotonic Ordering; Batasan dan Dampaknya

Dalam sistem terdistribusi, tidak ada "waktu global" yang dapat diandalkan — setiap node memiliki jam lokal yang dapat mengalami *clock drift* atau *clock skew* relatif terhadap node lain (Coulouris et al., 2012). Event dari sumber berbeda mungkin tiba *out-of-order* akibat perbedaan jam mesin, latensi jaringan yang tidak deterministik, atau mekanisme retry.

Sistem ini mengadopsi pendekatan **ordering praktis dua lapis**:

1. **Timestamp ISO8601** dari sumber event digunakan sebagai panduan urutan logis dan divalidasi ketat di level Pydantic sebelum masuk ke antrian.
2. **Kolom `received_at`** (waktu event diterima aggregator, diset oleh database) digunakan sebagai *tie-breaker* monotonis yang deterministik — endpoint `GET /events` mengembalikan event berdasarkan `received_at DESC`, bukan `timestamp`, sehingga urutan tampilan konsisten dengan urutan pemrosesan aktual.

**Batasan teknis yang diterima:**

- Event dapat tiba *out-of-order* — event dengan timestamp lebih awal mungkin diproses belakangan jika tertunda di jaringan atau antrian.
- *Clock skew* antar node publisher dapat menyebabkan anomali urutan logis.
- Tidak ada *total ordering* yang terjamin secara global.

**Dampak pada sistem:** Karena sistem ini adalah **log aggregator** (bukan sistem pemesanan finansial atau koordinasi kritis yang memerlukan total ordering), *out-of-order* events dapat ditoleransi dengan aman. Statistik agregat `received`, `unique_processed`, dan `duplicate_dropped` tidak bergantung pada urutan pemrosesan — hanya bergantung pada identitas event. Untuk use case yang memerlukan total ordering sejati, mekanisme *Lamport timestamps* atau *vector clocks* perlu diimplementasikan sebagai overhead tambahan.

---

## 7. Failure Modes dan Mitigasi: Retry, Backoff, Durable Dedup Store, Crash Recovery

Sistem terdistribusi harus dirancang dengan asumsi bahwa kegagalan adalah hal yang **normal dan pasti terjadi**, bukan pengecualian (Coulouris et al., 2012). Berikut adalah analisis *failure modes* dan mitigasi yang diimplementasikan:

| Mode Kegagalan | Contoh Skenario | Mitigasi yang Diimplementasikan |
|---|---|---|
| **Network partition** | Publisher tidak dapat menjangkau aggregator | Exponential backoff retry (1s → 2s → 4s); at-least-once memastikan event tidak hilang permanen |
| **Container crash — aggregator** | OOM atau exception tidak tertangani | `restart: unless-stopped` di Docker Compose; event sudah di Redis tidak hilang; consumer langsung melanjutkan setelah restart |
| **Container crash — storage** | PostgreSQL OOM atau disk penuh | Named volume `uas_pg_data` memastikan data WAL tidak hilang; `depends_on: condition: service_healthy` memastikan aggregator tidak memulai sebelum PostgreSQL siap |
| **Container crash — broker** | Redis restart | Named volume `uas_broker_data` + `appendonly yes` (AOF) memastikan antrian tidak hilang; `appendfsync everysec` menjamin maksimum kehilangan 1 detik data |
| **Duplikat event (akibat retry)** | Publisher mengirim ulang event setelah timeout | Idempotent consumer + `UNIQUE(topic, event_id)` + `ON CONFLICT DO NOTHING` di PostgreSQL |
| **Queue overflow** | Publisher mengirim lebih cepat dari consumer memproses | Circuit breaker: jika panjang antrian Redis > 100.000 item, aggregator mengembalikan HTTP 503 |
| **Worker error saat processing** | DB tidak responsif sementara | `await asyncio.sleep(0.5)` + retry loop; error di-log dengan full traceback |

**Crash recovery yang terjamin:** Setelah container aggregator restart, *durable dedup store* (UNIQUE constraint di PostgreSQL pada named volume) tetap ada dan utuh. Publisher yang mengirim ulang event setelah restart akan tetap ditolak sebagai duplikat — **tidak ada state yang hilang**. Shutdown graceful diimplementasikan melalui `signal_shutdown()` yang menunggu worker menyelesaikan event yang sedang diproses sebelum berhenti.

---

## 8. Eventual Consistency pada Aggregator; Peran Idempotency dan Deduplication

**Eventual consistency** mendefinisikan model konsistensi di mana sistem menjamin bahwa, jika tidak ada update baru yang masuk, semua replika dan komponen penyimpanan pada akhirnya akan konvergen ke nilai yang sama (Coulouris et al., 2012). Model ini diterima sebagai trade-off antara ketersediaan (*availability*) dan konsistensi kuat (*strong consistency*) sesuai teorema CAP.

Dalam konteks sistem ini, eventual consistency termanifestasi pada jeda antara event diterima di endpoint `/publish` (disimpan ke antrian Redis) dan event muncul di `/events` atau `/stats` (setelah diproses consumer dari antrian). Jeda ini adalah konsekuensi alami dari arsitektur antrian asinkron. Client tidak dapat mengasumsikan bahwa event yang baru dipublikasikan akan langsung terlihat di query berikutnya — diperlukan polling atau waktu tunggu singkat.

**Peran Idempotency:** Idempotency adalah prasyarat *sine qua non* untuk eventual consistency yang aman. Tanpa idempotency, retry dan duplikat yang inherent dalam at-least-once delivery akan terus-menerus mengubah state, mencegah konvergensi. Implementasi `ON CONFLICT DO NOTHING` menjamin bahwa pemrosesan event yang sama N kali menghasilkan state yang identik dengan memprosesnya satu kali.

**Peran Deduplication:** Dedup store berbasis `UNIQUE(topic, event_id)` di PostgreSQL menjamin **kausal consistency** pada statistik agregat: nilai `unique_processed` mencerminkan jumlah event unik yang benar-benar diproses, bukan jumlah total pengiriman. Invariant yang dijaga secara eksplisit dan diuji oleh `test_stats_invariant_received_equals_sum`:

```
received = unique_processed + duplicate_dropped
```

Jika invariant ini rusak, ini mengindikasikan *race condition* atau *lost-update* di logika update statistik — kondisi yang justru dicegah oleh pola *upsert atomik* yang diimplementasikan.

---

## 9. Desain Transaksi: ACID, Isolation Level, dan Strategi Menghindari Lost-Update ⭐

> **Catatan:** Bagian ini merupakan inti teknis sistem. Setiap aspek transaksi dan kontrol konkurensi dijelaskan dengan contoh langsung dari implementasi.

Sistem ini menggunakan **transaksi PostgreSQL eksplisit** untuk menjamin konsistensi data saat tiga consumer worker memproses event secara konkuren (Coulouris et al., 2012). Setiap event diproses dalam satu transaksi atomik yang mencakup tiga operasi: INSERT ke `processed_events`, UPDATE ke `event_stats`, dan INSERT ke `audit_log`.

### 9.1 ACID dalam Implementasi Nyata

| Properti ACID | Implementasi dalam Sistem |
|---|---|
| **Atomicity** | Tiga operasi DB (INSERT processed_events + UPDATE event_stats + INSERT audit_log) dibungkus dalam satu `async with conn.transaction(isolation="read_committed")`. Jika salah satu gagal, ketiganya di-rollback otomatis. |
| **Consistency** | UNIQUE constraint `uq_topic_event` memastikan tidak ada dua baris dengan `(topic, event_id)` yang sama. CHECK constraint memastikan kolom `action` hanya berisi `'processed'` atau `'duplicate_dropped'`. |
| **Isolation** | READ COMMITTED — setiap statement dalam transaksi hanya melihat data yang sudah di-commit oleh transaksi lain, mencegah *dirty reads*. |
| **Durability** | Named volume `uas_pg_data` + PostgreSQL WAL (*Write-Ahead Logging*) memastikan data tidak hilang meski terjadi crash setelah commit. |

### 9.2 Isolation Level READ COMMITTED — Alasan Pemilihan

Ketika Worker A dan Worker B memproses event yang sama secara bersamaan:

1. Worker A: `INSERT INTO processed_events ...` → **sukses**, PostgreSQL membuat baris baru
2. Worker B: `INSERT INTO processed_events ...` → **PostgreSQL mengacquire implicit row lock pada constraint index** → menunggu Worker A commit
3. Worker A commit → Worker B mendapat sinyal konflik → `ON CONFLICT DO NOTHING` dieksekusi tanpa error

READ COMMITTED cukup karena *conflict resolution* sudah di-handle di level UNIQUE constraint — bukan di level transaksi itu sendiri. SERIALIZABLE tidak diperlukan dan akan menambah overhead signifikan melalui *predicate locking* dan *snapshot isolation* penuh.

### 9.3 Strategi Menghindari Lost-Update pada Statistik

Pola `SET count = count + 1` yang dieksekusi dalam transaksi yang sama:

```sql
-- Upsert atomik: menghindari read-then-write race condition
INSERT INTO event_stats (topic, received, unique_processed, duplicate_dropped, last_updated)
VALUES ($1, 1, 1, 0, NOW())
ON CONFLICT (topic) DO UPDATE SET
    received         = event_stats.received + 1,
    unique_processed = event_stats.unique_processed + 1,
    last_updated     = NOW();
```

Operasi `event_stats.received + 1` dilakukan **dalam satu statement SQL atomik** — bukan `SELECT` kemudian `UPDATE` yang rawan *Time-Of-Check-To-Time-Of-Use (TOCTOU) race condition*. PostgreSQL mengeksekusi keduanya (INSERT atau UPDATE) sebagai satu operasi indivisible, menjamin tidak ada lost-update meskipun puluhan worker mengupdate baris statistik yang sama secara bersamaan.

---

## 10. Kontrol Konkurensi: Locking, Unique Constraints, Upsert; Idempotent Write Pattern ⭐

Kontrol konkurensi adalah mekanisme untuk memastikan eksekusi transaksi concurrent menghasilkan hasil yang setara (*serializable equivalent*) dengan eksekusi serial — mencegah anomali seperti *lost update*, *dirty read*, dan *phantom read* (Coulouris et al., 2012).

### 10.1 Implicit Row-Level Locking via Unique Constraint (Mekanisme Utama)

PostgreSQL menggunakan *speculative insertion* untuk UNIQUE constraints. Ketika dua INSERT concurrent dengan nilai kunci yang sama terjadi:

- Keduanya mencoba membuat entry di constraint index secara bersamaan
- Satu berhasil mem-*acquire lock* dan menulis baris baru
- Yang lain **menunggu** sampai yang pertama commit, lalu mendapatkan sinyal konflik
- `ON CONFLICT DO NOTHING` menangani konflik tanpa error dan tanpa deadlock

Overhead mekanisme ini minimal dibanding explicit `SELECT FOR UPDATE` karena tidak memerlukan query tambahan.

### 10.2 Upsert Atomik untuk Update Statistik (Mencegah TOCTOU)

Pola anti-pattern yang **dihindari** (rawan race condition):

```python
# BURUK: Read-then-write — race condition antara SELECT dan UPDATE
current = await conn.fetchval("SELECT received FROM event_stats WHERE topic = $1", topic)
await conn.execute("UPDATE event_stats SET received = $1 WHERE topic = $2", current + 1, topic)
```

Pola idempotent yang **diimplementasikan** (atomik):

```sql
-- BAIK: Upsert atomik — satu round-trip, tidak ada window untuk race condition
INSERT INTO event_stats (topic, received, ...) VALUES ($1, 1, ...)
ON CONFLICT (topic) DO UPDATE SET
    received = event_stats.received + 1, ...;
```

### 10.3 Connection Pool Management (asyncpg)

Connection pool `asyncpg` dikonfigurasi dengan batas minimum 2 dan maksimum 20 koneksi. Setiap consumer worker meminjam koneksi dari pool saat dibutuhkan (`async with pool.acquire() as conn`) dan mengembalikannya setelah transaksi selesai — mencegah koneksi yang tidak terpakai membebani PostgreSQL dan mencegah *connection exhaustion* saat beban tinggi.

### 10.4 Idempotent Write Pattern — Ringkasan Prinsip

| Anti-Pattern | Pattern yang Digunakan |
|---|---|
| `SELECT + IF EXISTS + INSERT/UPDATE` | `INSERT ... ON CONFLICT DO NOTHING/UPDATE` |
| `UPDATE SET count = $1` (dari SELECT) | `UPDATE SET count = count + 1` (atomik) |
| Multiple round-trips per event | Satu transaksi, tiga operasi atomik |
| Explicit `LOCK TABLE` | Implicit constraint-level locking |

---

## 11. Orkestrasi Compose, Keamanan Jaringan, Persistensi Volume, dan Observabilitas

### 11.1 Orkestrasi Docker Compose

Docker Compose berfungsi sebagai *orchestrator* lokal yang mendefinisikan urutan startup, dependensi antar service, dan kebijakan restart (Coulouris et al., 2012). Konfigurasi `depends_on` dengan `condition: service_healthy` mengimplementasikan **readiness check** yang deterministik:

```yaml
aggregator:
  depends_on:
    storage:
      condition: service_healthy  # Tunggu pg_isready sukses
    broker:
      condition: service_healthy  # Tunggu redis-cli ping sukses
```

Ini memastikan aggregator tidak memulai inisialisasi koneksi database sebelum PostgreSQL dan Redis benar-benar siap menerima koneksi — mencegah *race condition startup* yang umum pada sistem multi-container. *Liveness check* tambahan diimplementasikan melalui endpoint `/health` yang diperiksa oleh Docker healthcheck aggregator setiap 10 detik.

### 11.2 Keamanan Jaringan Lokal

Semua service terhubung melalui satu jaringan Docker bridge bernama `uas_internal_network`:

```yaml
networks:
  internal:
    driver: bridge
    name: uas_internal_network
```

Hanya aggregator yang mengekspos port ke host (`8080:8080`). Redis (6379) dan PostgreSQL (5432) tidak memiliki `ports` yang dipublikasikan — secara teknis tidak dapat diakses dari luar jaringan Compose. **Isolasi jaringan ini** memastikan:

1. Tidak ada akses langsung ke database dari luar;
2. Tidak ada koneksi ke layanan eksternal publik;
3. Semua komunikasi antar service menggunakan DNS resolution internal Docker (`aggregator`, `broker`, `storage` sebagai hostname).

### 11.3 Persistensi dengan Named Volumes

```yaml
volumes:
  pg_data:
    name: uas_pg_data      # PostgreSQL: processed_events, event_stats, audit_log
  broker_data:
    name: uas_broker_data  # Redis AOF: state antrian event
```

Named volumes tidak terhapus saat `docker compose down` tanpa flag `-v`, sehingga data bertahan meskipun container dihapus dan dibuat ulang. Redis dikonfigurasi dengan `--appendonly yes --appendfsync everysec` untuk persistensi AOF dengan maksimum kehilangan data 1 detik.

### 11.4 Observabilitas

| Mekanisme | Endpoint / Implementasi | Informasi yang Disediakan |
|---|---|---|
| **Real-time stats** | `GET /stats` | received, unique_processed, duplicate_dropped, per-topic breakdown, queue_length |
| **Health check** | `GET /health` | Status DB, Redis, jumlah worker aktif, uptime |
| **Audit log** | Tabel `audit_log` di PostgreSQL | Setiap keputusan pemrosesan: processed/duplicate_dropped, worker_id, waktu |
| **Structured logging** | `logging` Python dengan level DEBUG/INFO/ERROR | Setiap event yang diproses/diduplikat dicatat dengan context lengkap |

---

## 12. Keputusan Desain Utama

| Keputusan | Pilihan yang Diambil | Justifikasi Teknis |
|---|---|---|
| **Delivery semantics** | At-least-once + idempotent consumer | Lebih pragmatis dari exactly-once; hasil akhir setara dengan overhead minimal |
| **Dedup mechanism** | `UNIQUE(topic, event_id)` + `ON CONFLICT DO NOTHING` | Atomik di level database; tidak perlu lock eksplisit; durable melewati restart |
| **Isolation level** | READ COMMITTED | Cukup untuk skenario ini; SERIALIZABLE berlebihan dan menambah overhead predicate locking |
| **Stats update** | `INSERT ... ON CONFLICT DO UPDATE SET count = count + 1` | Mencegah TOCTOU race condition; satu round-trip atomik |
| **Consumer architecture** | 3 `asyncio.Task` dalam 1 process | Sederhana, efisien I/O-bound; mudah di-scale horizontal dengan `WORKER_COUNT` env var |
| **Broker** | Redis 7 dengan `BRPOP` | Non-blocking, AOF persistence, latensi rendah, ringan secara resource |
| **Payload storage** | PostgreSQL JSONB | Indexable, queryable, validasi skema di level DB, flexible schema |
| **Audit trail** | Tabel `audit_log` terpisah | Tidak membebani tabel `processed_events` saat query; dapat di-query per worker atau per action |
| **Skema event_id** | Format `evt-{UUID v4}` | 122 bit entropi; tidak memerlukan koordinasi terpusat; collision-resistant |
| **Topic naming** | Dot-separated hierarki (`sensor.temperature`) | Semantik jelas; mudah di-filter dengan prefix matching; extensible |

---

## 13. Analisis Performa dan Metrik

### 13.1 Konfigurasi Sistem

| Parameter | Nilai |
|---|---|
| Jumlah consumer workers | 3 (async tasks) |
| Connection pool PostgreSQL | min=2, max=20 |
| Redis BRPOP timeout | 2 detik |
| Queue overflow threshold | 100.000 item |
| Publisher duplicate ratio | 30% (konfigurasi default) |
| Publisher event count | 1.000 (konfigurasi default, scalable via env) |

### 13.2 Estimasi Throughput

Berdasarkan konfigurasi sistem pada hardware mid-range (Intel Core i5, 16GB RAM):

| Operasi | Estimasi Throughput |
|---|---|
| **Ingestion** (`POST /publish`) | ~500–1.000 request/detik (async, tidak menunggu processing) |
| **Processing** (consumer → PostgreSQL) | ~200–500 event/detik per worker |
| **Total processing** (3 workers) | ~600–1.500 event/detik |
| **Dedup overhead** | Minimal — `INSERT ON CONFLICT` adalah single round-trip |

Target: ≥ 20.000 event (dengan ≥ 30% duplikat) dapat diproses dalam waktu ≤ 40 detik dengan konfigurasi default.

### 13.3 Hasil Uji Konkurensi

#### Uji `test_concurrency_parallel_duplicate_event_ids`

Skenario: 20 HTTP request paralel dikirim dengan `event_id` yang **identik** ke endpoint `/publish`. Setelah antrian diproses:

| Metrik | Hasil yang Diharapkan | Hasil Aktual |
|---|---|---|
| `unique_processed` bertambah | +1 (bukan +20) | ✅ +1 |
| `duplicate_dropped` bertambah | +19 | ✅ +19 |
| Error HTTP 5xx | 0 | ✅ 0 |
| Data corruption / double-insert | Tidak ada | ✅ Tidak ada |

Hasil ini membuktikan bahwa kombinasi UNIQUE constraint + `ON CONFLICT DO NOTHING` + READ COMMITTED isolation bekerja sebagai mekanisme kontrol konkurensi yang benar dan aman di bawah tekanan paralel.

#### Uji `test_stats_invariant_received_equals_sum`

Invariant `received = unique_processed + duplicate_dropped` diverifikasi baik secara global maupun per-topic setelah setiap sesi pengujian. Invariant ini tidak pernah rusak dalam semua skenario pengujian, membuktikan bahwa upsert atomik pada `event_stats` bebas dari lost-update meskipun diakses oleh banyak worker secara bersamaan.

---

## 14. Link Demonstrasi Video

| **YouTube** | https://youtu.be/NgdjYdJoCs8?si=q57-L5CGlBglALy1 |

## 15. Daftar Pustaka

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.

FastAPI. (2024). *FastAPI framework, high performance, easy to learn, fast to code, ready for production*. Diakses dari https://fastapi.tiangolo.com

PostgreSQL Global Development Group. (2024). *PostgreSQL 16 documentation: ON CONFLICT clause*. Diakses dari https://www.postgresql.org/docs/16/sql-insert.html

Redis Ltd. (2024). *Redis documentation: BRPOP command and append-only file persistence*. Diakses dari https://redis.io/docs

Python Software Foundation. (2024). *asyncio — Asynchronous I/O*. Diakses dari https://docs.python.org/3/library/asyncio.html

MagicStack. (2024). *asyncpg: A fast PostgreSQL database client library for Python/asyncio*. Diakses dari https://magicstack.github.io/asyncpg/current/

Docker Inc. (2024). *Docker Compose: Define and run multi-container applications with Docker*. Diakses dari https://docs.docker.com/compose/
