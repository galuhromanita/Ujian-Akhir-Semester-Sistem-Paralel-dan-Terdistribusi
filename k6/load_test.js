/**
 * k6/load_test.js - Load Test Script menggunakan k6
 *
 * Tujuan:
 * 1. Membuktikan sistem mampu memproses ≥ 20.000 event (≥ 30% duplikat)
 * 2. Mengukur throughput, latency, dan duplicate rate
 * 3. Demo untuk video: menunjukkan performa di bawah beban nyata
 *
 * Cara menjalankan:
 *   # Install k6: https://k6.io/docs/getting-started/installation/
 *   k6 run k6/load_test.js
 *
 *   # Dengan output ke file:
 *   k6 run --out json=k6/results.json k6/load_test.js
 *
 *   # Via Docker:
 *   docker run --rm -i --network uas_internal_network grafana/k6 run - < k6/load_test.js
 *
 * Referensi: https://github.com/grafana/k6
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// ============================================================
// Konfigurasi Target
// ============================================================
const BASE_URL = __ENV.TARGET_URL || "http://localhost:8080";
const TOTAL_EVENTS = parseInt(__ENV.TOTAL_EVENTS || "20000");
const DUPLICATE_RATIO = parseFloat(__ENV.DUPLICATE_RATIO || "0.3");
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || "100");

// ============================================================
// Custom Metrics
// ============================================================
const eventsPublished = new Counter("events_published_total");
const duplicatesSent = new Counter("duplicates_sent_total");
const publishErrors = new Counter("publish_errors_total");
const publishLatency = new Trend("publish_latency_ms", true);
const successRate = new Rate("publish_success_rate");

// ============================================================
// Skenario Load Test
// ============================================================
export const options = {
  scenarios: {
    // Fase 1: Warm-up (1 menit, 10 VU)
    warmup: {
      executor: "constant-vus",
      vus: 10,
      duration: "1m",
      tags: { phase: "warmup" },
    },
    // Fase 2: Beban Penuh (3 menit, 50 VU) - mulai setelah warmup
    load: {
      executor: "constant-vus",
      vus: 50,
      duration: "3m",
      startTime: "1m",
      tags: { phase: "load" },
    },
    // Fase 3: Spike Test (30 detik, 100 VU) - mulai setelah load
    spike: {
      executor: "constant-vus",
      vus: 100,
      duration: "30s",
      startTime: "4m",
      tags: { phase: "spike" },
    },
  },
  thresholds: {
    // 95% request harus < 1 detik
    publish_latency_ms: ["p(95)<1000"],
    // Error rate < 5%
    publish_success_rate: ["rate>0.95"],
    // HTTP error rate < 1%
    http_req_failed: ["rate<0.01"],
  },
};

// ============================================================
// Pool Event ID (untuk mendaur ulang sebagai duplikat)
// ============================================================
const EVENT_ID_POOL = [];
const POOL_SIZE = 1000;

// Pre-generate pool event ID
for (let i = 0; i < POOL_SIZE; i++) {
  EVENT_ID_POOL.push(`pool-evt-${Math.random().toString(36).substr(2, 12)}`);
}

const TOPICS = [
  "sensor.temperature",
  "sensor.humidity",
  "system.cpu",
  "system.memory",
  "app.login",
  "app.logout",
  "network.packet",
  "db.query",
];

// ============================================================
// Fungsi Helper
// ============================================================

/**
 * Generate satu event JSON.
 * @param {boolean} isDuplicate - jika true, gunakan event_id dari pool
 */
function generateEvent(isDuplicate = false) {
  const topic = TOPICS[Math.floor(Math.random() * TOPICS.length)];

  let eventId;
  if (isDuplicate && EVENT_ID_POOL.length > 0) {
    // Ambil event_id dari pool (simulasi duplikat)
    eventId = EVENT_ID_POOL[Math.floor(Math.random() * EVENT_ID_POOL.length)];
  } else {
    // Buat event_id baru yang unik
    eventId = `k6-evt-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    // Tambahkan ke pool (maks POOL_SIZE)
    if (EVENT_ID_POOL.length < POOL_SIZE * 2) {
      EVENT_ID_POOL.push(eventId);
    }
  }

  return {
    topic: topic,
    event_id: eventId,
    timestamp: new Date().toISOString(),
    source: `k6-worker-${__VU}`,
    payload: {
      vu: __VU,
      iter: __ITER,
      value: Math.random() * 100,
      is_duplicate: isDuplicate,
    },
  };
}

/**
 * Generate batch event dengan rasio duplikat yang dikonfigurasi.
 */
function generateBatch(size = BATCH_SIZE) {
  const batch = [];
  let dupCount = 0;

  for (let i = 0; i < size; i++) {
    const isDuplicate = Math.random() < DUPLICATE_RATIO;
    if (isDuplicate) dupCount++;
    batch.push(generateEvent(isDuplicate));
  }

  return { batch, dupCount };
}

// ============================================================
// Skenario Utama
// ============================================================
export default function () {
  const { batch, dupCount } = generateBatch(BATCH_SIZE);

  const payload = JSON.stringify(batch);
  const params = {
    headers: { "Content-Type": "application/json" },
    timeout: "30s",
  };

  const startTime = Date.now();
  const response = http.post(`${BASE_URL}/publish`, payload, params);
  const duration = Date.now() - startTime;

  // Rekam metrik
  publishLatency.add(duration);
  eventsPublished.add(batch.length - dupCount);
  duplicatesSent.add(dupCount);

  const success = check(response, {
    "status adalah 200": (r) => r.status === 200,
    "response memiliki status queued": (r) => {
      try {
        return JSON.parse(r.body).status === "queued";
      } catch {
        return false;
      }
    },
  });

  successRate.add(success);
  if (!success) {
    publishErrors.add(1);
    console.error(`Batch gagal: HTTP ${response.status} - ${response.body.substring(0, 100)}`);
  }

  // Jeda kecil untuk tidak membanjiri antrian
  sleep(0.1);
}

// ============================================================
// Setup: Verifikasi aggregator siap
// ============================================================
export function setup() {
  console.log(`=== k6 Load Test UAS Pub-Sub Aggregator ===`);
  console.log(`Target URL  : ${BASE_URL}`);
  console.log(`Total Events: ${TOTAL_EVENTS}`);
  console.log(`Dup Ratio   : ${DUPLICATE_RATIO * 100}%`);
  console.log(`Batch Size  : ${BATCH_SIZE}`);
  console.log("==========================================");

  // Health check
  const healthResp = http.get(`${BASE_URL}/health`);
  if (healthResp.status !== 200) {
    throw new Error(`Aggregator tidak siap! Status: ${healthResp.status}`);
  }

  const health = JSON.parse(healthResp.body);
  console.log(`Aggregator status: ${health.status}`);
  console.log(`Workers aktif: ${health.workers_running}`);

  return { startTime: Date.now() };
}

// ============================================================
// Teardown: Tampilkan statistik akhir
// ============================================================
export function teardown(data) {
  const elapsed = (Date.now() - data.startTime) / 1000;

  // Ambil stats dari aggregator
  const statsResp = http.get(`${BASE_URL}/stats`);
  if (statsResp.status === 200) {
    const stats = JSON.parse(statsResp.body);
    console.log("\n=== Hasil Akhir Load Test ===");
    console.log(`Durasi test      : ${elapsed.toFixed(1)} detik`);
    console.log(`Total received   : ${stats.received}`);
    console.log(`Unique processed : ${stats.unique_processed}`);
    console.log(`Duplicate dropped: ${stats.duplicate_dropped}`);
    if (stats.received > 0) {
      const dupRate = ((stats.duplicate_dropped / stats.received) * 100).toFixed(1);
      console.log(`Duplicate rate   : ${dupRate}%`);
    }
    console.log(`Queue length     : ${stats.queue_length}`);
    console.log("==============================");
  }
}
