# Real-Time Food Delivery Stream Analytics

## Project Overview

This repository contains the full stream analytics pipeline for a simulated food delivery platform (similar to Uber Eats / Glovo). Synthetic events flow from a Python producer into Azure Event Hubs, are processed by three Spark Structured Streaming notebooks running on Google Colab, land as Parquet in Azure Blob Storage, and feed a live Streamlit dashboard.

The project covers the full path: synthetic data generation, event-time stream processing with watermarks, stream-stream joins, latency tuning, and operational lessons from running multiple stateful streams on a constrained runtime.

**Deliverables covered:**
- **Milestone 1** — feed design, AVRO schemas, synthetic data generator with configurable edge cases.
- **Milestone 2** — three Spark Structured Streaming notebooks producing 15 windowed KPIs, a cross-stream batch/streaming join, Parquet data at rest in Azure Blob, and a live Streamlit dashboard.

---

## Repository Structure

```
.
├── schemas/
│   ├── order_events.avsc              # AVRO schema — order lifecycle events
│   └── courier_status_events.avsc     # AVRO schema — courier status/location events
├── generator/
│   ├── generator.py                   # Synthetic event generator (Milestone 1)
│   └── requirements.txt               # Python dependencies (generator + dashboard)
├── notebooks/
│   ├── Food_order_notebook_final.ipynb       # Orders stream → 7 KPIs → Blob
│   ├── Courier_notebook_final.ipynb          # Couriers stream → 6 KPIs → Blob
│   └── Cross_stream_KPI_notebook_final.ipynb # Supply/demand + pickup wait (cross-stream)
├── sample_data/
│   ├── order_events_sample.json
│   ├── order_events_sample.avro
│   ├── courier_status_sample.json
│   └── courier_status_sample.avro
├── streamlit_dashboard.py             # Live dashboard reading Parquet from Blob
└── README.md
```

---

## Architecture

```
   ┌───────────────────┐        ┌──────────────────┐        ┌────────────────────────┐
   │  Python producer  │ ─────► │ Azure Event Hubs │ ─────► │ Spark Structured       │
   │  (synthetic AVRO) │        │ (Kafka protocol) │        │ Streaming (Colab)      │
   └───────────────────┘        └──────────────────┘        └────────────┬───────────┘
                                                                         │
                                                             ┌───────────┴──────────┐
                                                             │                      │
                                                    ┌────────▼────────┐   ┌─────────▼────────┐
                                                    │ Azure Blob      │   │ Streamlit        │
                                                    │ (Parquet, by    │   │ dashboard        │
                                                    │  zone / date)   │   │ (reads Parquet)  │
                                                    └─────────────────┘   └──────────────────┘
```

Three notebooks run in parallel:

1. **Food_order_notebook** — reads the orders feed, produces 7 windowed KPIs, writes to Blob.
2. **Courier_notebook** — reads the courier feed, produces 6 windowed KPIs, writes to Blob.
3. **Cross_stream_KPI_notebook** — produces two cross-stream KPIs: supply/demand imbalance (batch join of the two Parquet outputs) and pickup wait per order (streaming self-join on the orders feed).

---

## Feed Design

### Why These Two Feeds?

A food delivery platform has two core real-time concerns:

1. **Order lifecycle** — Is demand growing? Are restaurants keeping up? Are deliveries on time?
2. **Courier supply** — Are couriers available where orders are? Are sessions healthy?

Together, these feeds support **supply-demand balance analytics**, **SLA monitoring**, **anomaly detection**, and **fraud heuristics**.

### Feed 1: Order events (`order_events`)

Tracks the full lifecycle of every customer order from placement to delivery (or cancellation/refund).

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | string (UUID) | Unique event identifier |
| `order_id` | string (UUID) | Join key across lifecycle steps |
| `event_type` | enum | Lifecycle stage |
| `event_timestamp` | long (millis) | Event time — used for watermarks |
| `ingestion_timestamp` | long (millis) | Processing time — differs for late events |
| `restaurant_id` | string | Join key for restaurant reference data |
| `courier_id` | string (nullable) | Join key for courier events |
| `zone_id` | string | Zone-level aggregation key |
| `actual_prep_time_sec` | int (nullable) | Populated at READY_FOR_PICKUP — powers SLA KPIs |
| `total_amount` | double | Basket value (EUR) |
| `is_duplicate` | boolean | Flags duplicate events for deduplication logic |
| `schema_version` | string | Forward-compatibility tracking |

**Supported event types:**

```
ORDER_PLACED → ORDER_ACCEPTED → PREPARATION_STARTED → READY_FOR_PICKUP
→ PICKED_UP → OUT_FOR_DELIVERY → DELIVERED
                                               ↘ CANCELLED (any stage)
                                               ↘ REFUND_REQUESTED (post-delivery)
```

### Feed 2: Courier status events (`courier_status_events`)

Tracks real-time courier availability, GPS location, and delivery activity. Each event represents a state transition (ONLINE, ASSIGNED, EN_ROUTE, etc.).

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | string (UUID) | Unique event identifier |
| `courier_id` | string | Join key with order events |
| `order_id` | string (nullable) | Active order ID — null when idle/offline |
| `event_type` | enum | Current courier activity |
| `event_timestamp` | long (millis) | Event time for watermarking |
| `zone_id` | string | Current courier zone |
| `latitude`, `longitude` | double | GPS position for mapping |
| `session_id` | string | Groups events in one online session |
| `battery_level` | int (nullable) | Device battery — predictive offline indicator |
| `vehicle_type` | enum | Bike, scooter, car, etc. |
| `went_offline_mid_delivery` | boolean | Edge case flag |

**Supported event types:**

```
ONLINE → IDLE → ASSIGNED → EN_ROUTE_TO_RESTAURANT → ARRIVED_AT_RESTAURANT
       → EN_ROUTE_TO_CUSTOMER → ARRIVED_AT_CUSTOMER → IDLE (loop)
       → BREAK → OFFLINE
```

---

## Event-Time Processing Design

Both feeds include two timestamps:

- `event_timestamp` — when the event **actually happened** (event time)
- `ingestion_timestamp` — when the event **arrived at the broker** (processing time)

This separation is essential for:

1. **Watermark configuration** in Spark Structured Streaming (based on `event_timestamp`)
2. **Late data handling** — events with `ingestion_timestamp > event_timestamp + delay` are treated as late arrivals
3. **Correctness of windowed aggregations** regardless of network delays

---

## Edge Cases the Generator Produces

The generator deliberately injects realistic data-quality problems so the streaming queries can be validated against them.

| Edge case | Description | Parameter |
|-----------|-------------|-----------|
| Out-of-order events | `ingestion_timestamp` earlier than `event_timestamp` | `--late-event-prob` |
| Duplicate events | Same logical event emitted twice with different `event_id` | `--duplicate-prob` |
| Missing steps | `PICKED_UP` step skipped (order goes directly to `OUT_FOR_DELIVERY`) | `--missing-step-prob` |
| Impossible durations | Prep time of 1–30 seconds (anomaly detection target) | `--impossible-duration-prob` |
| Courier offline mid-delivery | Courier sends OFFLINE while an order is assigned | `--offline-mid-delivery-prob` |

These feed directly into the watermark choices: a watermark too tight will drop too many late events; one too loose will delay every downstream emission. The 8% late-event rate from the generator sits comfortably inside our 1-minute watermark.

---

## Realism Features

| Feature | Implementation |
|---------|---------------|
| Lunch/dinner peaks | `demand_weight_for_hour()` — hour-based demand multiplier |
| Weekday vs weekend | Different peak hours and distributions |
| Zone-level demand skew | `zone_centre` has 35% of demand vs 15% for outer zones |
| Configurability | All parameters exposed via CLI flags |
| Surge zones | `--surge-zones` flag marks specific zones as overloaded |
| Promo period | `--promo-period` flag marks events during promotional windows |

---

## KPIs Computed

### From the orders feed (Food_order_notebook)

| KPI | Window | Purpose |
|-----|--------|---------|
| Orders per minute by zone | 1 min | Live demand signal |
| Orders demand by zone | 1 min | Feeds supply/demand cross-stream KPI |
| Impossible prep time anomalies | streaming filter (no window) | Fraud/data-quality alerts |
| Avg prep time by restaurant & zone | 2 min | SLA monitoring |
| Cancel/refund hotspots | 2 min | Operational issues |
| Conversion funnel by zone | 2 min | Placed → accepted → delivered rates |
| Avg order value by zone | 2 min | Revenue per zone |

### From the courier feed (Courier_notebook)

| KPI | Window | Purpose |
|-----|--------|---------|
| Available couriers by zone | 1 min | Feeds supply/demand cross-stream KPI |
| Active delivery load by zone | 1 min | Live system load |
| Courier drop-off hotspots | 2 min | BREAK/OFFLINE clusters |
| Idle time by vehicle type | 2 min | Fleet utilization |
| Courier utilization rate | 2 min | Active vs idle time |
| Courier throughput per hour | 1 hour | Deliveries completed |

### Cross-stream KPIs (Cross_stream_KPI_notebook)

| KPI | Type | Purpose |
|-----|------|---------|
| Supply/demand imbalance by zone | Batch join of two Parquet outputs | `available_couriers` vs `orders_demand` per window |
| Pickup wait per order | Streaming self-join | Seconds between `READY_FOR_PICKUP` and `PICKED_UP` |

---

## Stream Processing Configuration

### Watermarks and windows

Both streams use a **1-minute watermark** on `event_time`. Aggregation windows are 1 or 2 minutes for most KPIs (with the exception of courier throughput at 1 hour).

Append-mode windowed aggregations only emit a window once `max_event_time − watermark ≥ window_end`, so this combination means the first row for a given window lands in Blob roughly 2–3 minutes after the first event of that window.

### Trigger intervals

- **Light queries** (1-minute windows, simple counts): `trigger(processingTime="20 seconds")`
- **Heavier queries** (2-minute windows, multi-column aggregations, joins): `trigger(processingTime="30 seconds")`

Triggers shorter than ~15 seconds cause more Colab overhead than they save.

### Output mode and partitioning

All queries use `outputMode("append")` and write Parquet partitioned by `zone_id` (and `event_date` for anomalies and pickup wait). Append mode means late rows that arrive after the watermark are dropped — this is an intentional tradeoff documented in "Settings Summary" below.

### Cross-stream join

The pickup-wait KPI is a **stream-stream self-join** on the orders feed with a 10-minute time bound between `READY_FOR_PICKUP` and `PICKED_UP`. Both sides of the join are watermarked; state is cleared once both watermarks and the time bound are exceeded.

---

## Operational Lessons

Three problems surfaced during load testing on Colab free tier. All three are addressed in the current notebooks.

### Queries died silently

**Symptom:** the monitor cell showed `numInputRows > 0` for every query, but some Blob folders stayed empty and those queries quietly disappeared from the active list after a few minutes.

**Cause:** the Spark driver was OOM-killed. Colab free's default driver memory (~1 GB) could not hold eight stateful streaming queries plus the AVRO producer. The monitor only printed `lastProgress`, so it never surfaced the crash.

**Fix:** increased `spark.driver.memory` to 6 GB, cut `spark.sql.shuffle.partitions` to 2 (Colab has only 2 cores — the default 200 was 99% scheduler overhead), and rewrote the monitor to call `handle.exception()` every tick so dead queries now print explicitly.

### Cross-stream join ran before upstream data existed

**Symptom:** the cross-stream batch join failed on first run. Spark could not infer a schema because `orders_demand_zone/` was empty.

**Cause:** not a bug in the join. Append-mode windowed streams only write their first file once the watermark passes the first window end. With a 1-minute window and a 1-minute watermark, the first Parquet file lands 2–3 minutes after the very first event — and the batch join was running before that.

**Fix:** added a `wait_for_parquet(path, timeout)` helper at the top of the cross-stream notebook. It polls the folder until real `part-*.parquet` files appear (or times out), so the batch join only runs once upstream data is actually on disk.

### Dashboard latency was ~10 minutes

**Symptom:** dashboard showed data ~10 minutes behind the producer.

**Cause:** three independent delays stacked. Old settings used a 2-minute watermark, 5-minute aggregation windows for most KPIs, and 30-second triggers.

**Fix:** reduced watermark to 1 minute, shrank 5-minute windows to 2 minutes, and tightened triggers to 20 seconds for light queries. The pickup-wait self-join time bound dropped from 30 minutes to 10 minutes, roughly tripling state-clearing speed.

Resulting end-to-end latency: ~3 minutes for windowed aggregates, 1.5–2 minutes for the stream-stream join.

### Scope reduction: 21 → 15 KPIs

Running 21 KPIs in parallel on Colab overwhelmed the driver (too many stateful queries, too much memory pressure, occasional silent deaths). We dropped the 6 most fragile ones (the ones most prone to NPEs on empty batches and commit-race collisions on the shared Blob container). Result: fewer stateful queries, half the collision surface, and a pipeline that stays alive for the full demo.

---

## Settings Summary

| Knob | Setting | Why we picked it | What we gave up |
|------|---------|------------------|-----------------|
| Driver memory | 6 GB | Holds stateful streams + producer on a 12 GB Colab VM | Less headroom for Python producer; drop to 4 GB if it OOMs |
| Shuffle partitions | 2 | Colab has 2 cores — default 200 is 99% scheduler overhead | Will not scale on a real cluster; bump back to 200+ in production |
| Watermark | 1 min | Short enough for low latency, long enough to catch most late events | Drops slightly more late events than a 2-min watermark |
| Trigger interval | 20–30 s | Blob-friendly (fewer small files), keeps CPU from thrashing | Dashboard refreshes in 20–30 s chunks, not sub-second |
| Output mode | append | Simplest, most replay-friendly Parquet format | No updates to closed windows; late rows dropped after the watermark |
| Consumer groups | One per notebook | Each notebook reads the full stream independently | Extra connections on Event Hub; watch the consumer-group ceiling |

---

## Schema Versioning

Both AVRO schemas include a `schema_version` field (default `"1.0"`). Schema evolution follows **AVRO schema registry patterns** — new optional fields use `["null", <type>]` unions with a `null` default to maintain backward compatibility.

---

## Setup & Usage

### Prerequisites

```bash
python3 -m pip install -r requirements.txt
```

### Generate sample data (Milestone 1, local)

```bash
# Default: 200 orders, 50 restaurants, 20 couriers
python3 generator.py

# Custom configuration
python3 generator.py \
  --orders 1000 \
  --restaurants 100 \
  --couriers 50 \
  --cancellation-prob 0.12 \
  --duplicate-prob 0.04 \
  --late-event-prob 0.10 \
  --promo-period \
  --surge-zones zone_centre zone_east \
  --output-dir ./sample_data

# Reproducible output
python3 generator.py --seed 123
```

### Run the notebooks (Milestone 2, streaming)

The notebooks are built for Google Colab.

1. Open each notebook in Colab.
2. Update **Cell 2** (`USER CONFIG`) with your Event Hub namespace, connection strings, and Blob account credentials.
3. If you are re-running after changing a watermark or window, set `WIPE_PATHS = True` in the optional cleanup cell, run it once, then flip it back. Spark rejects resumed checkpoints when the query plan changes.
4. **Order matters for the cross-stream notebook**: start the food-order notebook and the courier notebook first, wait until both have written at least one Parquet file, then run the cross-stream notebook.
5. Restart the runtime if you see OOM-killed behavior (empty Blob folders + queries vanishing from the monitor).

### Launch the dashboard

```bash
streamlit run streamlit_dashboard.py
```

The dashboard reads Parquet directly from Azure Blob via `abfs://` URLs and autorefreshes on a configurable interval.

### CLI reference (generator)

| Flag | Default | Description |
|------|---------|-------------|
| `--orders` | 200 | Number of orders to simulate |
| `--restaurants` | 50 | Number of restaurants |
| `--couriers` | 20 | Number of couriers |
| `--cancellation-prob` | 0.10 | Probability an order is cancelled |
| `--duplicate-prob` | 0.05 | Probability of injecting a duplicate event |
| `--late-event-prob` | 0.08 | Probability of a late-arriving event |
| `--missing-step-prob` | 0.05 | Probability of skipping PICKED_UP step |
| `--impossible-duration-prob` | 0.03 | Probability of anomalous prep time |
| `--promo-period` | false | Mark all events as during promo |
| `--surge-zones` | none | Zones experiencing demand surge |
| `--base-time` | today noon UTC | ISO8601 start time |
| `--output-dir` | ./sample_data | Output directory |
| `--seed` | 42 | Random seed for reproducibility |

---

## Assumptions

- Geographic area: Madrid, Spain (lat/lon bounding boxes per zone)
- Currency: EUR
- Time zone: UTC (all timestamps in UTC millis)
- Customer IDs are anonymised (no PII)
- Restaurant and courier IDs are synthetic
- Delivery distance is straight-line (Haversine), not routed
