# Real-Time Food Delivery Stream Analytics — Milestone 1

## Project Overview

This repository contains the **Milestone 1** deliverables for the Stream Analytics Group Project.  
We design and implement two realistic streaming data feeds simulating the core operational dynamics of a food delivery platform (similar to Uber Eats / Glovo).

---

## Repository Structure

```
.
├── schemas/
│   ├── order_events.avsc              # AVRO schema — Order lifecycle events
│   └── courier_status_events.avsc     # AVRO schema — Courier status/location events
├── generator/
│   ├── generator.py                   # Main data generator
│   └── requirements.txt               # Python dependencies
├── sample_data/
│   ├── order_events_sample.json       # Sample order events (JSON)
│   ├── order_events_sample.avro       # Sample order events (AVRO)
│   ├── courier_status_sample.json     # Sample courier events (JSON)
│   └── courier_status_sample.avro     # Sample courier events (AVRO)
└── README.md
```

---

## Feed Design

### Why These Two Feeds?

A food delivery platform has two core real-time concerns:

1. **Order lifecycle** — Is demand growing? Are restaurants keeping up? Are deliveries on time?  
2. **Courier supply** — Are couriers available where orders are? Are sessions healthy?

Together, these feeds support **supply-demand balance analytics**, **SLA monitoring**, **anomaly detection**, and **fraud heuristics** — all the analytics required by Milestones 2 and 3.

---

### Feed 1: Order Events (`order_events`)

**Purpose:** Tracks the full lifecycle of every customer order from placement to delivery (or cancellation/refund).

**Key fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | string (UUID) | Unique event identifier |
| `order_id` | string (UUID) | Join key — links all lifecycle steps |
| `event_type` | enum | Lifecycle stage (ORDER_PLACED → DELIVERED) |
| `event_timestamp` | long (millis) | **Event time** — used for watermarks |
| `ingestion_timestamp` | long (millis) | Processing time — differs for late events |
| `restaurant_id` | string | Join key for restaurant reference data |
| `courier_id` | string (nullable) | Join key for courier events |
| `zone_id` | string | Zone-level aggregation key |
| `actual_prep_time_sec` | int (nullable) | Populated at READY_FOR_PICKUP — enables SLA |
| `is_duplicate` | boolean | Flags duplicate events for deduplication logic |
| `schema_version` | string | Forward-compatibility tracking |

**Supported event types:**
```
ORDER_PLACED → ORDER_ACCEPTED → PREPARATION_STARTED → READY_FOR_PICKUP
→ PICKED_UP → OUT_FOR_DELIVERY → DELIVERED
                                               ↘ CANCELLED (any stage)
                                               ↘ REFUND_REQUESTED (post-delivery)
```

**Analytics enabled:**
- Windowed KPIs: orders/min, avg delivery time, cancellation rate
- Restaurant SLA: prep-time percentiles per restaurant/zone
- Fraud detection: repeated cancellations from the same customer
- Demand surge indicators per zone

---

### Feed 2: Courier Status Events (`courier_status_events`)

**Purpose:** Tracks real-time courier availability, GPS location, and delivery activity. Each event represents a state transition (ONLINE, ASSIGNED, EN_ROUTE, etc.).

**Key fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | string (UUID) | Unique event identifier |
| `courier_id` | string | Join key with order events |
| `order_id` | string (nullable) | Active order ID — null when idle/offline |
| `event_type` | enum | Current courier activity |
| `event_timestamp` | long (millis) | Event time for watermarking |
| `zone_id` | string | Current courier zone |
| `latitude` / `longitude` | double | GPS position for mapping |
| `session_id` | string | Groups events in one online session |
| `went_offline_mid_delivery` | boolean | Edge case flag |
| `battery_level` | int (nullable) | Device battery — predictive offline indicator |

**Supported event types:**
```
ONLINE → IDLE → ASSIGNED → EN_ROUTE_TO_RESTAURANT → ARRIVED_AT_RESTAURANT
       → EN_ROUTE_TO_CUSTOMER → ARRIVED_AT_CUSTOMER → IDLE (loop)
       → BREAK → OFFLINE
```

**Analytics enabled:**
- Session window analytics: time online, deliveries per session
- Demand-supply health per zone (idle couriers vs pending orders)
- Courier performance benchmarking
- Anomaly detection: offline mid-delivery events

---

## Edge Cases for Streaming Correctness

The generator deliberately injects the following edge cases:

| Edge Case | Description | Parameter |
|-----------|-------------|-----------|
| **Out-of-order events** | `ingestion_timestamp` is earlier than `event_timestamp` | `--late-event-prob` |
| **Duplicate events** | Same logical event emitted twice with different `event_id` | `--duplicate-prob` |
| **Missing steps** | `PICKED_UP` step skipped (order goes directly to `OUT_FOR_DELIVERY`) | `--missing-step-prob` |
| **Impossible durations** | Prep time of 1–30 seconds (anomaly detection target) | `--impossible-duration-prob` |
| **Courier offline mid-delivery** | Courier sends OFFLINE while an order is assigned | `--offline-mid-delivery-prob` |

These are critical to demonstrate:
- **Watermarks**: late-arriving events should fall within or outside the watermark window
- **Deduplication**: idempotent processing using `order_id_dedup` key
- **Anomaly detection**: impossible durations and offline mid-delivery events

---

## Event-Time Processing Design

Both feeds include two timestamps:

- `event_timestamp` — when the event **actually happened** (event time)
- `ingestion_timestamp` — when the event **arrived at the broker** (processing time)

This separation is essential for:
1. **Watermark configuration** in Spark Structured Streaming (based on `event_timestamp`)
2. **Late data handling** — events with `ingestion_timestamp < event_timestamp` are late arrivals
3. **Correctness of windowed aggregations** regardless of network delays

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

## Setup & Usage

### Prerequisites

```bash
python3 -m pip install -r generator/requirements.txt
```

### Generate sample data

```bash
# Default: 200 orders, 50 restaurants, 20 couriers
python3 generator/generator.py

# Custom configuration
python3 generator/generator.py \
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
python3 generator/generator.py --seed 123
```

### CLI Reference

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

## Planned Analytics (Milestone 2)

| Use Case | Type | Feeds Used |
|----------|------|-----------|
| Orders/min by zone (tumbling window) | Basic | Order Events |
| Avg delivery time per restaurant (hopping window) | Basic | Order Events |
| Courier active session analytics | Intermediate | Courier Status |
| Demand–supply health per zone | Intermediate | Both feeds joined |
| Restaurant SLA: prep-time percentiles | Intermediate | Order Events |
| Anomaly detection: impossible delivery times | Advanced | Order Events |
| Fraud heuristics: repeat cancellations | Advanced | Order Events |
| Surge zone prediction | Advanced | Both feeds |

---

## Schema Versioning

Both AVRO schemas include a `schema_version` field (default `"1.0"`).  
For Milestone 2, schema evolution will be handled using **AVRO schema registry patterns** — new optional fields will use `["null", <type>]` unions with a `null` default to maintain backward compatibility.

---

## Assumptions

- Geographic area: Madrid, Spain (lat/lon bounding boxes per zone)
- Currency: EUR
- Time zone: UTC (all timestamps in UTC millis)
- Customer IDs are anonymised (no PII)
- Restaurant and courier IDs are synthetic
- Delivery distance is straight-line (Haversine) — not routed
