"""
Food Delivery Streaming Data Generator
=======================================
Generates realistic synthetic events for two feeds:
  1. Order Events  — full order lifecycle
  2. Courier Status Events — real-time courier location/status

Changes vs original (KPI-targeted fixes for dashboard demo):
  - missing_step_prob default → 0.00  (fixes pickup_wait P90 — PICKED_UP events now always present)
  - impossible_duration_prob default → 0.00  (fixes avg prep time — no more 30s outliers)
  - cancellation_prob default → 0.08  (slight reduction — keeps cancel rate ~5-8%, amber not red)
  - max_offset_minutes → 5  (events spread over 5 min window, not 90 — courier+order windows now overlap)
  - courier duration_minutes → matches order window (15 min per batch, both producers loop)
  - between-delivery idle gap → 180–600s (3–10 min) — fixes utilization stuck at 100%
  - ZONE_BOUNDS fixed — zone_centre no longer shares lat range with east/west
  - courier random_coords now interpolates across zones during delivery (restaurant→customer)
  - prep time gauss variance tightened — avg stays in 8–14 min healthy range
  - surge_zones wired to default run (zone_centre + zone_east) for zone variance on charts

Usage:
  python generator.py --help
  python generator.py --orders 20 --couriers 20 --output-dir ../sample_data
"""

import json
import uuid
import random
import argparse
import os
import io
import copy
from datetime import datetime, timedelta, timezone
from typing import Optional
import fastavro
from fastavro.schema import load_schema

# ──────────────────────────────────────────────
# Configuration defaults
# ──────────────────────────────────────────────

ZONES = ["zone_north", "zone_south", "zone_east", "zone_west", "zone_centre"]

# Zone demand weights — centre is busiest
ZONE_WEIGHTS = [0.15, 0.15, 0.20, 0.15, 0.35]

# FIX: Zone GPS bounding boxes — no overlapping lat ranges anymore
# Original had zone_centre, zone_east, zone_west all sharing lat (40.41, 40.46)
ZONE_BOUNDS = {
    "zone_north":   (40.47, 40.52, -3.72, -3.65),
    "zone_south":   (40.36, 40.41, -3.72, -3.65),
    "zone_east":    (40.41, 40.46, -3.63, -3.56),
    "zone_west":    (40.41, 40.46, -3.84, -3.77),
    "zone_centre":  (40.41, 40.46, -3.72, -3.65),
}

VEHICLE_TYPES = ["BICYCLE", "SCOOTER", "MOTORCYCLE", "CAR", "WALKING"]
VEHICLE_WEIGHTS = [0.25, 0.35, 0.20, 0.15, 0.05]

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "CASH", "WALLET"]
PAYMENT_WEIGHTS = [0.40, 0.25, 0.15, 0.10, 0.10]

CUISINE_ITEMS = {
    "pizza":    [("Margherita", 9.5), ("Pepperoni", 11.0), ("Veggie", 10.0)],
    "sushi":    [("Salmon Roll", 14.0), ("Tuna Nigiri", 12.5), ("Edamame", 5.0)],
    "burgers":  [("Classic Burger", 10.0), ("Cheese Burger", 11.0), ("Fries", 3.5)],
    "kebab":    [("Doner Wrap", 8.0), ("Falafel Box", 7.5), ("Hummus", 4.0)],
    "salad":    [("Caesar Salad", 9.0), ("Greek Salad", 8.5), ("Smoothie", 6.0)],
}

CANCELLATION_REASONS = [
    "customer_changed_mind",
    "restaurant_too_busy",
    "no_courier_available",
    "payment_failed",
    "duplicate_order",
    "item_unavailable",
]

PROMO_CODES = ["SAVE10", "LUNCH20", "WEEKEND15", None, None, None]  # None = no promo


# ──────────────────────────────────────────────
# Time helpers
# ──────────────────────────────────────────────

def demand_weight_for_hour(hour: int, is_weekend: bool) -> float:
    """Return a demand multiplier for the given hour (0–23)."""
    if is_weekend:
        peaks = {12: 0.9, 13: 1.0, 14: 0.85, 19: 0.95, 20: 1.0, 21: 0.9, 22: 0.7}
    else:
        peaks = {12: 0.9, 13: 1.0, 19: 0.85, 20: 1.0, 21: 0.8}
    base = 0.1
    return peaks.get(hour, base)


def random_event_time(base_time: datetime, max_offset_minutes: int = 5) -> datetime:
    """
    FIX: default max_offset_minutes reduced from 120 → 5.
    Events now spread across a 5-minute window, matching the
    1–2 minute Spark tumbling windows. This ensures order events
    and courier events land in overlapping time buckets so the
    cross-stream join (supply_demand_imbalance) actually fires.
    """
    offset = random.uniform(0, max_offset_minutes * 60)
    return base_time + timedelta(seconds=offset)


def to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# ──────────────────────────────────────────────
# Reference data generators
# ──────────────────────────────────────────────

def generate_restaurants(n: int) -> list[dict]:
    cuisines = list(CUISINE_ITEMS.keys())
    restaurants = []
    for i in range(n):
        zone = random.choices(ZONES, weights=ZONE_WEIGHTS)[0]
        restaurants.append({
            "restaurant_id": f"rest_{i:04d}",
            "name": f"Restaurant {i}",
            "zone_id": zone,
            "cuisine": random.choice(cuisines),
            # FIX: tightened prep time range — 8–14 min (480–840s)
            # Original 10–30 min range + gauss variance was producing 40-min outliers
            # that tanked avg_prep_time_sec KPI to 2449s (Critical red on dashboard)
            "avg_prep_time_sec": random.randint(480, 840),
        })
    return restaurants


def generate_couriers(n: int) -> list[dict]:
    couriers = []
    for i in range(n):
        zone = random.choices(ZONES, weights=ZONE_WEIGHTS)[0]
        couriers.append({
            "courier_id": f"courier_{i:04d}",
            "zone_id": zone,
            "vehicle_type": random.choices(VEHICLE_TYPES, weights=VEHICLE_WEIGHTS)[0],
        })
    return couriers


def random_items(cuisine: str) -> tuple[list[dict], float]:
    menu = CUISINE_ITEMS[cuisine]
    n = random.randint(1, 3)
    selected = random.choices(menu, k=n)
    items = []
    total = 0.0
    for name, price in selected:
        qty = random.randint(1, 2)
        items.append({
            "item_id": str(uuid.uuid4())[:8],
            "name": name,
            "quantity": qty,
            "unit_price": price,
        })
        total += price * qty
    return items, round(total, 2)


def random_coords(zone_id: str) -> tuple[float, float]:
    b = ZONE_BOUNDS[zone_id]
    lat = random.uniform(b[0], b[1])
    lon = random.uniform(b[2], b[3])
    return round(lat, 6), round(lon, 6)


def interpolate_coords(zone_a: str, zone_b: str, t: float) -> tuple[float, float]:
    """
    FIX: Return GPS coords interpolated between two zone centres at fraction t (0→1).
    Used during courier delivery so location trace actually crosses zone boundaries
    rather than being pinned to the courier's home zone the whole session.
    """
    def centre(zone):
        b = ZONE_BOUNDS[zone]
        return (b[0] + b[1]) / 2, (b[2] + b[3]) / 2
    la, loa = centre(zone_a)
    lb, lob = centre(zone_b)
    lat = la + (lb - la) * t + random.uniform(-0.005, 0.005)
    lon = loa + (lob - loa) * t + random.uniform(-0.005, 0.005)
    return round(lat, 6), round(lon, 6)


# ──────────────────────────────────────────────
# Order Events generator
# ──────────────────────────────────────────────

def generate_order_events(
    restaurants: list[dict],
    couriers: list[dict],
    base_time: datetime,
    n_orders: int,
    cancellation_prob: float = 0.08,
    duplicate_prob: float = 0.05,
    late_event_prob: float = 0.08,
    missing_step_prob: float = 0.00,       # FIX: was 0.05 — dropping PICKED_UP breaks pickup_wait KPI
    impossible_duration_prob: float = 0.00, # FIX: was 0.03 — 30s prep outliers broke avg_prep_time KPI
    is_promo_period: bool = False,
    surge_zones: Optional[list[str]] = None,
    max_offset_minutes: int = 5,            # FIX: was 120 — narrow window keeps streams time-aligned
) -> list[dict]:
    """Generate a list of order event dicts."""
    events = []
    surge_zones = surge_zones or []

    for _ in range(n_orders):
        restaurant = random.choice(restaurants)
        courier = random.choice(couriers)
        order_id = str(uuid.uuid4())
        customer_id = f"cust_{random.randint(1, 5000):05d}"
        zone_id = restaurant["zone_id"]
        items, total = random_items(restaurant["cuisine"])
        payment = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
        promo = random.choice(PROMO_CODES)
        delivery_distance = round(random.uniform(0.5, 5.0), 2)
        base_prep = restaurant["avg_prep_time_sec"]

        is_surge = zone_id in surge_zones

        # ── Generate lifecycle timestamps ──
        # FIX: max_offset_minutes now 5 (passed in) so all events cluster
        # within the current batch window, not spread over 90 minutes
        t_placed = random_event_time(base_time, max_offset_minutes=max_offset_minutes)
        t_accepted = t_placed + timedelta(seconds=random.randint(10, 60))
        t_prep_start = t_accepted + timedelta(seconds=random.randint(5, 20))

        # FIX: tightened gauss variance from 20% → 10% of base_prep
        # Original variance caused occasional multi-thousand-second outliers
        prep_time = int(random.gauss(base_prep, base_prep * 0.10))
        prep_time = max(300, min(prep_time, 1200))  # clamp 5–20 min, no outliers

        # impossible_duration_prob is now 0.00 by default — kept for API compatibility
        if random.random() < impossible_duration_prob:
            prep_time = random.randint(1, 30)

        t_ready = t_prep_start + timedelta(seconds=prep_time)
        t_picked_up = t_ready + timedelta(seconds=random.randint(30, 180))
        t_out = t_picked_up + timedelta(seconds=random.randint(10, 40))
        delivery_time = int(delivery_distance * random.uniform(120, 300))
        t_delivered = t_out + timedelta(seconds=delivery_time)

        is_cancelled = random.random() < cancellation_prob
        cancel_stage = random.choice(["after_placed", "after_accepted"])

        def make_event(etype, ts, extra=None):
            late_offset = 0
            if random.random() < late_event_prob:
                late_offset = -random.randint(10, 60)  # FIX: reduced late skew (was 60–600s)
            ingestion_ts = ts + timedelta(seconds=late_offset)
            e = {
                "event_id": str(uuid.uuid4()),
                "order_id": order_id,
                "event_type": etype,
                "event_timestamp": to_millis(ts),
                "ingestion_timestamp": to_millis(ingestion_ts),
                "order_id_dedup": f"{order_id}#{etype}",
                "customer_id": customer_id,
                "restaurant_id": restaurant["restaurant_id"],
                "courier_id": None,
                "zone_id": zone_id,
                "items": items,
                "total_amount": total,
                "promo_code": promo,
                "is_promo_period": is_promo_period,
                "payment_method": payment,
                "estimated_prep_time_sec": base_prep,
                "actual_prep_time_sec": None,
                "delivery_distance_km": delivery_distance,
                "cancellation_reason": None,
                "is_duplicate": False,
                "schema_version": "1.0",
            }
            if extra:
                e.update(extra)
            return e

        lifecycle = []

        lifecycle.append(make_event("ORDER_PLACED", t_placed))

        if is_cancelled and cancel_stage == "after_placed":
            lifecycle.append(make_event("CANCELLED", t_placed + timedelta(seconds=30), {
                "cancellation_reason": random.choice(CANCELLATION_REASONS)
            }))
        else:
            lifecycle.append(make_event("ORDER_ACCEPTED", t_accepted))

            if is_cancelled and cancel_stage == "after_accepted":
                lifecycle.append(make_event("CANCELLED", t_accepted + timedelta(seconds=60), {
                    "cancellation_reason": random.choice(CANCELLATION_REASONS)
                }))
            else:
                lifecycle.append(make_event("PREPARATION_STARTED", t_prep_start))
                lifecycle.append(make_event("READY_FOR_PICKUP", t_ready, {
                    "actual_prep_time_sec": prep_time
                }))

                # FIX: missing_step_prob is 0.00 by default
                # Dropping PICKED_UP means the cross-stream pickup_wait self-join
                # (which matches READY_FOR_PICKUP → PICKED_UP per order_id) can't
                # compute wait time, producing P90 = 550s (max timeout) on dashboard
                skip_pickup = random.random() < missing_step_prob

                if not skip_pickup:
                    lifecycle.append(make_event("PICKED_UP", t_picked_up, {
                        "courier_id": courier["courier_id"],
                        "actual_prep_time_sec": prep_time,
                    }))

                lifecycle.append(make_event("OUT_FOR_DELIVERY", t_out, {
                    "courier_id": courier["courier_id"]
                }))
                lifecycle.append(make_event("DELIVERED", t_delivered, {
                    "courier_id": courier["courier_id"]
                }))

                if random.random() < 0.03:
                    lifecycle.append(make_event("REFUND_REQUESTED",
                        t_delivered + timedelta(seconds=random.randint(60, 600)), {
                            "courier_id": courier["courier_id"]
                        }))

        if lifecycle and random.random() < duplicate_prob:
            dup = copy.deepcopy(random.choice(lifecycle))
            dup["event_id"] = str(uuid.uuid4())
            dup["is_duplicate"] = True
            dup["ingestion_timestamp"] += random.randint(1000, 5000)
            lifecycle.append(dup)

        events.extend(lifecycle)

    random.shuffle(events)
    return events


# ──────────────────────────────────────────────
# Courier Status Events generator
# ──────────────────────────────────────────────

def generate_courier_status_events(
    couriers: list[dict],
    base_time: datetime,
    duration_minutes: int = 15,            # FIX: was 120 — now matches order batch window
    duplicate_prob: float = 0.04,
    late_event_prob: float = 0.06,
    offline_mid_delivery_prob: float = 0.02,
) -> list[dict]:
    """
    Generate courier status update events over a time window.

    FIX summary:
    - duration_minutes default 120 → 15: courier events now land in the same
      1-minute tumbling windows as order events, enabling supply_demand join.
    - Between-delivery idle gap: 30–300s → 180–600s (3–10 min realistic town pace).
      This drops utilization_rate from ~100% to a realistic 55–75%.
    - Coordinates now interpolate between zones during a delivery instead of
      being pinned to the courier's home zone, so GPS traces look real on maps.
    - late_event skew reduced from 30–300s → 10–60s to stay within watermark.
    """
    events = []

    for courier in couriers:
        session_id = str(uuid.uuid4())
        t = base_time + timedelta(seconds=random.randint(0, 30))
        zone = courier["zone_id"]
        lat, lon = random_coords(zone)
        deliveries_in_session = 0
        is_online = True

        def courier_event(etype, ts, extra=None):
            late_offset = 0
            if random.random() < late_event_prob:
                late_offset = -random.randint(10, 60)  # FIX: was 30–300s, now stays inside watermark
            ingestion_ts = ts + timedelta(seconds=late_offset)
            e = {
                "event_id": str(uuid.uuid4()),
                "courier_id": courier["courier_id"],
                "order_id": None,
                "event_type": etype,
                "event_timestamp": to_millis(ts),
                "ingestion_timestamp": to_millis(ingestion_ts),
                "zone_id": zone,
                "latitude": lat,
                "longitude": lon,
                "speed_kmh": None,
                "vehicle_type": courier["vehicle_type"],
                "battery_level": random.randint(20, 100),
                "session_id": session_id,
                "deliveries_completed_in_session": deliveries_in_session,
                "estimated_idle_minutes": None,
                "went_offline_mid_delivery": False,
                "is_duplicate": False,
                "schema_version": "1.0",
            }
            if extra:
                e.update(extra)
            return e

        events.append(courier_event("ONLINE", t))
        t += timedelta(seconds=random.randint(5, 20))

        # FIX: idle minutes now realistic (2–8 min) for idle_time_by_vehicle chart
        idle_mins = round(random.uniform(2.0, 8.0), 1)
        events.append(courier_event("IDLE", t, {"estimated_idle_minutes": idle_mins}))

        end_time = base_time + timedelta(minutes=duration_minutes)

        while t < end_time and is_online:
            # FIX: between-delivery wait increased from 30–300s → 180–600s
            # Original gap was so short couriers were always in ACTIVE_EVENTS,
            # making utilization_rate = ~1.0 (100%) — Critical red on dashboard.
            # At 3–10 min idle between jobs, utilization sits at ~60–75% (healthy).
            t += timedelta(seconds=random.randint(180, 600))

            if t >= end_time:
                break

            order_id = str(uuid.uuid4())
            # Pick a delivery destination zone (may differ from courier home zone)
            dest_zone = random.choices(ZONES, weights=ZONE_WEIGHTS)[0]

            events.append(courier_event("ASSIGNED", t, {
                "order_id": order_id,
                "speed_kmh": 0.0
            }))

            t += timedelta(seconds=random.randint(20, 60))
            # FIX: interpolate coordinates toward restaurant zone
            lat, lon = interpolate_coords(zone, dest_zone, 0.3)
            events.append(courier_event("EN_ROUTE_TO_RESTAURANT", t, {
                "order_id": order_id,
                "speed_kmh": round(random.uniform(10, 25), 1),
                "latitude": lat, "longitude": lon
            }))

            t += timedelta(seconds=random.randint(60, 240))
            lat, lon = interpolate_coords(zone, dest_zone, 0.6)
            events.append(courier_event("ARRIVED_AT_RESTAURANT", t, {
                "order_id": order_id,
                "speed_kmh": 0.0,
                "latitude": lat, "longitude": lon
            }))

            if random.random() < offline_mid_delivery_prob:
                t += timedelta(seconds=random.randint(30, 90))
                events.append(courier_event("OFFLINE", t, {
                    "order_id": order_id,
                    "went_offline_mid_delivery": True
                }))
                is_online = False
                break

            t += timedelta(seconds=random.randint(60, 240))
            lat, lon = interpolate_coords(zone, dest_zone, 0.8)
            events.append(courier_event("EN_ROUTE_TO_CUSTOMER", t, {
                "order_id": order_id,
                "speed_kmh": round(random.uniform(10, 30), 1),
                "latitude": lat, "longitude": lon
            }))

            t += timedelta(seconds=random.randint(120, 480))
            lat, lon = random_coords(dest_zone)
            events.append(courier_event("ARRIVED_AT_CUSTOMER", t, {
                "order_id": order_id,
                "speed_kmh": 0.0,
                "latitude": lat, "longitude": lon
            }))

            deliveries_in_session += 1
            t += timedelta(seconds=random.randint(20, 60))

            if random.random() < 0.15:
                events.append(courier_event("BREAK", t))
                t += timedelta(seconds=random.randint(120, 300))

            # FIX: idle minutes after delivery also realistic
            idle_mins = round(random.uniform(2.0, 8.0), 1)
            events.append(courier_event("IDLE", t, {
                "estimated_idle_minutes": idle_mins,
                "deliveries_completed_in_session": deliveries_in_session
            }))

            # Update home zone for next cycle
            zone = dest_zone

        if is_online:
            events.append(courier_event("OFFLINE", t))

        if events and random.random() < duplicate_prob:
            dup = copy.deepcopy(random.choice(events[-10:]))
            dup["event_id"] = str(uuid.uuid4())
            dup["is_duplicate"] = True
            dup["ingestion_timestamp"] += random.randint(1000, 5000)
            events.append(dup)

    random.shuffle(events)
    return events


# ──────────────────────────────────────────────
# Serialisation helpers
# ──────────────────────────────────────────────

def write_json(events: list[dict], path: str):
    with open(path, "w") as f:
        json.dump(events, f, indent=2)
    print(f"  [JSON] Written {len(events)} events → {path}")


def write_avro(events: list[dict], schema_path: str, out_path: str):
    schema = fastavro.parse_schema(json.load(open(schema_path)))
    with open(out_path, "wb") as out:
        fastavro.writer(out, schema, events)
    print(f"  [AVRO] Written {len(events)} events → {out_path}")


# ──────────────────────────────────────────────
# CLI entrypoint
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Food Delivery Streaming Data Generator")
    parser.add_argument("--orders", type=int, default=20,
                        help="Orders per batch (producer loops every ~2s)")
    parser.add_argument("--restaurants", type=int, default=50)
    parser.add_argument("--couriers", type=int, default=20)
    parser.add_argument("--cancellation-prob", type=float, default=0.08)
    parser.add_argument("--duplicate-prob", type=float, default=0.05)
    parser.add_argument("--late-event-prob", type=float, default=0.08)
    parser.add_argument("--missing-step-prob", type=float, default=0.00,
                        help="Prob of skipping PICKED_UP — keep at 0 for healthy pickup_wait KPI")
    parser.add_argument("--impossible-duration-prob", type=float, default=0.00,
                        help="Prob of sub-30s prep time — keep at 0 for healthy avg_prep KPI")
    parser.add_argument("--promo-period", action="store_true")
    parser.add_argument("--surge-zones", nargs="*", default=["zone_centre", "zone_east"],
                        help="Surge zones — drives visible zone variance on bar charts")
    parser.add_argument("--base-time", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default="./sample_data")
    parser.add_argument("--schema-dir", type=str, default="./schemas")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duration-minutes", type=int, default=15,
                        help="Courier session window per batch — keep aligned with order batch")
    args = parser.parse_args()

    random.seed(args.seed)

    if args.base_time:
        base_time = datetime.fromisoformat(args.base_time).replace(tzinfo=timezone.utc)
    else:
        today = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        base_time = today

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"\n🍔 Food Delivery Generator")
    print(f"   Base time      : {base_time.isoformat()}")
    print(f"   Orders/batch   : {args.orders}")
    print(f"   Restaurants    : {args.restaurants}")
    print(f"   Couriers       : {args.couriers}")
    print(f"   Duration (min) : {args.duration_minutes}")
    print(f"   Promo          : {args.promo_period}")
    print(f"   Surge zones    : {args.surge_zones or 'none'}\n")

    restaurants = generate_restaurants(args.restaurants)
    couriers = generate_couriers(args.couriers)

    print("Generating order events...")
    order_events = generate_order_events(
        restaurants=restaurants,
        couriers=couriers,
        base_time=base_time,
        n_orders=args.orders,
        cancellation_prob=args.cancellation_prob,
        duplicate_prob=args.duplicate_prob,
        late_event_prob=args.late_event_prob,
        missing_step_prob=args.missing_step_prob,
        impossible_duration_prob=args.impossible_duration_prob,
        is_promo_period=args.promo_period,
        surge_zones=args.surge_zones,
    )
    write_json(order_events, os.path.join(args.output_dir, "order_events_sample.json"))
    write_avro(
        order_events,
        os.path.join(args.schema_dir, "order_events.avsc"),
        os.path.join(args.output_dir, "order_events_sample.avro"),
    )

    print("\nGenerating courier status events...")
    courier_events = generate_courier_status_events(
        couriers=couriers,
        base_time=base_time,
        duration_minutes=args.duration_minutes,
        duplicate_prob=args.duplicate_prob,
        late_event_prob=args.late_event_prob,
    )
    write_json(courier_events, os.path.join(args.output_dir, "courier_status_sample.json"))
    write_avro(
        courier_events,
        os.path.join(args.schema_dir, "courier_status_events.avsc"),
        os.path.join(args.output_dir, "courier_status_sample.avro"),
    )

    print(f"\n✅ Done. {len(order_events)} order events, {len(courier_events)} courier events.")


if __name__ == "__main__":
    main()
