"""
stream_producer.py — Simulates a live e-commerce event stream for streaming_job.py.

Drops CSV files into data/raw/stream_input/ every 3 seconds.
Each batch contains purchase and view/cart events with realistic category distribution.
Occasionally injects a "purchase spike" to trigger the anomaly alert.

Usage:
    python src/stream_producer.py                     # normal mode
    python src/stream_producer.py --spike-every 5     # spike every 5th batch
"""

import argparse
import csv
import os
import random
import time
from datetime import datetime, timezone


CATEGORIES = [
    "electronics.smartphone", "electronics.audio", "computers.notebook",
    "appliances.kitchen", "appliances.environment.water_heater",
    "furniture.bedroom", "kids.toys", "sport.trainer",
]

REFERRERS = ["direct", "google", "facebook", "instagram", "email", "affiliate"]

REFERRER_WEIGHTS = [0.40, 0.25, 0.15, 0.08, 0.07, 0.05]


def generate_batch(batch_num, spike=False):
    """Return list of event dicts for one micro-batch."""
    n_events = random.randint(30, 100)
    events = []
    now = datetime.now(timezone.utc)

    # On spike batches, flood one category with purchases
    spike_category = random.choice(CATEGORIES) if spike else None
    if spike:
        n_events += random.randint(50, 150)
        print(f"    ⚡ SPIKE injected for category: {spike_category}")

    for _ in range(n_events):
        event_type = random.choices(
            ["view", "cart", "purchase"],
            weights=[0.70, 0.20, 0.10],
        )[0]

        category = spike_category if (spike and random.random() < 0.6) \
                   else random.choice(CATEGORIES)

        event = {
            "event_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type":    event_type,
            "user_id":       str(random.randint(100000, 999999)),
            "product_id":    str(random.randint(1000000, 9999999)),
            "category_code": category,
            "price":         round(random.uniform(5.0, 1500.0), 2) if event_type == "purchase" else 0.0,
            "referrer":      random.choices(REFERRERS, weights=REFERRER_WEIGHTS)[0],
        }
        events.append(event)

    return events


def main():
    parser = argparse.ArgumentParser(description="DSS Stream Producer")
    parser.add_argument("--spike-every", type=int, default=7,
                        help="Inject a purchase spike every N batches (default: 7)")
    parser.add_argument("--interval",    type=float, default=3.0,
                        help="Seconds between batches (default: 3)")
    parser.add_argument("--batches",     type=int, default=0,
                        help="Stop after N batches (0 = run forever)")
    args = parser.parse_args()

    base_dir  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(base_dir, "data", "raw", "stream_input")
    os.makedirs(input_dir, exist_ok=True)

    print("\n" + "=" * 60)
    print(" DSS STREAM PRODUCER")
    print("=" * 60)
    print(f" Output dir    : {input_dir}")
    print(f" Spike every   : {args.spike_every} batches")
    print(f" Batch interval: {args.interval}s")
    print(" Run streaming_job.py in another terminal to consume events.")
    print("=" * 60 + "\n")

    batch_num = 1
    try:
        while True:
            spike = (batch_num % args.spike_every == 0)
            events = generate_batch(batch_num, spike=spike)

            ts_str    = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            file_path = os.path.join(input_dir, f"batch_{batch_num:04d}_{ts_str}.csv")

            with open(file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(events[0].keys()))
                writer.writeheader()
                writer.writerows(events)

            purchases = sum(1 for e in events if e["event_type"] == "purchase")
            print(f"  Batch {batch_num:4d} | {len(events):4d} events | "
                  f"{purchases:3d} purchases | {'⚡ SPIKE' if spike else 'normal'}")

            if args.batches > 0 and batch_num >= args.batches:
                print(f"\n>>> Completed {batch_num} batches. Stopping.")
                break

            batch_num += 1
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n>>> Producer stopped by user.")


if __name__ == "__main__":
    main()
