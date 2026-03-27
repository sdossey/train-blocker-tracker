#!/usr/bin/env python3
"""
Train Status Poller — single-run version for GitHub Actions.
Called once per workflow run; GitHub Actions cron handles scheduling.

Output:
    train_history.csv  — appended with any detected events (Blocked / Train!)
"""

import requests
import csv
import logging
from datetime import datetime
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
API_URL         = "https://train.cohub.com/api/status"
TRAIN_THRESHOLD = 0.5       # train probability above this = "Train detected"
CSV_FILE        = "train_history.csv"
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

CSV_HEADERS = [
    "timestamp",
    "crossing",
    "event",          # "Train!" or "Blocked"
    "train_prob",
    "signal",
    "closed",
    "maintenance_mode",
    "raw_json",
]


def ensure_csv():
    if not Path(CSV_FILE).exists():
        with open(CSV_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()
        log.info(f"Created {CSV_FILE}")


def append_event(row: dict):
    with open(CSV_FILE, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)


def check_crossing(name, status, closed, ts, raw, maintenance):
    train_prob = status.get("train", 0)
    signal     = status.get("signal", 0)
    events     = []

    if closed:
        events.append("Blocked")
    if train_prob >= TRAIN_THRESHOLD:
        events.append("Train!")

    for event in events:
        append_event({
            "timestamp":        ts,
            "crossing":         name,
            "event":            event,
            "train_prob":       round(train_prob, 6),
            "signal":           round(signal, 6),
            "closed":           closed,
            "maintenance_mode": maintenance,
            "raw_json":         raw,
        })
        log.info(f"EVENT [{name}] {event} — train_prob={train_prob:.4f}, closed={closed}")

    return bool(events)


def poll():
    try:
        resp = requests.get(API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        log.warning(f"Request failed: {e}")
        return
    except ValueError as e:
        log.warning(f"JSON parse error: {e}")
        return

    ts          = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    raw         = resp.text
    maintenance = data.get("maintenanceMode", False)

    if maintenance:
        log.info(f"Maintenance mode — {data.get('maintenanceMessage', '')}")
        return

    crossings = {
        "Fourth":   (data.get("fourthStatus",  {}), data.get("fourthClosed",   False)),
        "Chestnut": (data.get("chestnutStatus",{}), data.get("chestnutClosed", False)),
    }

    any_event = False
    for name, (status, closed) in crossings.items():
        any_event |= check_crossing(name, status, closed, ts, raw, maintenance)

    if not any_event:
        log.info(
            f"All clear — "
            f"Fourth train={data['fourthStatus']['train']:.2e}, "
            f"Chestnut train={data['chestnutStatus']['train']:.2e}"
        )


if __name__ == "__main__":
    ensure_csv()
    poll()
