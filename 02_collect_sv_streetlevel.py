#!/usr/bin/env python3
"""
02_collect_sv_streetlevel.py

Queries Google Street View metadata using the `streetlevel` library (no API key needed).
Uses Google's internal/undocumented endpoints — no billing, no rate limits.

Features adaptive concurrency: speeds up when healthy, backs off on timeouts.

Usage:
    python3.9 02_collect_sv_streetlevel.py

Requires:
    - data/sample_points.csv (or sample_points_snapped.csv)
    - pip install streetlevel aiohttp tqdm
"""

import asyncio
import csv
import logging
import os
import sys
import time
from pathlib import Path

import aiohttp
from tqdm import tqdm

from streetlevel import streetview

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
SAMPLE_POINTS_ORIGINAL = DATA_DIR / "sample_points.csv"
SAMPLE_POINTS_SNAPPED = DATA_DIR / "sample_points_snapped.csv"
SAMPLE_POINTS_PATH = SAMPLE_POINTS_SNAPPED if SAMPLE_POINTS_SNAPPED.exists() else SAMPLE_POINTS_ORIGINAL
RESULTS_PATH = DATA_DIR / "sv_results_v4.csv"

# Adaptive concurrency settings
INITIAL_CONCURRENT = 80
MIN_CONCURRENT = 10
MAX_CONCURRENT = 200
# If error rate exceeds this in a window, back off
ERROR_RATE_THRESHOLD = 0.05  # 5%
# How many results to evaluate before adjusting
ADJUST_WINDOW = 200

FLUSH_INTERVAL = 500
LOG_INTERVAL = 5000

# Retry configuration
MAX_RETRIES = 3
BACKOFF_BASE = 2

# Output CSV columns (compatible with downstream scripts)
RESULT_COLUMNS = [
    "point_id", "query_lat", "query_lng", "status",
    "sv_date", "pano_id", "sv_lat", "sv_lng",
    "tier", "city_name", "state", "population",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_completed_ids(path: Path) -> set:
    completed = set()
    if not path.exists():
        return completed
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            completed.add(row["point_id"])
    return completed


def load_sample_points(path: Path) -> list:
    points = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            points.append(row)
    return points


def ensure_results_header(path: Path) -> None:
    if not path.exists():
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)
            writer.writeheader()


def format_date(date_obj) -> str:
    if date_obj is None:
        return ""
    try:
        return f"{date_obj.year:04d}-{date_obj.month:02d}"
    except (AttributeError, TypeError):
        return str(date_obj)


# ---------------------------------------------------------------------------
# Adaptive concurrency controller
# ---------------------------------------------------------------------------


class AdaptiveLimiter:
    """Dynamically adjusts concurrency based on error rate."""

    def __init__(self):
        self.current = INITIAL_CONCURRENT
        self.semaphore = asyncio.Semaphore(INITIAL_CONCURRENT)
        self._window_ok = 0
        self._window_err = 0
        self._lock = asyncio.Lock()

    async def record(self, success: bool):
        async with self._lock:
            if success:
                self._window_ok += 1
            else:
                self._window_err += 1

            total = self._window_ok + self._window_err
            if total >= ADJUST_WINDOW:
                error_rate = self._window_err / total
                old = self.current

                if error_rate > ERROR_RATE_THRESHOLD:
                    # Back off: halve concurrency
                    self.current = max(MIN_CONCURRENT, self.current // 2)
                elif error_rate < 0.01 and self.current < MAX_CONCURRENT:
                    # Healthy: ramp up by 25%
                    self.current = min(MAX_CONCURRENT, int(self.current * 1.25))

                if self.current != old:
                    self.semaphore = asyncio.Semaphore(self.current)
                    log.info(
                        "Concurrency adjusted: %d -> %d (error_rate=%.1f%%)",
                        old, self.current, error_rate * 100,
                    )

                self._window_ok = 0
                self._window_err = 0


# ---------------------------------------------------------------------------
# Async query logic
# ---------------------------------------------------------------------------


async def query_single_point(
    session: aiohttp.ClientSession,
    limiter: AdaptiveLimiter,
    point: dict,
) -> dict:
    lat = float(point["lat"])
    lng = float(point["lng"])

    row = {
        "point_id": point["point_id"],
        "query_lat": point["lat"],
        "query_lng": point["lng"],
        "status": "ZERO_RESULTS",
        "sv_date": "",
        "pano_id": "",
        "sv_lat": "",
        "sv_lng": "",
        "tier": point.get("tier", ""),
        "city_name": point.get("city_name", ""),
        "state": point.get("state", ""),
        "population": point.get("population", ""),
    }

    for attempt in range(MAX_RETRIES):
        try:
            async with limiter.semaphore:
                pano = await streetview.find_panorama_async(
                    lat, lng, session=session
                )

            if pano is None:
                row["status"] = "ZERO_RESULTS"
                await limiter.record(True)
                return row

            row["status"] = "OK"
            row["sv_date"] = format_date(pano.date)
            row["pano_id"] = pano.id or ""
            row["sv_lat"] = round(pano.lat, 6) if pano.lat else ""
            row["sv_lng"] = round(pano.lon, 6) if pano.lon else ""
            await limiter.record(True)
            return row

        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
            await limiter.record(False)
            wait = BACKOFF_BASE ** attempt
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(wait)
            else:
                log.debug("All retries exhausted for point %s", point["point_id"])
                row["status"] = "ERROR"
                return row

        except Exception as e:
            await limiter.record(False)
            log.error("Unexpected error for point %s: %s", point["point_id"], e)
            row["status"] = "ERROR"
            return row

    return row


async def run_collection(points: list) -> dict:
    total = len(points)
    limiter = AdaptiveLimiter()
    log.info(
        "Starting collection for %d points (initial concurrency=%d)",
        total, INITIAL_CONCURRENT,
    )

    counts = {"ok": 0, "zero_results": 0, "error": 0}

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ttl_dns_cache=300)

    results_file = open(RESULTS_PATH, "a", newline="")
    writer = csv.DictWriter(results_file, fieldnames=RESULT_COLUMNS)
    pending_writes = 0
    start_time = time.monotonic()

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            BATCH_SIZE = 5000
            pbar = tqdm(total=total, desc="Querying SV (streetlevel)", unit="pts")
            completed = 0

            for batch_start in range(0, total, BATCH_SIZE):
                batch = points[batch_start:batch_start + BATCH_SIZE]
                tasks = [
                    asyncio.create_task(query_single_point(session, limiter, p))
                    for p in batch
                ]

                for task in asyncio.as_completed(tasks):
                    result = await task

                    writer.writerow(result)
                    pending_writes += 1

                    status = result["status"]
                    if status == "OK":
                        counts["ok"] += 1
                    elif status in ("ZERO_RESULTS", "NOT_FOUND"):
                        counts["zero_results"] += 1
                    else:
                        counts["error"] += 1

                    completed += 1
                    pbar.update(1)

                    if pending_writes >= FLUSH_INTERVAL:
                        results_file.flush()
                        pending_writes = 0

                    if completed % LOG_INTERVAL == 0:
                        elapsed = time.monotonic() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        pct = (completed / total) * 100
                        log.info(
                            "Progress: %d/%d (%.1f%%) | %.0f pts/sec | "
                            "OK: %d | No imagery: %d | Errors: %d | Concurrency: %d",
                            completed, total, pct, rate,
                            counts["ok"], counts["zero_results"], counts["error"],
                            limiter.current,
                        )

            pbar.close()

    finally:
        results_file.flush()
        results_file.close()

    elapsed = time.monotonic() - start_time
    counts["total"] = completed
    counts["elapsed"] = elapsed
    return counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main():
    log.info("Using streetlevel library (no API key needed)")

    if not SAMPLE_POINTS_PATH.exists():
        log.error("Sample points file not found: %s", SAMPLE_POINTS_PATH)
        sys.exit(1)

    all_points = load_sample_points(SAMPLE_POINTS_PATH)
    log.info("Loaded %d sample points from %s", len(all_points), SAMPLE_POINTS_PATH)

    completed_ids = load_completed_ids(RESULTS_PATH)
    if completed_ids:
        log.info("Found %d already-queried points, resuming...", len(completed_ids))

    remaining = [p for p in all_points if p["point_id"] not in completed_ids]
    log.info("%d points remaining to query", len(remaining))

    if not remaining:
        log.info("All points already queried. Nothing to do.")
        return

    ensure_results_header(RESULTS_PATH)

    summary = await run_collection(remaining)

    elapsed = summary.get("elapsed", 0)
    rate = summary["total"] / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print("COLLECTION SUMMARY (streetlevel)")
    print("=" * 60)
    print(f"  Total queried:    {summary['total']:,}")
    print(f"  OK (has imagery): {summary['ok']:,}")
    print(f"  No imagery:       {summary['zero_results']:,}")
    print(f"  Errors:           {summary['error']:,}")
    print(f"  Time elapsed:     {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Average rate:     {rate:.1f} points/sec")
    print("=" * 60)
    print(f"  Results saved to: {RESULTS_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
