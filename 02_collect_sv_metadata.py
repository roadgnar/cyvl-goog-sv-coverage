#!/usr/bin/env python3
"""
02_collect_sv_metadata.py

Queries the Google Street View Static API metadata endpoint for all sample points.
Supports resumption from previous runs by checking existing results.

Usage:
    python 02_collect_sv_metadata.py

Requires:
    - data/sample_points.csv with columns: point_id, lat, lng, tier, city_name, state, population
    - .env file with GOOGLE_API_KEY=your_key_here
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
SAMPLE_POINTS_ORIGINAL = DATA_DIR / "sample_points.csv"
SAMPLE_POINTS_SNAPPED = DATA_DIR / "sample_points_snapped.csv"
# Use snapped points if available, otherwise fall back to original
SAMPLE_POINTS_PATH = SAMPLE_POINTS_SNAPPED if SAMPLE_POINTS_SNAPPED.exists() else SAMPLE_POINTS_ORIGINAL
RESULTS_PATH = DATA_DIR / "sv_results_v3.csv"
ENV_PATH = Path(__file__).parent / ".env"

API_BASE_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"

# Concurrency and rate limiting
MAX_CONCURRENT = 5           # semaphore limit for parallel requests
RATE_LIMIT_PER_MIN = 1_500   # conservative rate to avoid UNKNOWN_ERROR
FLUSH_INTERVAL = 100         # flush CSV writer every N results
LOG_INTERVAL = 1000          # print summary every N points

# Retry configuration
MAX_RETRIES = 5
BACKOFF_BASE = 3             # exponential backoff base in seconds
OVER_LIMIT_PAUSE = 60        # seconds to pause on OVER_QUERY_LIMIT

# Output CSV columns
RESULT_COLUMNS = [
    "point_id", "query_lat", "query_lng", "status",
    "sv_date", "pano_id", "sv_lat", "sv_lng",
    "tier", "city_name", "state", "population",
]

# ---------------------------------------------------------------------------
# Logging setup
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


def load_env(path: Path) -> dict[str, str]:
    """Parse a .env file into a dict. Handles KEY=VALUE lines, ignores comments."""
    env = {}
    if not path.exists():
        return env
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                # Strip optional surrounding quotes
                value = value.strip().strip("'\"")
                env[key.strip()] = value
    return env


def load_completed_ids(path: Path) -> set[str]:
    """Load point_ids that have already been queried from the results CSV."""
    completed = set()
    if not path.exists():
        return completed
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            completed.add(row["point_id"])
    return completed


def load_sample_points(path: Path) -> list[dict]:
    """Load sample points from CSV. Returns list of dicts."""
    points = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            points.append(row)
    return points


def ensure_results_header(path: Path) -> None:
    """Write CSV header if the results file doesn't exist yet."""
    if not path.exists():
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)
            writer.writeheader()


def parse_response(data: dict, point: dict) -> dict:
    """
    Parse the Street View metadata API response into a result row.
    Merges API response fields with the original point metadata.
    """
    status = data.get("status", "UNKNOWN")

    # Base row with query coordinates and point metadata
    row = {
        "point_id": point["point_id"],
        "query_lat": point["lat"],
        "query_lng": point["lng"],
        "status": status,
        "sv_date": "",
        "pano_id": "",
        "sv_lat": "",
        "sv_lng": "",
        "tier": point.get("tier", ""),
        "city_name": point.get("city_name", ""),
        "state": point.get("state", ""),
        "population": point.get("population", ""),
    }

    # Extract imagery fields when status is OK
    if status == "OK":
        row["sv_date"] = data.get("date", "")  # may be missing on old imagery
        row["pano_id"] = data.get("pano_id", "")
        location = data.get("location", {})
        row["sv_lat"] = location.get("lat", "")
        row["sv_lng"] = location.get("lng", "")

    return row


# ---------------------------------------------------------------------------
# Async query logic
# ---------------------------------------------------------------------------


async def query_single_point(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    rate_limiter: asyncio.Semaphore,
    point: dict,
    api_key: str,
) -> dict | None:
    """
    Query the Street View metadata endpoint for a single point.
    Handles retries with exponential backoff and OVER_QUERY_LIMIT pauses.
    Returns a parsed result row, or None if REQUEST_DENIED (caller should stop).
    """
    params = {
        "location": f"{point['lat']},{point['lng']}",
        "key": api_key,
    }

    for attempt in range(MAX_RETRIES):
        async with semaphore:
            # Rate limiting: acquire a token before making the request.
            # Tokens are released by the refill task at the configured rate.
            await rate_limiter.acquire()

            try:
                async with session.get(API_BASE_URL, params=params) as resp:
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Network error — retry with backoff
                wait = BACKOFF_BASE ** attempt
                log.warning(
                    "Network error for point %s (attempt %d/%d): %s. "
                    "Retrying in %ds...",
                    point["point_id"], attempt + 1, MAX_RETRIES, e, wait,
                )
                await asyncio.sleep(wait)
                continue

        status = data.get("status", "UNKNOWN")

        # Successful statuses — return the parsed result
        if status in ("OK", "ZERO_RESULTS", "NOT_FOUND"):
            return parse_response(data, point)

        # Over quota or server-side throttle — pause and retry
        if status in ("OVER_QUERY_LIMIT", "UNKNOWN_ERROR"):
            log.warning(
                "%s for point %s. Pausing %ds before retry...",
                status, point["point_id"], OVER_LIMIT_PAUSE,
            )
            await asyncio.sleep(OVER_LIMIT_PAUSE)
            continue

        # Bad API key — signal caller to stop everything
        if status == "REQUEST_DENIED":
            log.error(
                "REQUEST_DENIED — API key is invalid or restricted. "
                "Error: %s", data.get("error_message", "unknown"),
            )
            return None

        # Unknown status — treat as transient error, retry
        wait = BACKOFF_BASE ** attempt
        log.warning(
            "Unexpected status '%s' for point %s (attempt %d/%d). "
            "Retrying in %ds...",
            status, point["point_id"], attempt + 1, MAX_RETRIES, wait,
        )
        await asyncio.sleep(wait)

    # All retries exhausted — record as error
    log.error(
        "All %d retries exhausted for point %s", MAX_RETRIES, point["point_id"]
    )
    return {
        "point_id": point["point_id"],
        "query_lat": point["lat"],
        "query_lng": point["lng"],
        "status": "ERROR",
        "sv_date": "",
        "pano_id": "",
        "sv_lat": "",
        "sv_lng": "",
        "tier": point.get("tier", ""),
        "city_name": point.get("city_name", ""),
        "state": point.get("state", ""),
        "population": point.get("population", ""),
    }


async def rate_refill_task(rate_limiter: asyncio.Semaphore, stop_event: asyncio.Event):
    """
    Continuously refill the rate limiter semaphore to enforce the per-minute
    request cap. Releases tokens at a steady interval so requests are spread
    evenly across each second.
    """
    # Calculate how often to release a token.
    # E.g. 30K/min = 500/sec -> release one token every 2ms
    tokens_per_sec = RATE_LIMIT_PER_MIN / 60.0
    interval = 1.0 / tokens_per_sec

    while not stop_event.is_set():
        # Only refill if below the max (avoid unbounded growth)
        try:
            rate_limiter.release()
        except ValueError:
            pass  # semaphore at max, skip
        await asyncio.sleep(interval)


async def run_collection(points: list[dict], api_key: str) -> dict:
    """
    Main collection loop. Queries all points concurrently within rate limits.
    Appends results to the CSV incrementally.

    Returns a summary dict with counts.
    """
    total = len(points)
    log.info("Starting collection for %d points", total)

    # Counters for summary
    counts = {"ok": 0, "zero_results": 0, "not_found": 0, "error": 0}

    # Semaphore for max concurrent HTTP connections
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # Rate limiter semaphore — starts with a burst allowance, refilled continuously
    rate_limiter = asyncio.Semaphore(MAX_CONCURRENT)

    # Event to signal the refill task to stop
    stop_event = asyncio.Event()

    # Start the rate refill background task
    refill = asyncio.create_task(rate_refill_task(rate_limiter, stop_event))

    # Open results file in append mode
    results_file = open(RESULTS_PATH, "a", newline="")
    writer = csv.DictWriter(results_file, fieldnames=RESULT_COLUMNS)
    pending_writes = 0
    start_time = time.monotonic()
    request_denied = False

    # Timeout for individual requests (generous to handle slow responses)
    timeout = aiohttp.ClientTimeout(total=30)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Create tasks for all points
            tasks = []
            for point in points:
                task = asyncio.create_task(
                    query_single_point(session, semaphore, rate_limiter, point, api_key)
                )
                tasks.append((point, task))

            # Process results as they complete, with a progress bar
            pbar = tqdm(total=total, desc="Querying SV metadata", unit="pts")
            completed = 0

            for point, task in tasks:
                result = await task

                # REQUEST_DENIED returns None — stop all further work
                if result is None:
                    request_denied = True
                    log.error("Stopping due to REQUEST_DENIED.")
                    # Cancel remaining tasks
                    for _, remaining_task in tasks:
                        remaining_task.cancel()
                    break

                # Write result row
                writer.writerow(result)
                pending_writes += 1

                # Update counters
                status = result["status"]
                if status == "OK":
                    counts["ok"] += 1
                elif status in ("ZERO_RESULTS", "NOT_FOUND"):
                    counts["zero_results"] += 1
                else:
                    counts["error"] += 1

                completed += 1
                pbar.update(1)

                # Periodic flush to protect against interruption
                if pending_writes >= FLUSH_INTERVAL:
                    results_file.flush()
                    pending_writes = 0

                # Periodic log summary
                if completed % LOG_INTERVAL == 0:
                    elapsed = time.monotonic() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    pct = (completed / total) * 100
                    log.info(
                        "Progress: %d/%d (%.1f%%) | Rate: %.1f pts/sec | "
                        "OK: %d | No imagery: %d | Errors: %d",
                        completed, total, pct, rate,
                        counts["ok"], counts["zero_results"], counts["error"],
                    )

            pbar.close()

    finally:
        # Final flush and cleanup
        results_file.flush()
        results_file.close()
        stop_event.set()
        refill.cancel()
        try:
            await refill
        except asyncio.CancelledError:
            pass

    elapsed = time.monotonic() - start_time
    counts["total"] = completed
    counts["elapsed"] = elapsed
    counts["request_denied"] = request_denied
    return counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main():
    # Load API key from .env
    env = load_env(ENV_PATH)
    api_key = env.get("GOOGLE_API_KEY", os.environ.get("GOOGLE_API_KEY", ""))
    if not api_key:
        log.error(
            "No GOOGLE_API_KEY found. Set it in .env or as an environment variable."
        )
        sys.exit(1)

    log.info("API key loaded (ends with ...%s)", api_key[-4:])

    # Load sample points
    if not SAMPLE_POINTS_PATH.exists():
        log.error("Sample points file not found: %s", SAMPLE_POINTS_PATH)
        sys.exit(1)

    all_points = load_sample_points(SAMPLE_POINTS_PATH)
    log.info("Loaded %d sample points from %s", len(all_points), SAMPLE_POINTS_PATH)

    # Resume capability: skip already-queried points
    completed_ids = load_completed_ids(RESULTS_PATH)
    if completed_ids:
        log.info("Found %d already-queried points in %s", len(completed_ids), RESULTS_PATH)

    remaining = [p for p in all_points if p["point_id"] not in completed_ids]
    log.info("%d points remaining to query", len(remaining))

    if not remaining:
        log.info("All points already queried. Nothing to do.")
        return

    # Ensure CSV header exists (for fresh starts)
    ensure_results_header(RESULTS_PATH)

    # Run the async collection
    summary = await run_collection(remaining, api_key)

    # Print final summary
    elapsed = summary.get("elapsed", 0)
    rate = summary["total"] / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print("COLLECTION SUMMARY")
    print("=" * 60)
    print(f"  Total queried:    {summary['total']}")
    print(f"  OK (has imagery): {summary['ok']}")
    print(f"  No imagery:       {summary['zero_results']}")
    print(f"  Errors:           {summary['error']}")
    print(f"  Time elapsed:     {elapsed:.1f}s")
    print(f"  Average rate:     {rate:.1f} points/sec")
    if summary.get("request_denied"):
        print("  WARNING: Stopped early due to REQUEST_DENIED (bad API key)")
    print("=" * 60)
    print(f"  Results saved to: {RESULTS_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
