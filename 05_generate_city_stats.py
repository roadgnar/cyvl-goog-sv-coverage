"""Generate pre-computed city stats JSON for the web viewer.

Replaces the need to load the massive points GeoJSON client-side.
Reads data/sv_results_v4.csv and outputs data/city_stats.json (~200KB).
"""

import csv
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

INPUT_PATH = Path("data/sv_results_v4.csv")
OUTPUT_PATH = Path("data/city_stats.json")

STATUS_MAP = {"OK": 0, "ZERO_RESULTS": 1, "NOT_FOUND": 2}


def compute_age_years(sv_date: str) -> float | None:
    if not sv_date:
        return None
    try:
        dt = datetime.strptime(sv_date.strip(), "%Y-%m")
        if dt.year < 2000 or dt.year > datetime.now().year + 1:
            return None
        delta = datetime.now() - dt
        return round(delta.days / 365.25, 1)
    except ValueError:
        return None


def age_to_bucket(age: float | None, status_code: int) -> int:
    if status_code != 0:
        return 4
    if age is None:
        return 5
    if age < 1:
        return 0
    if age < 3:
        return 1
    if age < 5:
        return 2
    return 3


def make_key(city: str, state: str) -> str:
    """Normalized key for city lookup: lowercase alphanumeric + hyphen separator."""
    city_clean = re.sub(r"[^a-z0-9]", "", city.lower())
    state_clean = state.lower().strip()
    return f"{city_clean}-{state_clean}"


def main() -> None:
    if not INPUT_PATH.exists():
        print(f"Error: {INPUT_PATH} not found.")
        sys.exit(1)

    # Accumulate per-city data
    city_data: dict[str, dict] = defaultdict(lambda: {
        "city": "", "state": "", "total": 0, "buckets": Counter(),
        "ages": [], "lat_sum": 0.0, "lng_sum": 0.0, "population": 0,
        "tier": 3, "oldest_date": None, "newest_date": None,
    })

    national_buckets: Counter = Counter()
    national_ages: list[float] = []
    total_rows = 0

    print(f"Reading {INPUT_PATH} ...")
    with open(INPUT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lat = float(row["query_lat"])
                lon = float(row["query_lng"])
            except (KeyError, ValueError, TypeError):
                continue

            total_rows += 1
            status_str = row.get("status", "NOT_FOUND")
            status_code = STATUS_MAP.get(status_str, 2)
            sv_date = row.get("sv_date", "").strip()
            age = compute_age_years(sv_date)
            bucket = age_to_bucket(age, status_code)
            tier = int(row.get("tier", 3))
            city_name = row.get("city_name", "").strip()
            state = row.get("state", "").strip()

            national_buckets[bucket] += 1
            if age is not None:
                national_ages.append(age)

            # Only aggregate cities (tier 1 & 2)
            if tier == 3 or not city_name:
                continue

            key = make_key(city_name, state)
            cd = city_data[key]
            cd["city"] = city_name
            cd["state"] = state
            cd["total"] += 1
            cd["buckets"][bucket] += 1
            cd["lat_sum"] += lat
            cd["lng_sum"] += lon
            if age is not None:
                cd["ages"].append(age)
            cd["tier"] = tier

            try:
                pop = int(row.get("population", 0))
                if pop > cd["population"]:
                    cd["population"] = pop
            except (ValueError, TypeError):
                pass

            # Track date range
            if sv_date and status_code == 0:
                if cd["oldest_date"] is None or sv_date < cd["oldest_date"]:
                    cd["oldest_date"] = sv_date
                if cd["newest_date"] is None or sv_date > cd["newest_date"]:
                    cd["newest_date"] = sv_date

    # Build national stats
    national_ages.sort()
    national = {
        "total": total_rows,
        "buckets": [national_buckets.get(i, 0) for i in range(6)],
        "medianAge": round(statistics.median(national_ages), 1) if national_ages else None,
        "avgAge": round(statistics.mean(national_ages), 1) if national_ages else None,
    }

    # Build city entries
    cities = {}
    for key, cd in city_data.items():
        total = cd["total"]
        if total == 0:
            continue

        ages = cd["ages"]
        avg_age = round(statistics.mean(ages), 1) if ages else None
        median_age = round(statistics.median(ages), 1) if ages else None
        oldest = round(max(ages), 1) if ages else None

        cities[key] = {
            "city": cd["city"],
            "state": cd["state"],
            "total": total,
            "population": cd["population"],
            "tier": cd["tier"],
            "buckets": [cd["buckets"].get(i, 0) for i in range(6)],
            "avgAge": avg_age,
            "medianAge": median_age,
            "oldest": oldest,
            "lat": round(cd["lat_sum"] / total, 4),
            "lng": round(cd["lng_sum"] / total, 4),
        }

    result = {"national": national, "cities": cities}

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, separators=(",", ":"))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"Wrote {OUTPUT_PATH} ({size_kb:.0f} KB, {len(cities):,} cities)")
    print(f"National: {national['total']:,} points, median age {national['medianAge']}yr, avg {national['avgAge']}yr")
    print(f"Buckets: {national['buckets']}")


if __name__ == "__main__":
    main()
