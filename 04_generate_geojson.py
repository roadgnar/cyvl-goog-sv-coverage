"""Convert Street View results CSV to GeoJSON files for the web viewer.

Produces two files:
  - data/freshness.geojson     — individual points with date filtering support
  - data/freshness_hex.geojson — H3 hexagonal grid aggregated by freshness

The hex file uses Uber's H3 spatial index at resolution 7 (~1.2km edge length).
"""

import csv
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import h3

INPUT_PATH = Path("data/sv_results_v2.csv")
POINTS_OUTPUT = Path("data/freshness.geojson")
HEX_OUTPUT = Path("data/freshness_hex.geojson")

# H3 resolution: 7 = ~1.2km edge, good for city-level view
H3_RESOLUTION = 7

STATUS_MAP = {"OK": 0, "ZERO_RESULTS": 1, "NOT_FOUND": 2}

AGE_BUCKET_LABELS = {
    0: "<1 year", 1: "1-3 years", 2: "3-5 years",
    3: ">5 years", 4: "No coverage", 5: "Unknown date",
}


def compute_age_years(sv_date: str) -> float | None:
    if not sv_date:
        return None
    try:
        dt = datetime.strptime(sv_date.strip(), "%Y-%m")
        delta = datetime.now() - dt
        return round(delta.days / 365.25, 1)
    except ValueError:
        return None


def date_to_decimal_year(sv_date: str) -> float | None:
    """Convert YYYY-MM to a decimal year (e.g., 2024.25 for March 2024)."""
    if not sv_date:
        return None
    try:
        dt = datetime.strptime(sv_date.strip(), "%Y-%m")
        # Sanity: reject dates before 2000 or after current year + 1
        if dt.year < 2000 or dt.year > datetime.now().year + 1:
            return None
        year_start = datetime(dt.year, 1, 1)
        year_end = datetime(dt.year + 1, 1, 1)
        frac = (dt - year_start).days / (year_end - year_start).days
        return round(dt.year + frac, 3)
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


def h3_cell_to_geojson_polygon(h3_index: str) -> list:
    """Convert H3 cell to GeoJSON polygon coordinates."""
    boundary = h3.cell_to_boundary(h3_index)
    # h3 returns (lat, lng) tuples, GeoJSON needs [lng, lat]
    coords = [[round(lng, 4), round(lat, 4)] for lat, lng in boundary]
    coords.append(coords[0])  # close the ring
    return [coords]


def main() -> None:
    if not INPUT_PATH.exists():
        print(f"Error: {INPUT_PATH} not found.")
        sys.exit(1)

    # --- Pass 1: Build point features and collect hex cell data ---
    features = []
    bucket_counts: Counter = Counter()
    hex_cells: dict[str, dict] = defaultdict(lambda: {
        "ages": [], "buckets": [], "total": 0, "ok": 0, "no_cov": 0
    })
    skipped = 0
    min_year = 9999.0
    max_year = 0.0

    with open(INPUT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lon = round(float(row["query_lng"]), 4)
                lat = round(float(row["query_lat"]), 4)
            except (KeyError, ValueError, TypeError):
                skipped += 1
                continue

            status_str = row.get("status", "NOT_FOUND")
            status_code = STATUS_MAP.get(status_str, 2)
            sv_date = row.get("sv_date", "").strip()
            age = compute_age_years(sv_date)
            bucket = age_to_bucket(age, status_code)
            dec_year = date_to_decimal_year(sv_date)
            tier = int(row.get("tier", 3))
            city = row.get("city_name", "") if tier != 3 else ""
            state = row.get("state", "")
            try:
                population = int(row.get("population", 0))
            except (ValueError, TypeError):
                population = 0
            if tier == 3:
                population = 0

            bucket_counts[bucket] += 1

            # Track date range
            if dec_year is not None:
                min_year = min(min_year, dec_year)
                max_year = max(max_year, dec_year)

            # Build point feature with decimal year for slider filtering
            props = {"b": bucket, "t": tier}
            if status_code == 0:
                if age is not None:
                    props["a"] = age
                if sv_date:
                    props["d"] = sv_date
                if dec_year is not None:
                    props["y"] = dec_year
            if city:
                props["c"] = city
            if state:
                props["st"] = state
            if population:
                props["p"] = population

            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            })

            # Aggregate into H3 hex cell
            try:
                h3_index = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)
            except Exception:
                continue

            cell = hex_cells[h3_index]
            cell["total"] += 1
            cell["buckets"].append(bucket)
            if status_code == 0:
                cell["ok"] += 1
                if age is not None:
                    cell["ages"].append(age)
            else:
                cell["no_cov"] += 1

    # --- Write point GeoJSON ---
    # Include date range metadata for the slider
    points_geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "minYear": min_year if min_year < 9999 else None,
            "maxYear": max_year if max_year > 0 else None,
        },
        "features": features,
    }

    POINTS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(POINTS_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(points_geojson, f, separators=(",", ":"))

    pts_size = os.path.getsize(POINTS_OUTPUT) / (1024 * 1024)
    print(f"Points: {len(features):,} features, {pts_size:.1f} MB")
    print(f"Date range: {min_year:.1f} - {max_year:.1f}")

    # --- Build H3 hex features ---
    hex_features = []
    for h3_index, cell in hex_cells.items():
        total = cell["total"]
        ok = cell["ok"]
        no_cov = cell["no_cov"]
        ages = cell["ages"]

        avg_age = round(sum(ages) / len(ages), 1) if ages else None
        pct_no_cov = round(no_cov / total * 100) if total else 0

        # Dominant bucket (most common)
        if cell["buckets"]:
            bc = Counter(cell["buckets"])
            dominant = bc.most_common(1)[0][0]
        else:
            dominant = 4

        # Effective bucket based on average age (for coloring)
        if avg_age is not None:
            if avg_age < 1:
                color_bucket = 0
            elif avg_age < 3:
                color_bucket = 1
            elif avg_age < 5:
                color_bucket = 2
            else:
                color_bucket = 3
        elif pct_no_cov > 80:
            color_bucket = 4
        else:
            color_bucket = 5

        try:
            coords = h3_cell_to_geojson_polygon(h3_index)
        except Exception:
            continue

        hex_features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": coords},
            "properties": {
                "b": color_bucket,
                "n": total,
                "ok": ok,
                "nc": pct_no_cov,
                "aa": avg_age,
            },
        })

    hex_geojson = {"type": "FeatureCollection", "features": hex_features}

    with open(HEX_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(hex_geojson, f, separators=(",", ":"))

    hex_size = os.path.getsize(HEX_OUTPUT) / (1024 * 1024)
    print(f"Hexes: {len(hex_features):,} cells, {hex_size:.1f} MB")

    print()
    print("Breakdown by age bucket (points):")
    for bid in sorted(AGE_BUCKET_LABELS.keys()):
        count = bucket_counts.get(bid, 0)
        label = AGE_BUCKET_LABELS[bid]
        pct = (count / len(features) * 100) if features else 0
        print(f"  {label:>15}: {count:>8,}  ({pct:5.1f}%)")


if __name__ == "__main__":
    main()
