"""Convert Street View results CSV to multi-resolution H3 hex GeoJSON files.

Produces three files at different H3 resolutions for tippecanoe/PMTiles:
  - data/hex_r3.geojson — coarse (~12km edge), zoom 3-6
  - data/hex_r5.geojson — medium (~3.5km edge), zoom 7-9
  - data/hex_r7.geojson — fine (~1.2km edge), zoom 10+
"""

import csv
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import h3

INPUT_PATH = Path("data/sv_results_v4.csv")

# Multi-resolution config: (h3_res, minzoom, maxzoom, output_path)
# (h3_res, minzoom, maxzoom, output_path)
RESOLUTIONS = [
    (3, 3, 6, Path("data/hex_r3.geojson")),
    (5, 7, 9, Path("data/hex_r5.geojson")),
    (7, 10, 12, Path("data/hex_r7.geojson")),
    (9, 12, 14, Path("data/hex_r9.geojson")),
]

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


def avg_age_to_bucket(avg_age: float | None, pct_no_cov: int) -> int:
    """Derive color bucket from average age."""
    if avg_age is not None:
        if avg_age < 1:
            return 0
        if avg_age < 3:
            return 1
        if avg_age < 5:
            return 2
        return 3
    if pct_no_cov > 80:
        return 4
    return 5


def h3_cell_to_geojson_polygon(h3_index: str) -> list:
    """Convert H3 cell to GeoJSON polygon coordinates."""
    boundary = h3.cell_to_boundary(h3_index)
    coords = [[round(lng, 6), round(lat, 6)] for lat, lng in boundary]
    coords.append(coords[0])
    return [coords]


def main() -> None:
    if not INPUT_PATH.exists():
        print(f"Error: {INPUT_PATH} not found.")
        sys.exit(1)

    # --- Pass 1: Read all points and aggregate into all resolutions ---
    # hex_data[res][h3_index] = {ages, total, ok, no_cov, tiers}
    hex_data: dict[int, dict] = {}
    for res, _, _, _ in RESOLUTIONS:
        hex_data[res] = defaultdict(lambda: {
            "age_sum": 0.0, "age_count": 0, "total": 0, "ok": 0, "no_cov": 0,
            "buckets": Counter(), "tiers": Counter(),
        })

    bucket_counts: Counter = Counter()
    row_count = 0
    skipped = 0

    print(f"Reading {INPUT_PATH} ...")
    with open(INPUT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lon = float(row["query_lng"])
                lat = float(row["query_lat"])
            except (KeyError, ValueError, TypeError):
                skipped += 1
                continue

            row_count += 1
            status_str = row.get("status", "NOT_FOUND")
            status_code = STATUS_MAP.get(status_str, 2)
            sv_date = row.get("sv_date", "").strip()
            age = compute_age_years(sv_date)
            bucket = age_to_bucket(age, status_code)
            tier = int(row.get("tier", 3))

            bucket_counts[bucket] += 1

            # Aggregate into each resolution
            for res, _, _, _ in RESOLUTIONS:
                try:
                    h3_index = h3.latlng_to_cell(lat, lon, res)
                except Exception:
                    continue

                cell = hex_data[res][h3_index]
                cell["total"] += 1
                cell["buckets"][bucket] += 1
                cell["tiers"][tier] += 1
                if status_code == 0:
                    cell["ok"] += 1
                    if age is not None:
                        cell["age_sum"] += age
                        cell["age_count"] += 1
                else:
                    cell["no_cov"] += 1

    print(f"Read {row_count:,} points (skipped {skipped:,})")

    # --- Pass 2: Build GeoJSON for each resolution ---
    for res, minzoom, maxzoom, output_path in RESOLUTIONS:
        cells = hex_data[res]
        features = []
        dropped = 0

        for h3_index, cell in cells.items():
            total = cell["total"]
            ok = cell["ok"]
            no_cov = cell["no_cov"]

            avg_age = round(cell["age_sum"] / cell["age_count"], 1) if cell["age_count"] > 0 else None
            pct_no_cov = round(no_cov / total * 100) if total else 0
            color_bucket = avg_age_to_bucket(avg_age, pct_no_cov)

            dominant_tier = cell["tiers"].most_common(1)[0][0] if cell["tiers"] else 3

            # Drop low-value cells at fine resolutions
            if res == 7 and color_bucket == 4 and dominant_tier == 3:
                # Res 7: drop rural no-coverage (coarser res covers them)
                dropped += 1
                continue
            if res == 9 and color_bucket == 4 and dominant_tier == 3:
                # Res 9: drop rural no-coverage (same as res 7)
                dropped += 1
                continue

            try:
                coords = h3_cell_to_geojson_polygon(h3_index)
            except Exception:
                continue

            props = {
                "b": color_bucket,
                "n": total,
                "ok": ok,
                "nc": pct_no_cov,
                "t": dominant_tier,
                "r": res,  # H3 resolution for runtime zoom filtering
                "tippecanoe:minzoom": minzoom,
                "tippecanoe:maxzoom": maxzoom,
            }
            if avg_age is not None:
                props["aa"] = avg_age

            features.append({
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": coords},
                "properties": props,
            })

        geojson = {"type": "FeatureCollection", "features": features}

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, separators=(",", ":"))

        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        drop_msg = f" (dropped {dropped:,} rural no-cov)" if dropped else ""
        print(f"  res {res}: {len(features):,} cells, {size_mb:.1f} MB → {output_path}{drop_msg}")

    # --- Summary ---
    print()
    print("Breakdown by age bucket (points):")
    for bid in sorted(AGE_BUCKET_LABELS.keys()):
        count = bucket_counts.get(bid, 0)
        label = AGE_BUCKET_LABELS[bid]
        pct = (count / row_count * 100) if row_count else 0
        print(f"  {label:>15}: {count:>8,}  ({pct:5.1f}%)")


if __name__ == "__main__":
    main()
