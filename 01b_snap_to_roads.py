#!/usr/bin/env python3
"""
01b_snap_to_roads.py

Snap sample points to the nearest TIGER/Line road so that Google Street View
metadata queries hit actual roads instead of parks, rivers, and empty lots.

Downloads Census TIGER/Line county-level road shapefiles, builds a spatial
index (scipy cKDTree), and snaps each point to the nearest road vertex.

Input:  data/sample_points.csv
Output: data/sample_points_snapped.csv
"""

import asyncio
import csv
import math
import os
import sys
import zipfile
from pathlib import Path

import aiohttp
import numpy as np
from scipy.spatial import cKDTree
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
ROADS_DIR = DATA_DIR / "tiger_roads"
INPUT_PATH = DATA_DIR / "sample_points.csv"
OUTPUT_PATH = DATA_DIR / "sample_points_snapped.csv"

# TIGER/Line county roads URL pattern
TIGER_ROADS_URL = "https://www2.census.gov/geo/tiger/TIGER2023/ROADS/tl_2023_{fips}_roads.zip"

# County FIPS reference
COUNTY_FIPS_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"

# CONUS state FIPS codes (exclude AK, HI, territories)
CONUS_STATE_FIPS = {
    "01", "04", "05", "06", "08", "09", "10", "11", "12", "13",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
    "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    "36", "37", "38", "39", "40", "41", "42", "44", "45", "46",
    "47", "48", "49", "50", "51", "53", "54", "55", "56",
}

# Max snap distance in meters
MAX_SNAP_URBAN_M = 500   # Tier 1/2: snap up to 500m
MAX_SNAP_RURAL_M = 5000  # Tier 3: snap up to 5km

# Download concurrency
MAX_CONCURRENT_DOWNLOADS = 80

# Approximate conversions at US mid-latitude (~38°N)
KM_PER_DEG_LAT = 111.0
KM_PER_DEG_LNG = 87.5  # ~111 * cos(38°)
M_PER_DEG_LAT = KM_PER_DEG_LAT * 1000
M_PER_DEG_LNG = KM_PER_DEG_LNG * 1000


# ---------------------------------------------------------------------------
# Step 1: Get list of county FIPS codes
# ---------------------------------------------------------------------------

def get_county_fips_list() -> list[str]:
    """Download and parse the Census county FIPS reference file."""
    import requests

    cache_path = DATA_DIR / "county_fips.txt"
    if cache_path.exists():
        text = cache_path.read_text()
    else:
        print("  Downloading county FIPS list...")
        r = requests.get(COUNTY_FIPS_URL, timeout=30)
        r.raise_for_status()
        text = r.text
        cache_path.write_text(text)

    counties = []
    for line in text.strip().split("\n")[1:]:
        parts = line.split("|")
        if len(parts) >= 3:
            state_fips = parts[1]
            county_fips = parts[2]
            if state_fips in CONUS_STATE_FIPS:
                counties.append(state_fips + county_fips)
    return counties


# ---------------------------------------------------------------------------
# Step 2: Download TIGER road shapefiles
# ---------------------------------------------------------------------------

async def download_county_roads(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    fips: str,
    pbar: tqdm,
) -> bool:
    """Download and extract a single county roads shapefile."""
    zip_path = ROADS_DIR / f"tl_2023_{fips}_roads.zip"
    shp_path = ROADS_DIR / f"tl_2023_{fips}_roads.shp"

    # Skip if already extracted
    if shp_path.exists():
        pbar.update(1)
        return True

    url = TIGER_ROADS_URL.format(fips=fips)

    async with semaphore:
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    pbar.update(1)
                    return False
                resp.raise_for_status()
                data = await resp.read()
        except Exception:
            pbar.update(1)
            return False

    # Write and extract
    zip_path.write_bytes(data)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(ROADS_DIR)
    except zipfile.BadZipFile:
        zip_path.unlink(missing_ok=True)
        pbar.update(1)
        return False

    # Remove zip to save space
    zip_path.unlink(missing_ok=True)
    pbar.update(1)
    return True


async def download_all_roads(county_fips: list[str]) -> int:
    """Download all county road shapefiles in parallel."""
    ROADS_DIR.mkdir(parents=True, exist_ok=True)

    # Check how many are already cached
    cached = sum(1 for f in county_fips if (ROADS_DIR / f"tl_2023_{f}_roads.shp").exists())
    remaining = len(county_fips) - cached
    print(f"  {cached} already cached, {remaining} to download")

    if remaining == 0:
        return cached

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    timeout = aiohttp.ClientTimeout(total=60)
    pbar = tqdm(total=len(county_fips), desc="Downloading roads", unit="county", initial=0)

    downloaded = 0
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            download_county_roads(session, semaphore, fips, pbar)
            for fips in county_fips
        ]
        results = await asyncio.gather(*tasks)
        downloaded = sum(1 for r in results if r)

    pbar.close()
    return downloaded


# ---------------------------------------------------------------------------
# Step 3: Build spatial index from road shapefiles
# ---------------------------------------------------------------------------

def extract_road_vertices_from_shp(shp_path: Path) -> np.ndarray:
    """Extract all vertex coordinates from a TIGER roads shapefile.

    Uses raw binary parsing of the .shp file for speed — avoids the overhead
    of pyshp's full record parsing.  We only need the geometry coordinates.
    """
    import shapefile
    try:
        sf = shapefile.Reader(str(shp_path))
    except Exception:
        return np.empty((0, 2))

    coords = []
    for shape in sf.iterShapes():
        if shape.shapeType in (3, 13, 23):  # PolyLine variants
            for point in shape.points:
                coords.append((point[1], point[0]))  # (lat, lng)

    if not coords:
        return np.empty((0, 2))
    return np.array(coords, dtype=np.float64)


def build_spatial_index(county_fips: list[str]) -> cKDTree:
    """Build a KDTree from all road vertices across all counties."""
    all_coords = []
    shp_files = sorted(ROADS_DIR.glob("tl_2023_*_roads.shp"))

    print(f"  Loading {len(shp_files)} road shapefiles...")
    for shp_path in tqdm(shp_files, desc="Reading roads", unit="file"):
        verts = extract_road_vertices_from_shp(shp_path)
        if len(verts) > 0:
            all_coords.append(verts)

    if not all_coords:
        print("ERROR: No road vertices found!")
        sys.exit(1)

    coords = np.vstack(all_coords)
    print(f"  Total road vertices: {len(coords):,}")

    # Convert to scaled coordinates for distance calculation
    # Scale lng by cos(38°) so KDTree distances approximate meters
    scaled = coords.copy()
    scaled[:, 1] *= KM_PER_DEG_LNG / KM_PER_DEG_LAT  # scale lng

    print("  Building KDTree...")
    tree = cKDTree(scaled)
    print("  KDTree ready.")

    return tree, coords


# ---------------------------------------------------------------------------
# Step 4: Snap points
# ---------------------------------------------------------------------------

def snap_points(tree: cKDTree, road_coords: np.ndarray, points: list[dict]) -> list[dict]:
    """Snap each sample point to the nearest road vertex."""
    # Build query array
    lats = np.array([float(p["lat"]) for p in points])
    lngs = np.array([float(p["lng"]) for p in points])

    query = np.column_stack([lats, lngs * (KM_PER_DEG_LNG / KM_PER_DEG_LAT)])

    print("  Querying KDTree for nearest roads...")
    distances_deg, indices = tree.query(query)

    # Convert tree distance (in scaled degrees) to approximate meters
    distances_m = distances_deg * M_PER_DEG_LAT

    snapped = []
    snap_count = 0
    too_far = 0

    for i, point in enumerate(points):
        tier = int(point.get("tier", 3))
        max_dist = MAX_SNAP_URBAN_M if tier in (1, 2) else MAX_SNAP_RURAL_M
        dist_m = distances_m[i]

        row = dict(point)
        row["orig_lat"] = point["lat"]
        row["orig_lng"] = point["lng"]

        if dist_m <= max_dist:
            snapped_lat, snapped_lng = road_coords[indices[i]]
            row["lat"] = f"{snapped_lat:.6f}"
            row["lng"] = f"{snapped_lng:.6f}"
            row["snap_distance_m"] = f"{dist_m:.1f}"
            snap_count += 1
        else:
            row["snap_distance_m"] = f"{dist_m:.1f}"
            too_far += 1

        snapped.append(row)

    print(f"  Snapped: {snap_count:,} ({snap_count/len(points)*100:.1f}%)")
    print(f"  Too far (kept original): {too_far:,} ({too_far/len(points)*100:.1f}%)")
    return snapped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not INPUT_PATH.exists():
        print(f"Error: {INPUT_PATH} not found. Run 01_prepare_sample_points.py first.")
        sys.exit(1)

    # Load sample points
    print("Step 1: Loading sample points")
    with open(INPUT_PATH, newline="") as f:
        reader = csv.DictReader(f)
        points = list(reader)
    print(f"  Loaded {len(points):,} points")

    # Get county FIPS list
    print("\nStep 2: Getting county FIPS codes")
    county_fips = get_county_fips_list()
    print(f"  {len(county_fips)} CONUS counties")

    # Download roads
    print("\nStep 3: Downloading TIGER/Line road data")
    downloaded = asyncio.run(download_all_roads(county_fips))
    print(f"  {downloaded} county road files ready")

    # Build spatial index
    print("\nStep 4: Building spatial index")
    tree, road_coords = build_spatial_index(county_fips)

    # Snap points
    print("\nStep 5: Snapping points to roads")
    snapped = snap_points(tree, road_coords, points)

    # Write output
    print("\nStep 6: Writing output")
    fieldnames = list(points[0].keys()) + ["orig_lat", "orig_lng", "snap_distance_m"]
    # Dedupe fieldnames (in case of overlap)
    seen = set()
    unique_fields = []
    for f in fieldnames:
        if f not in seen:
            unique_fields.append(f)
            seen.add(f)

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=unique_fields)
        writer.writeheader()
        writer.writerows(snapped)

    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"  Written: {OUTPUT_PATH} ({size_mb:.1f} MB)")

    # Summary stats
    dists = [float(r["snap_distance_m"]) for r in snapped]
    print(f"\n{'='*50}")
    print("SNAP SUMMARY")
    print(f"{'='*50}")
    print(f"  Total points:     {len(snapped):,}")
    print(f"  Median snap dist: {sorted(dists)[len(dists)//2]:.0f}m")
    print(f"  Mean snap dist:   {sum(dists)/len(dists):.0f}m")
    print(f"  <50m:   {sum(1 for d in dists if d < 50):,}")
    print(f"  50-200m: {sum(1 for d in dists if 50 <= d < 200):,}")
    print(f"  200-500m: {sum(1 for d in dists if 200 <= d < 500):,}")
    print(f"  >500m:  {sum(1 for d in dists if d >= 500):,}")
    print(f"{'='*50}")
    print(f"\nNext: run 02_collect_sv_metadata.py to re-query with snapped coordinates")
    print(f"  Update SAMPLE_POINTS_PATH to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
