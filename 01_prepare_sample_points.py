#!/usr/bin/env python3
"""
01_prepare_sample_points.py

Downloads US Census data (Gazetteer places + population estimates + place boundaries +
state boundaries), joins them, and generates sample points using ACTUAL CITY POLYGONS
for Tier 1/2 cities and a land-filtered hex grid for rural areas.

Tier structure:
  - Tier 1: Cities 50K+ pop -> dense grid within actual city boundary polygon
  - Tier 2: Cities 10K-50K pop -> sparser grid within actual city boundary polygon
  - Tier 3: Rural hex grid at 10km spacing across continental US land only

Output: data/sample_points.csv
"""

import json
import math
import os
import zipfile
from pathlib import Path

import pandas as pd
import requests
import shapefile  # pyshp
from shapely.geometry import Point, shape, box
from shapely.ops import unary_union
from shapely.prepared import prep
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"

GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2024_Gazetteer/2024_Gaz_place_national.zip"
)
POPULATION_URL = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2020-2023/cities/totals/sub-est2023.csv"
)
# US states GeoJSON for land filtering (rural grid)
STATES_GEOJSON_URL = (
    "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/"
    "master/data/geojson/us-states.json"
)
# Census cartographic boundary place shapefile - actual city polygons
PLACE_BOUNDARY_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_place_500k.zip"
)
# Census cartographic boundary county subdivision (MCD/township) shapefile
COUSUB_BOUNDARY_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_cousub_500k.zip"
)

GAZETTEER_ZIP = DATA_DIR / "2024_Gaz_place_national.zip"
GAZETTEER_FILE = DATA_DIR / "2024_Gaz_place_national.txt"
POPULATION_FILE = DATA_DIR / "sub-est2023.csv"
STATES_GEOJSON_FILE = DATA_DIR / "us-states.json"
PLACE_BOUNDARY_ZIP = DATA_DIR / "cb_2023_us_place_500k.zip"
PLACE_BOUNDARY_DIR = DATA_DIR / "place_boundaries"
COUSUB_BOUNDARY_ZIP = DATA_DIR / "cb_2023_us_cousub_500k.zip"
COUSUB_BOUNDARY_DIR = DATA_DIR / "cousub_boundaries"
OUTPUT_FILE = DATA_DIR / "sample_points.csv"

# Continental US bounding box
LAT_MIN, LAT_MAX = 24.5, 49.5
LNG_MIN, LNG_MAX = -125.0, -66.5

# Tier thresholds
TIER1_MIN_POP = 50_000
TIER2_MIN_POP = 10_000

# Grid spacing within city polygons (km), by population
# Much denser now that we use streetlevel (no API cost/rate limits)
CITY_GRID_SPACING = [
    (1_000_000, 0.15),  # 1M+   -> 150m grid (block-level in NYC, LA, Chicago etc)
    (500_000,   0.2),   # 500K+ -> 200m grid
    (250_000,   0.25),  # 250K+ -> 250m grid
    (100_000,   0.3),   # 100K+ -> 300m grid
    (50_000,    0.4),   # 50K+  -> 400m grid
    (10_000,    0.5),   # 10K+  -> 500m grid
]

# Rural hex grid spacing (km) — tighter for better national coverage
HEX_SPACING_KM = 3.0

# Approximate conversions
KM_PER_DEG_LAT = 111.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def km_per_deg_lng(lat: float) -> float:
    """Approximate km per degree of longitude at a given latitude."""
    return 111.0 * math.cos(math.radians(lat))


def download_file(url: str, dest: Path, description: str) -> None:
    """Download a file with progress bar, skipping if already cached."""
    if dest.exists():
        print(f"  [cached] {dest.name}")
        return

    print(f"  Downloading {description}...")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc=dest.name
    ) as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))


def in_conus(lat: float, lng: float) -> bool:
    """Check whether a point falls within the continental US bounding box."""
    return LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX


def get_grid_spacing_km(population: int) -> float:
    """Return grid spacing in km for a city based on its population."""
    for pop_threshold, spacing in CITY_GRID_SPACING:
        if population >= pop_threshold:
            return spacing
    return 1.0


# ---------------------------------------------------------------------------
# Step 1: Download all data
# ---------------------------------------------------------------------------


def download_all_data():
    """Download Gazetteer, population, state boundaries, and place boundaries."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("Step 1: Downloading data")

    # Gazetteer
    if GAZETTEER_FILE.exists():
        print(f"  [cached] {GAZETTEER_FILE.name}")
    else:
        download_file(GAZETTEER_URL, GAZETTEER_ZIP, "Gazetteer places (zip)")
        print("  Extracting Gazetteer zip...")
        with zipfile.ZipFile(GAZETTEER_ZIP) as zf:
            txt_names = [n for n in zf.namelist() if n.endswith(".txt")]
            if not txt_names:
                raise RuntimeError(f"No .txt file found in {GAZETTEER_ZIP}")
            zf.extract(txt_names[0], DATA_DIR)
            extracted = DATA_DIR / txt_names[0]
            if extracted != GAZETTEER_FILE:
                extracted.rename(GAZETTEER_FILE)

    # Population estimates
    download_file(POPULATION_URL, POPULATION_FILE, "Population estimates")

    # US states GeoJSON (for rural grid land filtering)
    download_file(STATES_GEOJSON_URL, STATES_GEOJSON_FILE, "US states GeoJSON")

    # Place boundary shapefile (actual city polygons)
    if PLACE_BOUNDARY_DIR.exists():
        print(f"  [cached] place_boundaries/")
    else:
        download_file(PLACE_BOUNDARY_URL, PLACE_BOUNDARY_ZIP, "Place boundaries (23MB)")
        print("  Extracting place boundaries...")
        PLACE_BOUNDARY_DIR.mkdir(exist_ok=True)
        with zipfile.ZipFile(PLACE_BOUNDARY_ZIP) as zf:
            zf.extractall(PLACE_BOUNDARY_DIR)

    # County subdivision (MCD/township) boundary shapefile
    if COUSUB_BOUNDARY_DIR.exists():
        print(f"  [cached] cousub_boundaries/")
    else:
        download_file(COUSUB_BOUNDARY_URL, COUSUB_BOUNDARY_ZIP, "MCD/township boundaries (39MB)")
        print("  Extracting MCD boundaries...")
        COUSUB_BOUNDARY_DIR.mkdir(exist_ok=True)
        with zipfile.ZipFile(COUSUB_BOUNDARY_ZIP) as zf:
            zf.extractall(COUSUB_BOUNDARY_DIR)


def load_us_land_polygon():
    """Load US states GeoJSON and return a prepared shapely geometry for land testing."""
    print("\n  Loading US land boundary...")
    with open(STATES_GEOJSON_FILE) as f:
        data = json.load(f)

    state_geoms = []
    for feature in data["features"]:
        geom = shape(feature["geometry"])
        c = geom.centroid
        if LAT_MIN <= c.y <= LAT_MAX and LNG_MIN <= c.x <= LNG_MAX:
            state_geoms.append(geom)

    us_land = unary_union(state_geoms)
    prepared = prep(us_land)
    print(f"  Loaded {len(state_geoms)} state boundaries")
    return prepared


def load_place_boundaries() -> dict:
    """
    Load Census place boundary shapefile. Returns dict of GEOID -> shapely polygon.
    GEOID format is state_fips (2 digits) + place_fips (5 digits).
    """
    print("  Loading place boundary polygons...")

    # Find the .shp file in the extracted directory
    shp_files = list(PLACE_BOUNDARY_DIR.glob("*.shp"))
    if not shp_files:
        raise RuntimeError(f"No .shp file found in {PLACE_BOUNDARY_DIR}")

    sf = shapefile.Reader(str(shp_files[0]))
    fields = [f[0] for f in sf.fields[1:]]  # skip DeletionFlag

    geoid_idx = fields.index("GEOID") if "GEOID" in fields else None
    if geoid_idx is None:
        # Try PLACEFP + STATEFP
        statefp_idx = fields.index("STATEFP")
        placefp_idx = fields.index("PLACEFP")
    else:
        statefp_idx = None
        placefp_idx = None

    boundaries = {}
    for sr in sf.iterShapeRecords():
        rec = sr.record
        if geoid_idx is not None:
            geoid = str(rec[geoid_idx]).zfill(7)
        else:
            geoid = str(rec[statefp_idx]).zfill(2) + str(rec[placefp_idx]).zfill(5)

        try:
            geom = shape(sr.shape.__geo_interface__)
            if geom.is_valid and not geom.is_empty:
                boundaries[geoid] = geom
        except Exception:
            continue

    print(f"  Loaded {len(boundaries):,} place boundary polygons")
    return boundaries


def load_cousub_boundaries() -> dict:
    """
    Load Census county subdivision (MCD) boundary shapefile.
    Returns dict of GEOID -> shapely polygon.
    GEOID format is state_fips (2) + county_fips (3) + cousub_fips (5) = 10 chars.
    """
    print("  Loading MCD/township boundary polygons...")
    shp_files = list(COUSUB_BOUNDARY_DIR.glob("*.shp"))
    if not shp_files:
        raise RuntimeError(f"No .shp file found in {COUSUB_BOUNDARY_DIR}")

    sf = shapefile.Reader(str(shp_files[0]))
    fields = [f[0] for f in sf.fields[1:]]

    geoid_idx = fields.index("GEOID") if "GEOID" in fields else None
    if geoid_idx is None:
        statefp_idx = fields.index("STATEFP")
        countyfp_idx = fields.index("COUNTYFP")
        cousubfp_idx = fields.index("COUSUBFP")
    else:
        statefp_idx = countyfp_idx = cousubfp_idx = None

    boundaries = {}
    for sr in sf.iterShapeRecords():
        rec = sr.record
        if geoid_idx is not None:
            geoid = str(rec[geoid_idx]).zfill(10)
        else:
            geoid = (str(rec[statefp_idx]).zfill(2)
                     + str(rec[countyfp_idx]).zfill(3)
                     + str(rec[cousubfp_idx]).zfill(5))
        try:
            geom = shape(sr.shape.__geo_interface__)
            if geom.is_valid and not geom.is_empty:
                boundaries[geoid] = geom
        except Exception:
            continue

    print(f"  Loaded {len(boundaries):,} MCD/township boundary polygons")
    return boundaries


# ---------------------------------------------------------------------------
# Step 2: Load and join data
# ---------------------------------------------------------------------------


def load_and_join(cousub_boundaries: dict) -> pd.DataFrame:
    """
    Load Gazetteer and population estimates, join on FIPS codes.
    Includes both Census places (SUMLEV 162) and MCDs/townships (SUMLEV 061).
    Returns DataFrame with city info for continental US.
    """
    print("\nStep 2: Loading and joining Census data")

    # --- Load Gazetteer (for places) ---
    gaz = pd.read_csv(
        GAZETTEER_FILE,
        sep="\t",
        encoding="latin-1",
        dtype={"GEOID": str, "ANSICODE": str},
    )
    gaz.columns = gaz.columns.str.strip()
    print(f"  Gazetteer: {len(gaz)} places loaded")

    gaz["STATE_FIPS"] = gaz["GEOID"].str[:2]
    gaz["PLACE_FIPS"] = gaz["GEOID"].str[2:]

    # --- Load population estimates ---
    pop = pd.read_csv(
        POPULATION_FILE,
        encoding="latin-1",
        dtype={"STATE": str, "PLACE": str, "COUNTY": str, "COUSUB": str},
    )
    print(f"  Population estimates: {len(pop)} rows loaded")

    pop["STATE"] = pop["STATE"].str.zfill(2)
    pop["PLACE"] = pop["PLACE"].str.zfill(5)

    pop_col = "POPESTIMATE2023"
    if pop_col not in pop.columns:
        pop_cols = [c for c in pop.columns if c.startswith("POPESTIMATE")]
        pop_col = sorted(pop_cols)[-1] if pop_cols else None
        if pop_col is None:
            raise ValueError("No POPESTIMATE column found")
    print(f"  Using population column: {pop_col}")

    # === Part A: Census Places (SUMLEV 162) — original approach ===
    pop_places = pop[pop["SUMLEV"].astype(str) == "162"].copy()

    merged_places = gaz.merge(
        pop_places[["STATE", "PLACE", "NAME", pop_col]],
        left_on=["STATE_FIPS", "PLACE_FIPS"],
        right_on=["STATE", "PLACE"],
        how="inner",
    )
    merged_places = merged_places.rename(columns={pop_col: "population"})
    merged_places["lat"] = pd.to_numeric(merged_places["INTPTLAT"], errors="coerce")
    merged_places["lng"] = pd.to_numeric(merged_places["INTPTLONG"], errors="coerce")
    merged_places["city_name"] = merged_places["NAME_x"] if "NAME_x" in merged_places.columns else merged_places["NAME"]
    merged_places["state"] = merged_places["USPS"]
    merged_places["geoid"] = merged_places["STATE_FIPS"] + merged_places["PLACE_FIPS"].str.zfill(5)
    merged_places["source"] = "place"

    places_df = merged_places[["city_name", "state", "population", "lat", "lng", "geoid", "source"]].copy()
    print(f"  Census places: {len(places_df)} matched")

    # === Part B: MCDs/Townships (SUMLEV 061) ===
    pop_mcds = pop[pop["SUMLEV"].astype(str) == "61"].copy()
    pop_mcds["COUNTY"] = pop_mcds["COUNTY"].str.zfill(3)
    pop_mcds["COUSUB"] = pop_mcds["COUSUB"].str.zfill(5)
    pop_mcds["mcd_geoid"] = pop_mcds["STATE"] + pop_mcds["COUNTY"] + pop_mcds["COUSUB"]
    pop_mcds = pop_mcds.rename(columns={pop_col: "population"})

    # Get lat/lng from MCD boundary shapefile centroids
    mcd_rows = []
    for _, row in pop_mcds.iterrows():
        geoid = row["mcd_geoid"]
        boundary = cousub_boundaries.get(geoid)
        if boundary is None:
            continue
        centroid = boundary.centroid
        mcd_rows.append({
            "city_name": row["NAME"],
            "state": row["STNAME"],
            "population": row["population"],
            "lat": round(centroid.y, 6),
            "lng": round(centroid.x, 6),
            "geoid": geoid,
            "source": "mcd",
        })
    mcds_df = pd.DataFrame(mcd_rows)

    # Build state name -> abbreviation from Gazetteer (which has USPS codes)
    gaz_states = gaz[["STATE_FIPS", "USPS"]].drop_duplicates()
    # Also build from pop data state FIPS
    pop_state_names = pop[["STATE", "STNAME"]].drop_duplicates()
    state_fips_to_abbrev = dict(zip(gaz_states["STATE_FIPS"], gaz_states["USPS"]))
    state_name_to_fips = dict(zip(pop_state_names["STNAME"], pop_state_names["STATE"]))
    state_name_to_abbrev = {
        name: state_fips_to_abbrev.get(fips, "")
        for name, fips in state_name_to_fips.items()
    }
    mcds_df["state"] = mcds_df["state"].map(state_name_to_abbrev)
    mcds_df = mcds_df.dropna(subset=["state"])
    mcds_df = mcds_df[mcds_df["state"] != ""]

    print(f"  MCDs/townships: {len(mcds_df)} with boundaries")

    # === Deduplicate: keep places, add MCDs not already covered ===
    # Match on normalized name + state to find overlaps
    def normalize(name):
        import re
        return re.sub(r"\s+(city|town|village|CDP|borough|township)$", "", name, flags=re.IGNORECASE).strip().lower()

    place_keys = set(zip(places_df["city_name"].apply(normalize), places_df["state"]))
    mask = mcds_df.apply(lambda r: (normalize(r["city_name"]), r["state"]) not in place_keys, axis=1)
    new_mcds = mcds_df[mask].copy()
    print(f"  MCDs not already in places: {len(new_mcds)}")

    # === Combine ===
    combined = pd.concat([places_df, new_mcds], ignore_index=True)

    # Filter to continental US
    conus_mask = combined.apply(lambda r: in_conus(r["lat"], r["lng"]), axis=1)
    combined = combined[conus_mask].copy()
    print(f"  Continental US total: {len(combined)} communities")

    # Assign tiers
    combined["tier"] = 3
    combined.loc[combined["population"] >= TIER2_MIN_POP, "tier"] = 2
    combined.loc[combined["population"] >= TIER1_MIN_POP, "tier"] = 1

    tier_counts = combined["tier"].value_counts().sort_index()
    for t, c in tier_counts.items():
        print(f"    Tier {t}: {c} cities/towns")

    return combined[["city_name", "state", "population", "lat", "lng", "tier", "geoid", "source"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 3: Generate sample points
# ---------------------------------------------------------------------------


def generate_grid_within_polygon(polygon, spacing_km: float) -> list[tuple[float, float]]:
    """
    Generate a regular grid of (lat, lng) points within a shapely polygon.
    Spacing is approximate in km using flat-earth conversion.
    """
    bounds = polygon.bounds  # (minx, miny, maxx, maxy) = (min_lng, min_lat, max_lng, max_lat)
    min_lng, min_lat, max_lng, max_lat = bounds

    center_lat = (min_lat + max_lat) / 2.0
    lat_step = spacing_km / KM_PER_DEG_LAT
    lng_step = spacing_km / km_per_deg_lng(center_lat)

    # Prepare polygon for fast contains checks
    prepared_poly = prep(polygon)

    points = []
    lat = min_lat
    while lat <= max_lat:
        lng = min_lng
        while lng <= max_lng:
            if prepared_poly.contains(Point(lng, lat)):
                points.append((round(lat, 6), round(lng, 6)))
            lng += lng_step
        lat += lat_step

    return points


def generate_city_points(cities: pd.DataFrame, boundaries: dict, tier: int) -> list[dict]:
    """
    Generate grid points within actual city boundary polygons.
    Falls back to a simple radius-based grid if no boundary polygon is found.
    """
    tier_cities = cities[cities["tier"] == tier]
    all_points = []
    matched = 0
    fallback = 0

    for _, city in tqdm(tier_cities.iterrows(), total=len(tier_cities), desc=f"  Tier {tier}"):
        population = int(city["population"])
        spacing_km = get_grid_spacing_km(population)
        geoid = city["geoid"]

        base = {
            "tier": tier,
            "city_name": city["city_name"],
            "state": city["state"],
            "population": population,
        }

        polygon = boundaries.get(geoid)

        if polygon is not None:
            matched += 1
            grid_points = generate_grid_within_polygon(polygon, spacing_km)
            for lat, lng in grid_points:
                all_points.append({**base, "lat": lat, "lng": lng, "point_type": "grid"})
        else:
            # Fallback: simple grid around centroid (3km radius for tier 1, 1.5km for tier 2)
            fallback += 1
            clat, clng = city["lat"], city["lng"]
            radius_km = 3.0 if tier == 1 else 1.5
            steps = int(radius_km / spacing_km)

            for dy in range(-steps, steps + 1):
                for dx in range(-steps, steps + 1):
                    dist = math.sqrt(dx**2 + dy**2) * spacing_km
                    if dist > radius_km:
                        continue
                    plat = clat + (dy * spacing_km) / KM_PER_DEG_LAT
                    plng = clng + (dx * spacing_km) / km_per_deg_lng(clat)
                    all_points.append({**base, "lat": plat, "lng": plng, "point_type": "grid"})

    print(f"    Boundary matched: {matched}, fallback: {fallback}")
    return all_points


def generate_hex_grid(us_land) -> list[dict]:
    """
    Tier 3: Hex grid across continental US at 10km spacing, land-only.
    """
    points = []
    total_generated = 0
    ocean_filtered = 0

    center_lat = (LAT_MIN + LAT_MAX) / 2.0
    row_spacing_deg = (HEX_SPACING_KM * math.sqrt(3) / 2) / KM_PER_DEG_LAT
    col_spacing_deg = HEX_SPACING_KM / km_per_deg_lng(center_lat)

    lat = LAT_MIN
    row_idx = 0
    while lat <= LAT_MAX:
        lng_offset = (col_spacing_deg / 2) if (row_idx % 2 == 1) else 0
        lng = LNG_MIN + lng_offset

        while lng <= LNG_MAX:
            total_generated += 1
            if us_land.contains(Point(lng, lat)):
                points.append(
                    {
                        "lat": round(lat, 6),
                        "lng": round(lng, 6),
                        "tier": 3,
                        "city_name": "",
                        "state": "",
                        "population": 0,
                        "point_type": "grid",
                    }
                )
            else:
                ocean_filtered += 1
            lng += col_spacing_deg

        lat += row_spacing_deg
        row_idx += 1

    print(f"  Tier 3: {total_generated} total, {ocean_filtered} ocean/water filtered")
    return points


def generate_all_points(cities: pd.DataFrame, place_boundaries: dict, cousub_boundaries: dict, us_land) -> pd.DataFrame:
    """Generate sample points for all tiers."""
    print("\nStep 3: Generating sample points")

    # Merge both boundary dicts for lookup
    all_boundaries = {**place_boundaries, **cousub_boundaries}
    all_points = []

    # Tier 1: dense grid within actual city boundaries
    print(f"  Tier 1: {len(cities[cities['tier'] == 1])} cities (grid within boundary polygons)")
    tier1_points = generate_city_points(cities, all_boundaries, tier=1)
    all_points.extend(tier1_points)
    print(f"  Tier 1: {len(tier1_points):,} points generated")

    # Tier 2: sparser grid within actual city boundaries
    print(f"  Tier 2: {len(cities[cities['tier'] == 2])} cities (grid within boundary polygons)")
    tier2_points = generate_city_points(cities, all_boundaries, tier=2)
    all_points.extend(tier2_points)
    print(f"  Tier 2: {len(tier2_points):,} points generated")

    # Tier 3: hex grid with land filtering
    print(f"  Tier 3: Generating hex grid ({HEX_SPACING_KM}km spacing, land-only)...")
    grid_points = generate_hex_grid(us_land)
    all_points.extend(grid_points)
    print(f"  Tier 3: {len(grid_points):,} land points kept")

    # Build DataFrame and assign point IDs
    df = pd.DataFrame(all_points)
    df.insert(0, "point_id", range(1, len(df) + 1))
    df["lat"] = df["lat"].round(6)
    df["lng"] = df["lng"].round(6)

    return df


# ---------------------------------------------------------------------------
# Step 4: Save and report
# ---------------------------------------------------------------------------


def save_and_report(df: pd.DataFrame) -> None:
    """Save the sample points CSV and print summary statistics."""
    print(f"\nStep 4: Saving to {OUTPUT_FILE}")

    df = df[["point_id", "lat", "lng", "tier", "city_name", "state", "population", "point_type"]]
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"  Wrote {len(df):,} points to {OUTPUT_FILE}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for tier in sorted(df["tier"].unique()):
        subset = df[df["tier"] == tier]
        label = {
            1: "Large cities 50K+ (boundary polygons)",
            2: "Medium cities 10K-50K (boundary polygons)",
            3: "Rural hex grid (land only)",
        }
        print(f"\n  Tier {tier} - {label.get(tier, 'Unknown')}:")
        print(f"    Points: {len(subset):,}")
        if tier in (1, 2):
            n_cities = subset["city_name"].nunique()
            print(f"    Cities: {n_cities:,}")
            print(f"    Avg points per city: {len(subset) // max(n_cities, 1)}")

    city_points = df[df["tier"].isin([1, 2]) & (df["state"] != "")]
    states = sorted(city_points["state"].unique())
    print(f"\n  States covered: {len(states)}")
    print(f"    {', '.join(states)}")

    print(f"\n  Total points: {len(df):,}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    download_all_data()
    us_land = load_us_land_polygon()
    place_boundaries = load_place_boundaries()
    cousub_boundaries = load_cousub_boundaries()
    cities = load_and_join(cousub_boundaries)
    points = generate_all_points(cities, place_boundaries, cousub_boundaries, us_land)
    save_and_report(points)


if __name__ == "__main__":
    main()
