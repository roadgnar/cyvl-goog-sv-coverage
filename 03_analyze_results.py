#!/usr/bin/env python3
"""Analyze Street View metadata results and generate summary statistics.

Reads data/sv_results.csv and produces:
  - data/city_summary.csv   (one row per city)
  - data/state_summary.csv  (one row per state)
  - data/national_summary.csv (single row, national aggregates)

Also prints headline statistics and a sales-deck narrative to stdout.
"""

import os
from datetime import date

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
INPUT_CSV = os.path.join(DATA_DIR, "sv_results_v4.csv")

TODAY = date.today()

NO_COVERAGE_STATUSES = {"ZERO_RESULTS", "NOT_FOUND"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_sv_date_to_years(sv_date_series: pd.Series) -> pd.Series:
    """Convert 'YYYY-MM' strings to age in fractional years from TODAY.

    Returns NaN for missing or unparseable dates.
    """
    parsed = pd.to_datetime(sv_date_series, format="%Y-%m", errors="coerce")
    delta_days = (pd.Timestamp(TODAY) - parsed).dt.days
    return delta_days / 365.25


def pct(numerator, denominator):
    """Safe percentage, rounded to 1 decimal."""
    if denominator == 0:
        return 0.0
    return round(100.0 * numerator / denominator, 1)


def age_threshold_pcts(ages: pd.Series, thresholds: list[int]) -> dict[str, float]:
    """Return {pct_over_Nyr: value} for each threshold in years."""
    valid = ages.dropna()
    total = len(valid)
    result = {}
    for yr in thresholds:
        count = (valid > yr).sum()
        result[f"pct_over_{yr}yr"] = pct(count, total)
    return result


def freshness_stats(ages: pd.Series) -> dict:
    """Compute avg, median, and threshold percentages for a series of ages."""
    valid = ages.dropna()
    stats = {
        "avg_age_years": round(valid.mean(), 1) if len(valid) else None,
        "median_age_years": round(valid.median(), 1) if len(valid) else None,
    }
    stats.update(age_threshold_pcts(ages, [1, 2, 3, 5]))
    return stats


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

print(f"Reading {INPUT_CSV} ...")
df = pd.read_csv(INPUT_CSV, dtype={"sv_date": str})

# Filter out bad dates (e.g., year 2611 from garbage pano data)
bad_date_mask = df["sv_date"].str.match(r"^2[6-9]\d{2}", na=False) | df["sv_date"].str.match(r"^[3-9]\d{3}", na=False)
if bad_date_mask.any():
    print(f"Filtering {bad_date_mask.sum()} rows with bad dates")
    df.loc[bad_date_mask, "sv_date"] = ""

# Derived columns
df["is_ok"] = df["status"] == "OK"
df["is_no_coverage"] = df["status"].isin(NO_COVERAGE_STATUSES)
df["age_years"] = parse_sv_date_to_years(df["sv_date"])

# For OK rows with missing date, age_years is NaN -- counted separately
ok_no_date_count = df["is_ok"].sum() - df.loc[df["is_ok"], "age_years"].notna().sum()

print(f"Loaded {len(df):,} points. OK with unknown date: {ok_no_date_count:,}")

# ---------------------------------------------------------------------------
# 1. City summary
# ---------------------------------------------------------------------------

city_groups = df.groupby(["city_name", "state"], sort=True)


def city_agg(g: pd.DataFrame) -> pd.Series:
    ages = g.loc[g["is_ok"], "age_years"]
    valid_dates = g.loc[g["is_ok"] & g["age_years"].notna(), "sv_date"]
    stats = freshness_stats(ages)

    newest = valid_dates.max() if len(valid_dates) else None
    oldest = valid_dates.min() if len(valid_dates) else None

    return pd.Series(
        {
            "population": g["population"].iloc[0],
            "tier": g["tier"].iloc[0],
            "total_points": len(g),
            "ok_count": g["is_ok"].sum(),
            "no_coverage_count": g["is_no_coverage"].sum(),
            "pct_no_coverage": pct(g["is_no_coverage"].sum(), len(g)),
            "avg_age_years": stats["avg_age_years"],
            "median_age_years": stats["median_age_years"],
            "pct_over_2yr": stats["pct_over_2yr"],
            "pct_over_3yr": stats["pct_over_3yr"],
            "pct_over_5yr": stats["pct_over_5yr"],
            "newest_date": newest,
            "oldest_date": oldest,
        }
    )


city_summary = city_groups.apply(city_agg, include_groups=False).reset_index()

city_out = os.path.join(DATA_DIR, "city_summary.csv")
city_summary.to_csv(city_out, index=False)
print(f"Wrote {city_out} ({len(city_summary):,} cities)")

# ---------------------------------------------------------------------------
# 2. State summary
# ---------------------------------------------------------------------------

state_groups = df.groupby("state", sort=True)


def state_agg(g: pd.DataFrame) -> pd.Series:
    ages = g.loc[g["is_ok"], "age_years"]
    stats = freshness_stats(ages)
    n_cities = g.groupby("city_name").ngroups

    return pd.Series(
        {
            "total_cities": n_cities,
            "total_points": len(g),
            "ok_count": g["is_ok"].sum(),
            "no_coverage_count": g["is_no_coverage"].sum(),
            "pct_no_coverage": pct(g["is_no_coverage"].sum(), len(g)),
            "avg_age_years": stats["avg_age_years"],
            "median_age_years": stats["median_age_years"],
            "pct_over_2yr": stats["pct_over_2yr"],
            "pct_over_3yr": stats["pct_over_3yr"],
            "pct_over_5yr": stats["pct_over_5yr"],
            "total_population_sampled": g.groupby("city_name")["population"]
            .first()
            .sum(),
        }
    )


state_summary = state_groups.apply(state_agg, include_groups=False).reset_index()

state_out = os.path.join(DATA_DIR, "state_summary.csv")
state_summary.to_csv(state_out, index=False)
print(f"Wrote {state_out} ({len(state_summary):,} states)")

# ---------------------------------------------------------------------------
# 3. National summary
# ---------------------------------------------------------------------------

all_ages = df.loc[df["is_ok"], "age_years"]
nat_stats = freshness_stats(all_ages)

national = pd.DataFrame(
    [
        {
            "total_points": len(df),
            "total_cities": df["city_name"].nunique(),
            "total_states": df["state"].nunique(),
            "ok_count": df["is_ok"].sum(),
            "no_coverage_count": df["is_no_coverage"].sum(),
            "pct_no_coverage": pct(df["is_no_coverage"].sum(), len(df)),
            "avg_age_years": nat_stats["avg_age_years"],
            "median_age_years": nat_stats["median_age_years"],
            "pct_over_1yr": nat_stats["pct_over_1yr"],
            "pct_over_2yr": nat_stats["pct_over_2yr"],
            "pct_over_3yr": nat_stats["pct_over_3yr"],
            "pct_over_5yr": nat_stats["pct_over_5yr"],
        }
    ]
)

national_out = os.path.join(DATA_DIR, "national_summary.csv")
national.to_csv(national_out, index=False)
print(f"Wrote {national_out}")

# ---------------------------------------------------------------------------
# 4. Print headline statistics
# ---------------------------------------------------------------------------

n = national.iloc[0]

print("\n" + "=" * 72)
print("  NATIONAL STREET VIEW FRESHNESS ANALYSIS")
print("=" * 72)
print(f"  Reference date:       {TODAY.isoformat()}")
print(f"  Total points sampled: {int(n['total_points']):,}")
print(f"  Cities:               {int(n['total_cities']):,}")
print(f"  States:               {int(n['total_states'])}")
print()
print(f"  Coverage:")
print(f"    OK:           {int(n['ok_count']):>8,}  ({pct(n['ok_count'], n['total_points'])}%)")
print(f"    No coverage:  {int(n['no_coverage_count']):>8,}  ({n['pct_no_coverage']}%)")
print()
print(f"  Imagery age (where date available):")
print(f"    Average:  {n['avg_age_years']} years")
print(f"    Median:   {n['median_age_years']} years")
print(f"    > 1 year: {n['pct_over_1yr']}%")
print(f"    > 2 year: {n['pct_over_2yr']}%")
print(f"    > 3 year: {n['pct_over_3yr']}%")
print(f"    > 5 year: {n['pct_over_5yr']}%")
if ok_no_date_count > 0:
    print(f"    Unknown date (OK status): {ok_no_date_count:,} points")
print("=" * 72)

# ---------------------------------------------------------------------------
# 5. Top 10 states by % no coverage
# ---------------------------------------------------------------------------

print("\n  TOP 10 STATES BY % NO COVERAGE")
print("  " + "-" * 50)
top_no_cov = state_summary.nlargest(10, "pct_no_coverage")
for _, row in top_no_cov.iterrows():
    print(
        f"  {row['state']:<6} {row['pct_no_coverage']:>5.1f}%  "
        f"({int(row['no_coverage_count']):,} / {int(row['total_points']):,} points)"
    )

# ---------------------------------------------------------------------------
# 6. Top 10 states by average imagery age
# ---------------------------------------------------------------------------

print(f"\n  TOP 10 STATES BY AVERAGE IMAGERY AGE")
print("  " + "-" * 50)
top_age = state_summary.dropna(subset=["avg_age_years"]).nlargest(10, "avg_age_years")
for _, row in top_age.iterrows():
    print(
        f"  {row['state']:<6} {row['avg_age_years']:>5.1f} yr avg  "
        f"(median {row['median_age_years']:.1f} yr, "
        f"{row['pct_over_3yr']:.1f}% > 3yr)"
    )

# ---------------------------------------------------------------------------
# 7. Sales deck narrative paragraph
# ---------------------------------------------------------------------------

print("\n" + "=" * 72)
print("  SALES DECK NARRATIVE")
print("=" * 72)

narrative = (
    f'We analyzed {int(n["total_cities"]):,} cities across all '
    f'{int(n["total_states"])} US states using Google\'s own Street View API. '
    f'{n["pct_over_3yr"]}% of sampled locations have imagery over 3 years old. '
    f'{n["pct_no_coverage"]}% have no Street View coverage at all. '
    f"Google's '280 billion images' includes historical captures and global "
    f"data -- for US road infrastructure, their data is "
    f'{n["avg_age_years"]} years stale on average.'
)

print()
print(narrative)
print()
print("=" * 72)
