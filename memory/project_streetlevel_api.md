---
name: streetlevel library for SV data
description: Using sk-zk/streetlevel to bypass Google SV API — no key needed, no rate limits on metadata
type: project
---

Switched from official Google Street View API to `streetlevel` library (pip install streetlevel, repo: github.com/sk-zk/streetlevel).

**Why:** Official API was blocking our IP after ~1M calls. streetlevel reverse-engineers Google's internal endpoints with no API key and no known rate limits on metadata requests.

**Endpoints used (zero billing):**
- `google.com/maps/photometa/ac/v1` — coverage tiles (internal, no key)
- `maps.googleapis.com/.../GeoPhotoService.SingleImageSearch` — pano search (internal, no key)
- `google.com/maps/photometa/v1` — pano metadata by ID (internal, no key)

**Collection script:** `02_collect_sv_streetlevel.py`
- Adaptive concurrency: starts at 80, ramps to 200, backs off on errors
- Sustained ~650 pts/sec, spikes to 1,400+
- Full resume support via CSV point_id matching
- Output: `data/sv_results_v4.csv`

**Sampling updates (01_prepare_sample_points.py):**
- Denser city grids: 150m for 1M+ cities, 200m for 500K+, down to 500m for 10K+
- Rural hex grid: 3km (was 5km)
- Total: 3,114,074 points (was ~1M)
- 798 Tier 1 cities (50K+), 2373 Tier 2 cities (10K-50K)

**Progress as of 2026-03-14:**
- Collection running in background: ~1.75M+ of 3.1M done
- Old 1.08M snapped points fully collected (saved as sv_results_v4_old_snapped.csv)
- Resume command: `python3.9 02_collect_sv_streetlevel.py`

**GeoJSON generation:**
- H3 resolution bumped from 7 to 8 (~460m edge vs 1.2km)
- h3 pip package has build issues on Python 3.9 — need to fix (try h3==3.7.7 or upgrade Python)

**Viewer:** `viewer/index.html` — Mapbox GL hex-grid viewer, dev server at localhost:8080/viewer/
