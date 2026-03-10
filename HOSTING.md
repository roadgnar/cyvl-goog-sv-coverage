# Hosting Plan: Google Street View Freshness Viewer

## What We're Deploying

A static site with:
- `viewer/index.html` — single-file Mapbox GL viewer (~1,400 lines)
- `data/freshness.geojson` — 83MB point data
- `data/freshness_hex.geojson` — 35MB H3 hex data

Total: ~120MB of static assets. No backend, no API, no auth.

---

## Recommended: Cloudflare Pages

We already use Cloudflare for Workers. Pages is the simplest option.

### Setup

1. Create a new Cloudflare Pages project (separate from the platform)
2. Connect to a GitHub repo or do direct upload
3. Build settings: none needed (it's all static files)
4. Root directory: `/` (serves `viewer/index.html` at `/viewer/`)

### Steps

```bash
# Option A: Direct upload (fastest, no repo needed)
npx wrangler pages deploy . --project-name=sv-freshness

# Option B: GitHub repo + auto-deploy
# 1. Push google-sv-sucks to a new GitHub repo
# 2. Cloudflare Dashboard > Pages > Create > Connect to Git
# 3. Set build output directory to "/"
# 4. Deploy
```

### GeoJSON Size Problem

The GeoJSON files are 83MB + 35MB. This works but is slow to load. Two fixes:

**Fix 1: Enable Cloudflare compression (automatic)**
Cloudflare Pages auto-compresses with Brotli/gzip. GeoJSON compresses ~5-8x, so users download ~15-20MB total. Good enough.

**Fix 2: Pre-compress to gzip (better)**
```bash
gzip -k data/freshness.geojson        # creates freshness.geojson.gz
gzip -k data/freshness_hex.geojson    # creates freshness_hex.geojson.gz
```
Then update `index.html` to fetch the `.gz` files, or configure Cloudflare to serve pre-compressed assets (it does this automatically for Pages).

### Custom Domain

```bash
# Add a custom domain in Cloudflare Pages settings
# e.g., sv-freshness.cyvl.ai or streetview.cyvl.ai
```

### Cost

Free tier covers this easily (unlimited bandwidth for static sites on Pages).

---

## Alternative: S3 + CloudFront

If we want it on AWS instead:

```bash
# Create bucket
aws s3 mb s3://sv-freshness-viewer

# Upload with correct content types
aws s3 sync . s3://sv-freshness-viewer \
  --exclude ".env" --exclude ".venv/*" --exclude "__pycache__/*" \
  --exclude "*.py" --exclude "requirements.txt" --exclude "*.csv" \
  --exclude "*.zip" --exclude "*.txt"

# Enable static website hosting
aws s3 website s3://sv-freshness-viewer \
  --index-document viewer/index.html

# Create CloudFront distribution pointing to the bucket
# Enable compression (gzip/brotli)
```

Cost: pennies/month for storage, CloudFront free tier covers 1TB/month.

---

## Before Going Public

### Security Checklist

- [ ] **Mapbox token**: The current token is embedded in the HTML. Create a restricted token scoped to the production domain only (Mapbox Dashboard > Tokens > URL restrictions)
- [ ] **Google API key**: NOT exposed in the viewer (only used during data collection). Confirm it's not in any committed files
- [ ] **No `.env` file deployed**: gitignore already handles this, but double-check

### Performance Checklist

- [ ] Test load time on throttled connection (the 83MB GeoJSON is the bottleneck)
- [ ] Consider converting GeoJSON to [PMTiles](https://protomaps.com/docs/pmtiles) or Mapbox vector tiles for production (drops load from 83MB to ~5MB)
- [ ] Add `Cache-Control: public, max-age=86400` headers for the GeoJSON files

### Polish Checklist

- [ ] Add an `<meta>` description and OpenGraph tags for link previews
- [ ] Add a favicon
- [ ] Test on mobile (the panel may need responsive adjustments)
- [ ] Add a "Methodology" link/section explaining the data source (US Census + Google SV API)

---

## Future: Vector Tiles (if load time matters)

If we want instant loading instead of waiting for 120MB of GeoJSON:

1. Install [tippecanoe](https://github.com/felt/tippecanoe)
2. Convert to MBTiles/PMTiles:
   ```bash
   tippecanoe -o freshness.pmtiles \
     -z12 -Z3 \
     --drop-densest-as-needed \
     data/freshness.geojson data/freshness_hex.geojson
   ```
3. Host the `.pmtiles` file on Cloudflare R2 or S3
4. Use `pmtiles` protocol in Mapbox GL to load tiles on-demand
5. Result: ~5MB initial load, tiles stream as you pan/zoom

This is the right move if we're sending this to prospects or embedding it in the website. The GeoJSON approach is fine for internal demos.

---

## Quick Deploy (5 minutes)

Fastest path to a public URL:

```bash
cd /Users/dp/Code/roadgnar/google-sv-sucks
npx wrangler pages deploy . --project-name=sv-freshness
```

This gives you a `sv-freshness.pages.dev` URL immediately. Add a custom domain later.
