
# Glacier Analysis — Implementation Plan

**Purpose:** lightweight Python app (Flask + JS) and CLI to extract glacier outlines (from OSM or RGI), fetch elevation (OpenTopography), compute slope and depth maps, visualize flow gradients, and export reproducible outputs via a `config.yaml`.

---

## High-level architecture

- Frontend: HTML + JavaScript (Leaflet) single-page UI with 4 panels (map / outline / elevation / slope & flow) and controls.
- Backend: Python Flask app exposing JSON API endpoints and serving the frontend.
- CLI: Python command-line tool that accepts command-line args or a `config.yaml` to reproduce runs.
- Data processing: raster/vector processing with `rasterio`, `numpy`, `scipy`, `shapely`, `pyproj`, and optional `gdal`/`osgeo`.
- Optional: use `geopandas` for vector handling and `matplotlib` for image exports.

---

## Components & Responsibilities

### Frontend (Leaflet)
- Panel A: Interactive map base layer (OpenTopo or OSM tiles). User draws/selects a bounding box (Leaflet Draw).
- Panel B: Glacier outline display — fetched from backend (OSM Overpass query or RGI lookup) and rendered as GeoJSON.
- Panel C: Elevation overlay — fetched as tiled raster image (PNG/GeoTIFF rendered to canvas) or vectorized hillshade served by backend.
- Panel D: Slope / Flow visualization — color-shaded slope raster and flow vectors/streamlines overlay. UI inputs:
  - `grid_size` (meters) for spatial averaging,
  - `tau_f` parameter (the "tau-f" factor),
  - units toggle (degrees / radians).
- Export controls: save `config.yaml`, request processing, download report bundle (ZIP).

### Backend (Flask)
Key endpoints (examples):

- `POST /api/area`  
  Body: `{ "bbox": [minx, miny, maxx, maxy], "source": "osm"|"rgi" }`  
  Response: `{ "id": "<job_id>", "outline_geojson": <GeoJSON> }`

- `POST /api/elevation`  
  Body: `{ "bbox": ..., "resolution": <meters>, "dataset": "opentopo", "api_key": "..." }`  
  Action: fetch DEM tiles from OpenTopography or internal cache, mosaic, reproject to EPSG:3857 or local UTM.  
  Response: link to GeoTIFF and preview image.

- `POST /api/slope`  
  Body: `{ "dem_path": "...", "method": "central_diff", "units": "radians" }`  
  Action: compute slope angle raster using gradient operators.  
  Response: slope GeoTIFF and preview PNG.

- `POST /api/average_slope`  
  Body: `{ "slope_path": "...", "grid_size_m": 100 }`  
  Action: compute block-averaged slope per `grid_size_m`. Returns grid raster and statistics.

- `POST /api/flow`  
  Body: `{ "slope_path": "...", "method": "gradient" }`  
  Action: compute gradient vectors and streamlines (using `scipy.ndimage` or `skimage`), return vector GeoJSON for flowlines.

- `POST /api/depth`  
  Body: `{ "avg_slope_path": "...", "tau_f": 100 }`  
  Action: compute depth raster from relationship: `depth * slope_angle = tau_f` -> `depth = tau_f / slope_angle` (units: **tau_f in same units as depth·angle; slope_angle in radians**). Replace small/zero slopes with NaN or apply a threshold to avoid singularities.  
  Response: depth GeoTIFF and preview PNG.

- `GET /api/report/<job_id>`  
  Returns ZIP bundle with intermediate GeoTIFFs/PNGs and final overlays.

### CLI
- Command: `python glacier_cli.py --config config.yaml` or inline args:
  - `--bbox`, `--grid-size`, `--tau-f`, `--dataset`, `--opentopo-key`, `--outdir`
- Recreates the same pipeline as the Flask app but runs headless and writes outputs to disk.

---

## Data sources & APIs

- **OpenTopography** — DEM access for high-resolution elevation data; has APIs for DEM tiles and 3DEP access in the US (requires API key for some datasets). Use their REST endpoints to request DEM tiles for a bounding box and resolution. citeturn0search2

- **OpenStreetMap / Overpass API** — extract glacier outlines tagged `natural=glacier` (and related tags like `glacier:type`, `glacier:part`). Use Overpass to query features within the selected bbox and return GeoJSON. OSM glacier tagging and guidance are documented. citeturn1search1turn1search7

- **Randolph Glacier Inventory (RGI)** — authoritative global glacier outlines packaged as shapefiles and GeoTIFF auxiliaries; good fallback or supplement to OSM outlines. Download and serve regional shapes for the bounding box as needed. citeturn0search0turn0search8

---

## Algorithms and math

### Elevation → Slope (per-pixel)
1. Read DEM as `z(x,y)` in projected coordinates (meters). Reproject to a UTM or other equal-distance CRS to keep meters consistent.
2. Compute partial derivatives using central differences:
   - `dz_dx = (z[x+1,y] - z[x-1,y]) / (2*dx)`
   - `dz_dy = (z[x,y+1] - z[x,y-1]) / (2*dy)`
3. Slope magnitude (gradient) = `g = sqrt(dz_dx**2 + dz_dy**2)`
4. Slope angle (radians) = `theta = arctan(g)`  
   (Alternatively compute degrees via `degrees(theta)` depending on UI choice.)

### Spatial averaging (block/grid)
- Aggregate slope angle into blocks of `grid_size` meters using raster resampling (e.g., mean pooling with `rasterio` or `scipy.ndimage.uniform_filter` over pixel window corresponding to `grid_size`).
- The averaged slope angle raster will be used for depth computation and for smoothing noisy local variations.

### Flow vectors and streamlines
- Compute continuous gradient vector field `(-dz_dx, -dz_dy)` to approximate downslope direction.
- Convert the vector field to streamlines (seed points on glacier outline or on grid centres) by numerically integrating the vector field (e.g., `scipy.integrate.odeint` or custom RK4 over the raster grid), or use `skimage`/`pyflwdir` tools for flowlines.
- Render streamlines as GeoJSON linestrings for overlay.

### Depth calculation
- Relationship given: `depth * slope_angle = tau_f`  →  `depth = tau_f / slope_angle`.  
  - **Important:** slope_angle must be in **radians** for consistent units unless `tau_f` is defined differently.  
  - Handle near-zero slope: clip minimum slope to avoid infinite depth (`theta_min = max(theta, epsilon)`).

---

## File outputs (report bundle)
Inside `report_<job_id>.zip`:

- `config.yaml` (the executed input)
- `outline.geojson` (glacier outline)
- `dem.tif` and `dem_preview.png`
- `slope_angle.tif` and `slope_angle_preview.png`
- `avg_slope.tif` and `avg_slope_preview.png`
- `flowlines.geojson` and `flowlines_preview.png`
- `depth.tif` and `depth_preview.png`
- `final_overlay.png` (flow lines + depth over base topo)
- `report.json` metadata & statistics (mean slope, mean depth, grid parameters)

---

## Example Overpass query (OSM) for glacier outlines
```
[out:json][timeout:25];
(
  way["natural"="glacier"]({{bbox}});
  relation["natural"="glacier"]({{bbox}});
);
out body;
>;
out skel qt;
```
Run via Overpass API or Overpass Turbo and return GeoJSON to backend. Guidance: keep bbox reasonably small to avoid timeouts. citeturn1search7turn1search1

---

## Example OpenTopography DEM fetch (pseudo)
- Call OpenTopography REST DEM service for bounding box and resolution; handle API key as required.
- Cache responses to avoid repeated network calls for same bbox/resolution. citeturn0search2

---

## Libraries & Environment

- Python 3.10+
- Flask
- rasterio
- numpy
- scipy
- geopandas
- shapely
- pyproj
- requests (for HTTP APIs)
- folium or direct Leaflet in frontend
- matplotlib (for PNG exports)
- optional: GDAL / osgeo for robust raster ops
- Testing: pytest

---

## Config.yaml schema (example)
```yaml
job_id: run_2026_03_02_001
bbox: [-122.53, 37.6, -122.35, 37.83]   # minx, miny, maxx, maxy (lon/lat WGS84)
crs: EPSG:4326
dataset: opentopography
opentopo_api_key: "<KEY_IF_REQUIRED>"
grid_size_m: 100
resolution_m: 30
tau_f: 100.0
tau_f_units: "m·rad"    # user-declared; depth (m) * slope_angle (rad) = tau_f
slope_units: "radians"  # or "degrees"
output_dir: "./outputs/run_2026_03_02_001"
```

---

## Edge cases & notes
- Web interface default port is `8080` (run `python glacier_app/app.py` and browse to `http://localhost:8080` unless overridden with `--port`).
- OSM outlines may be outdated or incomplete; RGI is a useful supplement for global coverage. Consider priority: OSM (local edits) → RGI (global standardized) if OSM result is empty. citeturn0search0turn1search1
- Ensure DEM and vector outlines are in same CRS (reproject outlines to DEM CRS) before clipping/masking.
- Handle nodata and apply smoothing / hole-filling for small artifacts.
- For shallow slopes, enforce a maximum depth clamp or mask based on physical plausibility.
- Document units clearly in UI and YAML; default slope angles use radians internally.

---

## Next steps for development (milestones)
1. Prototype backend DEM fetch + DEM -> slope conversion (CLI small bbox).
2. Add Overpass OSM outline extraction + overlay with DEM (CLI).
3. Implement averaging & depth calculation; produce single-run report bundle.
4. Build Flask UI with Leaflet bounding box selector and parameter controls.
5. Add streamlines/flowline rendering and combine overlays.
6. Add tests, logging, error handling, and Dockerfile.

---

## References
- OpenTopography API & DEM services (DEMs including USGS 3DEP). citeturn0search2  
- Randolph Glacier Inventory (global glacier outlines / shapefiles). citeturn0search0turn0search8  
- OpenStreetMap glacier tagging and Overpass usage for extracting `natural=glacier`. citeturn1search1turn1search7
