# Glacier App

Lightweight glacier analysis tooling with both CLI and Flask UI workflows. The project scaffolding below matches the `implementation_plan.md` pipeline (outline -> DEM -> slope -> flow -> depth -> report ZIP).

## Prerequisites

- Python 3.10+
- `pip` and virtual environment support

## Setup

```bash
cd /workspace/glacier_app
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Or install as a package with entry points:

```bash
pip install -e ".[dev]"
```

## Configuration

Use the provided example and edit values:

```bash
cp config.example.yaml config.yaml
```

Key fields are aligned with the implementation plan schema:
- `job_id`, `bbox`, `crs`, `source`, `dataset`
- `opentopo_api_key`, `grid_size_m`, `resolution_m`
- `tau_f`, `tau_f_units`, `slope_units`, `output_dir`

## Run CLI

Console script name: `glacier-cli`

```bash
glacier-cli --config config.yaml
```

Expected inline usage once CLI implementation is present:

```bash
glacier-cli \
  --bbox=-122.53,37.6,-122.35,37.83 \
  --grid-size 100 \
  --tau-f 100 \
  --dataset opentopography \
  --opentopo-key "$OPENTOPO_API_KEY" \
  --outdir ./outputs/run_manual
```

## Run Flask UI

Recommended dev launch:

```bash
export FLASK_APP=app:create_app
export FLASK_ENV=development
flask run --host 0.0.0.0 --port 5000 --debug
```

Alternative production-style local run:

```bash
gunicorn -w 2 -b 0.0.0.0:5000 "app:create_app()"
```

## API examples

`POST /api/area`

```bash
curl -X POST http://localhost:5000/api/area \
  -H "Content-Type: application/json" \
  -d '{"bbox":[-122.53,37.6,-122.35,37.83],"source":"osm"}'
```

`POST /api/elevation`

```bash
curl -X POST http://localhost:5000/api/elevation \
  -H "Content-Type: application/json" \
  -d '{"bbox":[-122.53,37.6,-122.35,37.83],"resolution":30,"dataset":"opentopo","api_key":"YOUR_KEY"}'
```

`POST /api/slope`

```bash
curl -X POST http://localhost:5000/api/slope \
  -H "Content-Type: application/json" \
  -d '{"dem_path":"outputs/run_001/dem.tif","method":"central_diff","units":"radians"}'
```

`POST /api/depth`

```bash
curl -X POST http://localhost:5000/api/depth \
  -H "Content-Type: application/json" \
  -d '{"avg_slope_path":"outputs/run_001/avg_slope.tif","tau_f":100.0}'
```

`GET /api/report/<job_id>`

```bash
curl -L "http://localhost:5000/api/report/run_2026_03_02_001" -o report.zip
```

## Testing

Run offline tests:

```bash
pytest
```

Current tests cover:
- Config parsing and schema-shaped validation.
- Synthetic DEM generation shape guarantees.
- Slope/depth numerical stability around low gradients.
- Report ZIP creation in temporary directories.

Tests cover real pipeline modules and run fully offline using synthetic fallbacks.
