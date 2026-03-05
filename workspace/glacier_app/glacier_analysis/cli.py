"""Package CLI entrypoint for glacier analysis pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .cli_runner import run_from_inputs_safe


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run glacier analysis pipeline")
    p.add_argument("--config", help="Path to YAML config file")
    p.add_argument("--job-id", help="Run identifier")
    p.add_argument("--bbox", help="Bounding box in WGS84: minx,miny,maxx,maxy")
    p.add_argument("--grid-size", type=float, help="Slope averaging grid size in meters")
    p.add_argument("--tau-f", type=float, help="Depth parameter tau_f")
    p.add_argument("--dataset", help="DEM dataset name (e.g. opentopography, cop30)")
    p.add_argument("--opentopo-key", help="OpenTopography API key")
    p.add_argument("--resolution", type=float, help="DEM target resolution in meters")
    p.add_argument("--outdir", help="Output run directory")
    p.add_argument("--slope-units", choices=["radians", "degrees"], help="Slope output units")
    p.add_argument("--source", choices=["osm", "rgi"], help="Outline source selector")
    p.add_argument("--overpass-url", help="Overpass API URL override")
    p.add_argument("--opentopo-url", help="OpenTopography API URL override")
    p.add_argument("--timeout", type=int, help="HTTP timeout seconds")
    return p


def _cli_values(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "job_id": args.job_id,
        "bbox": args.bbox,
        "grid_size_m": args.grid_size,
        "tau_f": args.tau_f,
        "dataset": args.dataset,
        "opentopo_api_key": args.opentopo_key,
        "resolution_m": args.resolution,
        "output_dir": args.outdir,
        "slope_units": args.slope_units,
        "source": args.source,
        "overpass_url": args.overpass_url,
        "opentopo_url": args.opentopo_url,
        "request_timeout_s": args.timeout,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_from_inputs_safe(args.config, _cli_values(args))

    if not result.get("ok"):
        print("Pipeline failed:", file=sys.stderr)
        print(result.get("error", "unknown error"), file=sys.stderr)
        return 1

    payload = result["result"]
    print(json.dumps(payload, indent=2))
    return 0
