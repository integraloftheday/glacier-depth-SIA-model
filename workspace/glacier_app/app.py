from __future__ import annotations

import argparse
import io
import json
import math
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import matplotlib.pyplot as plt
import numpy as np
import rasterio
import requests
from PIL import Image
from pyproj import Transformer
from flask import Flask, jsonify, render_template, request, send_file, send_from_directory
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter
from rasterio.features import geometry_mask
from rasterio.transform import Affine
from rasterio.transform import array_bounds
from rasterio.transform import rowcol as raster_rowcol
from rasterio.transform import xy as raster_xy
from rasterio.warp import Resampling, reproject, transform_bounds
from shapely.geometry import mapping, shape
from shapely.ops import transform as shapely_transform

from glacier_analysis.api_models import (
    AreaRequest,
    AverageSlopeRequest,
    DepthRequest,
    ElevationRequest,
    FlowRequest,
    RunFullRequest,
    SlopeRequest,
    ValidationError,
)
from glacier_analysis.cli_runner import run_pipeline
from glacier_analysis.config import PipelineConfig, save_config_yaml
from glacier_analysis.elevation import get_dem, local_utm_crs
from glacier_analysis.geometry import (
    get_outline_geojson,
    parse_glacier_name_from_text,
    parse_relation_id_from_text,
    select_glacier_at_point,
    select_glacier_by_name,
    select_glacier_by_relation_id,
)
from glacier_analysis.processing import average_slope, depth_from_slope, flowlines_geojson, slope_degrees, slope_radians
from glacier_analysis.report import (
    build_zip_bundle,
    write_flow_preview_png,
    write_geotiff,
    write_geojson,
    write_json,
    write_overlay_png,
    write_raster_preview_png,
)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = BASE_DIR / "outputs"
RUN_TASKS: Dict[str, Dict[str, Any]] = {}
RUN_TASKS_LOCK = threading.Lock()
WEB_MERCATOR_HALF_WORLD_M = 20037508.342789244


def _load_env_file(env_path: Path) -> None:
    """Load KEY=VALUE pairs from a local .env into process env (non-overriding)."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _resolve_opentopo_key(provided_key: str | None) -> str:
    candidate = (provided_key or "").strip()
    if candidate:
        return candidate
    return os.getenv("OPEN_TOPO_KEY", "").strip()


def _resolve_depth_params(payload: Dict[str, Any], default_bulk_constant_m: float = 11.0) -> tuple[float, float, float]:
    bulk_constant_m = float(payload.get("bulk_constant_m", payload.get("tau_0_kpa", default_bulk_constant_m)))
    f_prime = float(payload.get("f_prime", 1.0))
    depth_scale_m = bulk_constant_m / f_prime
    if bulk_constant_m <= 0:
        raise ApiError("'bulk_constant_m' must be > 0", 400)
    if f_prime <= 0:
        raise ApiError("'f_prime' must be > 0", 400)
    if depth_scale_m <= 0:
        raise ApiError("'bulk_constant_m/f_prime' must be > 0", 400)
    return bulk_constant_m, f_prime, depth_scale_m


# Load local project .env first, then workspace-level .env as fallback.
_load_env_file(BASE_DIR / ".env")
_load_env_file(BASE_DIR.parent / ".env")


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 400, details: Dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details or {}


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    @app.get("/outputs/<path:rel_path>")
    def serve_output(rel_path: str):
        return send_from_directory(OUTPUT_ROOT, rel_path)

    @app.errorhandler(ApiError)
    def handle_api_error(err: ApiError):
        return jsonify({"error": err.message, "details": err.details}), err.status_code

    @app.errorhandler(ValidationError)
    def handle_validation_error(err: ValidationError):
        return jsonify({"error": str(err)}), 400

    @app.errorhandler(404)
    def handle_not_found(_err):
        return jsonify({"error": "not found"}), 404

    @app.errorhandler(Exception)
    def handle_uncaught(err: Exception):
        return (
            jsonify(
                {
                    "error": "internal server error",
                    "details": {"type": err.__class__.__name__, "message": str(err)},
                }
            ),
            500,
        )

    @app.post("/api/area")
    def api_area():
        payload = _json_payload()
        model = AreaRequest.from_payload(payload)
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)
        allow_synthetic_fallback = bool(payload.get("allow_synthetic_fallback", False))

        bbox: Tuple[float, float, float, float] = tuple(model.bbox)  # type: ignore[assignment]
        if model.source == "rgi":
            raise ApiError("RGI source is not implemented in this build", 400)
        try:
            outline_geojson = get_outline_geojson(
                bbox=bbox,
                overpass_url=str(payload.get("overpass_url", "https://overpass-api.de/api/interpreter")),
                allow_fallback=allow_synthetic_fallback,
            )
        except Exception as exc:
            raise ApiError(f"Glacier outline fetch failed (no synthetic fallback): {exc}", 502) from exc
        if outline_geojson.get("features"):
            props = outline_geojson["features"][0].setdefault("properties", {})
            props["requested_source"] = model.source

        outline_path = job_dir / "outline.geojson"
        write_geojson(str(outline_path), outline_geojson)
        _merge_state(job_dir, {"bbox": model.bbox, "source": model.source, "outline_path": str(outline_path)})
        _update_report(job_dir, area={"outline_path": str(outline_path)})
        return jsonify({"job_id": job_id, "outline_geojson": outline_geojson, "outline_path": str(outline_path)})

    @app.post("/api/elevation")
    def api_elevation():
        payload = _json_payload()
        model = ElevationRequest.from_payload(payload)
        resolved_api_key = _resolve_opentopo_key(model.api_key)
        allow_synthetic_fallback = bool(payload.get("allow_synthetic_fallback", False))
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)
        bbox: Tuple[float, float, float, float] = tuple(model.bbox)  # type: ignore[assignment]

        try:
            dem_info = get_dem(
                bbox=bbox,
                resolution_m=model.resolution,
                dst_crs=local_utm_crs(bbox),
                dataset=model.dataset,
                api_key=resolved_api_key or None,
                opentopo_url=str(payload.get("opentopo_url", "https://portal.opentopography.org/API/globaldem")),
                timeout_s=int(payload.get("timeout", 20)),
                work_dir=str(job_dir),
                allow_fallback=allow_synthetic_fallback,
            )
        except Exception as exc:
            raise ApiError(_format_fetch_failure("DEM fetch failed (synthetic fallback disabled)", exc), 502) from exc
        dem = np.asarray(dem_info["array"], dtype=np.float32)
        transform = dem_info["transform"]
        crs = dem_info["crs"]

        dem_path = job_dir / "dem.tif"
        dem_preview = job_dir / "dem_preview.png"
        outline_geojson = _load_outline_geojson(job_dir)
        extent = _extent_from_transform(transform, dem.shape)
        write_geotiff(str(dem_path), dem, transform, crs)
        write_raster_preview_png(
            str(dem_preview),
            dem,
            title="DEM",
            cmap="terrain",
            extent=extent,
            outline_geojson=outline_geojson,
            raster_crs=crs,
        )

        state_patch = {
            "bbox": model.bbox,
            "resolution_m": model.resolution,
            "dataset": model.dataset,
            "dem_path": str(dem_path),
            "dem_source": dem_info.get("source"),
            "dem_fallback": bool(dem_info.get("fallback")),
            "api_key_present": bool(resolved_api_key),
        }
        _merge_state(job_dir, state_patch)
        _update_report(job_dir, elevation=state_patch)
        return jsonify(
            {
                "job_id": job_id,
                "dem_path": str(dem_path),
                "preview_path": str(dem_preview),
                "source": dem_info.get("source"),
                "fallback": bool(dem_info.get("fallback")),
            }
        )

    @app.post("/api/slope")
    def api_slope():
        payload = _json_payload()
        model = SlopeRequest.from_payload(payload)
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)

        dem_arr, transform, crs, _ = _read_raster(model.dem_path)
        glacier_mask = _mask_from_outline_if_available(job_dir, dem_arr.shape, transform, crs)
        pixel_x = abs(float(transform.a))
        pixel_y = abs(float(transform.e))
        theta_rad = slope_radians(dem_arr, pixel_size_x=pixel_x, pixel_size_y=pixel_y)
        theta_rad = np.where(glacier_mask, theta_rad, np.nan).astype(np.float32)
        slope_arr = theta_rad if model.units == "radians" else slope_degrees(theta_rad)

        slope_path = job_dir / "slope_angle.tif"
        slope_preview = job_dir / "slope_angle_preview.png"
        outline_geojson = _load_outline_geojson(job_dir)
        extent = _extent_from_transform(transform, slope_arr.shape)
        write_geotiff(str(slope_path), slope_arr, transform, crs)
        write_raster_preview_png(
            str(slope_preview),
            slope_arr,
            title=f"Slope Angle ({model.units})",
            cmap="magma",
            extent=extent,
            outline_geojson=outline_geojson,
            raster_crs=crs,
        )

        _merge_state(
            job_dir,
            {
                "dem_path": model.dem_path,
                "slope_path": str(slope_path),
                "slope_units": model.units,
                "slope_method": model.method,
            },
        )
        _update_report(job_dir, slope={"slope_path": str(slope_path), "slope_units": model.units})
        return jsonify({"job_id": job_id, "slope_path": str(slope_path), "preview_path": str(slope_preview)})

    @app.post("/api/average_slope")
    def api_average_slope():
        payload = _json_payload()
        model = AverageSlopeRequest.from_payload(payload)
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)

        slope_arr, transform, crs, _ = _read_raster(model.slope_path)
        state = _read_state(job_dir)
        units = str(state.get("slope_units", "radians"))
        slope_rad = np.radians(slope_arr) if units == "degrees" else slope_arr

        pixel_x = abs(float(transform.a))
        pixel_y = abs(float(transform.e))
        pixel_size_m = (pixel_x + pixel_y) / 2.0
        avg_rad = average_slope(slope_rad, pixel_size_m=pixel_size_m, grid_size_m=float(model.grid_size_m))
        glacier_mask = _mask_from_outline_if_available(job_dir, avg_rad.shape, transform, crs)
        avg_rad = np.where(glacier_mask, avg_rad, np.nan).astype(np.float32)

        avg_path = job_dir / "avg_slope.tif"
        avg_preview = job_dir / "avg_slope_preview.png"
        outline_geojson = _load_outline_geojson(job_dir)
        extent = _extent_from_transform(transform, avg_rad.shape)
        write_geotiff(str(avg_path), avg_rad, transform, crs)
        write_raster_preview_png(
            str(avg_preview),
            avg_rad,
            title="Average Slope (radians)",
            cmap="inferno",
            extent=extent,
            outline_geojson=outline_geojson,
            raster_crs=crs,
        )

        stats = {
            "grid_size_m": model.grid_size_m,
            "mean_slope_radians": float(np.nanmean(avg_rad)),
            "mean_slope_degrees": float(np.nanmean(np.degrees(avg_rad))),
        }
        write_json(str(job_dir / "avg_slope_stats.json"), stats)
        _merge_state(job_dir, {"avg_slope_path": str(avg_path), "avg_slope_units": "radians"})
        _update_report(job_dir, average_slope={"avg_slope_path": str(avg_path), "stats": stats})
        return jsonify({"job_id": job_id, "avg_slope_path": str(avg_path), "preview_path": str(avg_preview), "stats": stats})

    @app.post("/api/flow")
    def api_flow():
        payload = _json_payload()
        model = FlowRequest.from_payload(payload)
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)

        state = _read_state(job_dir)
        dem_path = str(state.get("dem_path", "")).strip()
        if not dem_path:
            raise ApiError("flow requires DEM generated first (call /api/elevation)", 400)

        dem_arr, transform, crs, _ = _read_raster(dem_path)
        glacier_mask = _mask_from_outline_if_available(job_dir, dem_arr.shape, transform, crs)
        flow_geo = flowlines_geojson(dem_arr, transform=transform, glacier_mask=glacier_mask)

        flow_path = job_dir / "flowlines.geojson"
        flow_preview = job_dir / "flowlines_preview.png"
        outline_geojson = _load_outline_geojson(job_dir)
        extent = _extent_from_transform(transform, dem_arr.shape)
        write_geojson(str(flow_path), flow_geo)
        write_flow_preview_png(
            str(flow_preview),
            dem_arr,
            flow_geo,
            title="Flowlines + Vectors",
            extent=extent,
            outline_geojson=outline_geojson,
            raster_crs=crs,
        )

        _merge_state(job_dir, {"flowlines_path": str(flow_path), "flow_method": model.method})
        _update_report(job_dir, flow={"flowlines_path": str(flow_path), "feature_count": len(flow_geo.get("features", []))})
        return jsonify(
            {
                "job_id": job_id,
                "flowlines_path": str(flow_path),
                "preview_path": str(flow_preview),
                "flowlines_geojson": flow_geo,
            }
        )

    @app.post("/api/depth")
    def api_depth():
        payload = _json_payload()
        model = DepthRequest.from_payload(payload)
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)

        avg_arr, transform, crs, _ = _read_raster(model.avg_slope_path)
        state = _read_state(job_dir)
        avg_units = str(state.get("avg_slope_units", "radians"))
        avg_rad = np.radians(avg_arr) if avg_units == "degrees" else avg_arr

        depth = depth_from_slope(
            avg_rad,
            depth_scale_m=(model.bulk_constant_m / model.f_prime),
            epsilon=1e-3,
            depth_min_m=0.0,
            depth_max_m=2000.0,
        )
        glacier_mask = _mask_from_outline_if_available(job_dir, depth.shape, transform, crs)
        depth = np.where(glacier_mask, depth, np.nan).astype(np.float32)

        depth_path = job_dir / "depth.tif"
        depth_preview = job_dir / "depth_preview.png"
        outline_geojson = _load_outline_geojson(job_dir)
        extent = _extent_from_transform(transform, depth.shape)
        write_geotiff(str(depth_path), depth, transform, crs)
        write_raster_preview_png(
            str(depth_preview),
            depth,
            title="Depth (m)",
            cmap="cividis",
            extent=extent,
            outline_geojson=outline_geojson,
            raster_crs=crs,
        )

        _merge_state(
            job_dir,
            {
                "depth_path": str(depth_path),
                "bulk_constant_m": model.bulk_constant_m,
                "f_prime": model.f_prime,
                "depth_scale_m": model.bulk_constant_m / model.f_prime,
            },
        )
        _update_report(
            job_dir,
            depth={
                "depth_path": str(depth_path),
                "mean_depth_m": float(np.nanmean(depth)),
                "bulk_constant_m": model.bulk_constant_m,
                "f_prime": model.f_prime,
                "depth_scale_m": model.bulk_constant_m / model.f_prime,
            },
        )
        return jsonify({"job_id": job_id, "depth_path": str(depth_path), "preview_path": str(depth_preview)})

    @app.post("/api/run_full")
    def api_run_full():
        payload = _json_payload()
        model = RunFullRequest.from_payload(payload)
        resolved_api_key = _resolve_opentopo_key(model.api_key)
        allow_synthetic_fallback = bool(payload.get("allow_synthetic_fallback", False))
        plot_max_dim = _sanitize_plot_max_dim(payload.get("plot_max_dim", 220))
        job_id = _ensure_job_id(payload)
        job_dir = _job_dir(job_id)
        bulk_constant_m, f_prime, depth_scale_m = _resolve_depth_params(
            payload,
            default_bulk_constant_m=float(model.bulk_constant_m),
        )
        async_mode = bool(payload.get("async_mode", False))
        dataset = str(payload.get("dataset", "COP30")).strip() or "COP30"

        config = PipelineConfig(
            job_id=job_id,
            bbox=tuple(model.bbox),
            source=model.source,
            dataset=dataset,
            opentopo_api_key=resolved_api_key or None,
            grid_size_m=float(model.grid_size_m),
            resolution_m=float(model.resolution),
            tau_f=depth_scale_m,
            slope_units=model.slope_units,
            output_dir=str(job_dir),
            overpass_url=str(payload.get("overpass_url", "https://overpass-api.de/api/interpreter")),
            opentopo_url=str(payload.get("opentopo_url", "https://portal.opentopography.org/API/globaldem")),
            request_timeout_s=int(payload.get("timeout", 60)),
            allow_outline_fallback=allow_synthetic_fallback,
            allow_synthetic_dem_fallback=allow_synthetic_fallback,
        )
        config.validate()
        save_config_yaml(config, str(job_dir / "config.yaml"))
        outline_override = payload.get("outline_geojson_override")
        if outline_override is not None and not isinstance(outline_override, dict):
            raise ApiError("'outline_geojson_override' must be a GeoJSON object when provided", 400)

        def progress_cb(done: int, total: int, message: str) -> None:
            _merge_state(
                job_dir,
                {
                    "run_status": "running",
                    "progress": {
                        "phase": "dem_fetch",
                        "done": int(done),
                        "total": int(max(total, 1)),
                        "message": str(message),
                    },
                },
            )

        def execute_run() -> Dict[str, Any]:
            def _should_cancel() -> bool:
                with RUN_TASKS_LOCK:
                    task = RUN_TASKS.get(job_id, {})
                    return bool(task.get("cancel_requested", False))

            try:
                summary = run_pipeline(
                    config,
                    outline_geojson_override=outline_override,
                    dem_progress_callback=progress_cb,
                    should_cancel=_should_cancel,
                )
            except Exception as exc:
                cancelled = "cancel" in str(exc).lower()
                _merge_state(
                    job_dir,
                    {
                        "run_status": "cancelled" if cancelled else "failed",
                        "error": ("Run cancelled by user" if cancelled else _format_fetch_failure("Run failed (synthetic fallback disabled)", exc)),
                    },
                )
                with RUN_TASKS_LOCK:
                    RUN_TASKS[job_id] = {"status": "cancelled" if cancelled else "failed", "cancel_requested": cancelled}
                raise
            _write_topomap_overlays(job_dir)
            summary["plot_max_dim"] = plot_max_dim
            summary["bulk_constant_m"] = bulk_constant_m
            summary["f_prime"] = f_prime
            summary["depth_scale_m"] = depth_scale_m
            _merge_state(
                job_dir,
                {
                    "run_status": "completed",
                    "progress": {"phase": "complete", "done": 1, "total": 1, "message": "Completed"},
                    **summary,
                },
            )
            _update_report(job_dir, run_full=summary, completed_at=datetime.now(UTC).isoformat())
            with RUN_TASKS_LOCK:
                RUN_TASKS[job_id] = {"status": "completed", "cancel_requested": False}
            return summary

        _merge_state(
            job_dir,
            {
                "run_status": "running",
                "progress": {"phase": "queued", "done": 0, "total": 1, "message": "Starting pipeline"},
                "bulk_constant_m": bulk_constant_m,
                "f_prime": f_prime,
                "depth_scale_m": depth_scale_m,
            },
        )

        if async_mode:
            with RUN_TASKS_LOCK:
                existing = RUN_TASKS.get(job_id, {})
                if existing.get("status") == "running":
                    raise ApiError(f"job '{job_id}' is already running", 409)
                RUN_TASKS[job_id] = {"status": "running", "cancel_requested": False}

            def _runner() -> None:
                try:
                    execute_run()
                except Exception:
                    pass

            threading.Thread(target=_runner, daemon=True).start()
            return jsonify(
                {
                    "job_id": job_id,
                    "message": "pipeline started",
                    "status_url": f"/api/run_status/{job_id}",
                }
            )

        try:
            summary = execute_run()
        except Exception as exc:
            raise ApiError(_format_fetch_failure("Run failed (synthetic fallback disabled)", exc), 502) from exc

        artifacts = _artifact_paths(job_dir)
        return jsonify(
            {
                "job_id": job_id,
                "message": "pipeline completed",
                "artifacts": artifacts,
                "summary": summary,
                "report_url": f"/api/report/{job_id}",
                "plot_data_url": f"/api/plot_data/{job_id}",
            }
        )

    @app.get("/api/run_status/<job_id>")
    def api_run_status(job_id: str):
        job_dir = OUTPUT_ROOT / job_id
        if not job_dir.exists():
            raise ApiError("job not found", 404, {"job_id": job_id})
        state = _read_state(job_dir)
        status = str(state.get("run_status", "unknown"))
        payload: Dict[str, Any] = {
            "job_id": job_id,
            "status": status,
            "progress": state.get("progress", {}),
        }
        if status == "completed":
            artifacts = _artifact_paths(job_dir)
            summary = {
                "dem_source": state.get("dem_source"),
                "dem_fallback": state.get("dem_fallback"),
                "plot_max_dim": state.get("plot_max_dim"),
                "bulk_constant_m": state.get("bulk_constant_m"),
                "f_prime": state.get("f_prime"),
                "depth_scale_m": state.get("depth_scale_m"),
                "grid_size_m": state.get("grid_size_m"),
                "resolution_m": state.get("resolution_m"),
                "flowline_count": state.get("flowline_count"),
                "bundle_path": state.get("bundle_path"),
                "outline_source": state.get("outline_source"),
            }
            payload.update(
                {
                    "artifacts": artifacts,
                    "summary": summary,
                    "report_url": f"/api/report/{job_id}",
                    "plot_data_url": f"/api/plot_data/{job_id}",
                }
            )
        if status == "failed":
            payload["error"] = state.get("error", "run failed")
        if status == "cancelling":
            payload["error"] = state.get("error", "run cancellation requested")
        if status == "cancelled":
            payload["error"] = state.get("error", "run cancelled")
        return jsonify(payload)

    @app.post("/api/run_cancel/<job_id>")
    def api_run_cancel(job_id: str):
        job_dir = OUTPUT_ROOT / job_id
        if not job_dir.exists():
            raise ApiError("job not found", 404, {"job_id": job_id})
        state = _read_state(job_dir)
        existing_status = str(state.get("run_status", "")).lower()
        if existing_status in {"completed", "failed", "cancelled"}:
            raise ApiError(f"job is already {existing_status}", 409, {"job_id": job_id})
        with RUN_TASKS_LOCK:
            task = RUN_TASKS.get(job_id)
            if not task:
                RUN_TASKS[job_id] = {"status": "cancelling", "cancel_requested": True}
            else:
                task["cancel_requested"] = True
                if task.get("status") == "running":
                    task["status"] = "cancelling"
        _merge_state(
            job_dir,
            {
                "run_status": "cancelling",
                "progress": {"phase": "cancelling", "done": 0, "total": 1, "message": "Cancellation requested"},
                "error": "Run cancellation requested",
            },
        )
        return jsonify({"job_id": job_id, "status": "cancelling", "message": "Cancellation requested"})

    @app.post("/api/reanalyze")
    def api_reanalyze():
        payload = _json_payload()
        job_id = str(payload.get("job_id", "")).strip()
        if not job_id:
            raise ApiError("'job_id' is required", 400)

        job_dir = _job_dir(job_id)
        state = _read_state(job_dir)
        dem_path = str(state.get("dem_path", "")).strip() or str(job_dir / "dem.tif")
        dem_path_candidate = Path(dem_path)
        if not dem_path_candidate.is_absolute():
            dem_path_candidate = (BASE_DIR / dem_path_candidate).resolve()
        if not dem_path_candidate.exists():
            raise ApiError("reanalyze requires existing DEM for this job", 400, {"job_id": job_id, "dem_path": dem_path})

        grid_size_m = int(payload.get("grid_size_m", state.get("grid_size_m", 100)))
        if grid_size_m <= 0:
            raise ApiError("'grid_size_m' must be > 0", 400)
        bulk_constant_m, f_prime, depth_scale_m = _resolve_depth_params(
            payload={
                "bulk_constant_m": payload.get(
                    "bulk_constant_m",
                    state.get("bulk_constant_m", state.get("tau_0_kpa", state.get("depth_scale_m", 11.0))),
                ),
                "f_prime": payload.get("f_prime", state.get("f_prime", 1.0)),
            },
            default_bulk_constant_m=float(state.get("bulk_constant_m", state.get("depth_scale_m", 11.0))),
        )
        slope_units = str(payload.get("slope_units", state.get("slope_units", "radians"))).strip().lower()
        if slope_units not in {"radians", "degrees"}:
            raise ApiError("'slope_units' must be 'radians' or 'degrees'", 400)
        plot_max_dim = _sanitize_plot_max_dim(payload.get("plot_max_dim", state.get("plot_max_dim", 220)))

        recompute_summary = _reanalyze_from_existing_dem(
            job_dir=job_dir,
            dem_path=dem_path,
            grid_size_m=grid_size_m,
            depth_scale_m=depth_scale_m,
            slope_units=slope_units,
        )
        recompute_summary["plot_max_dim"] = plot_max_dim
        recompute_summary["bulk_constant_m"] = bulk_constant_m
        recompute_summary["f_prime"] = f_prime
        recompute_summary["depth_scale_m"] = depth_scale_m
        _write_topomap_overlays(job_dir)
        _merge_state(job_dir, recompute_summary)
        _update_report(job_dir, reanalyze=recompute_summary, completed_at=datetime.now(UTC).isoformat())

        artifacts = _artifact_paths(job_dir)
        return jsonify(
            {
                "job_id": job_id,
                "message": "reanalyze completed (used cached DEM)",
                "artifacts": artifacts,
                "summary": recompute_summary,
                "report_url": f"/api/report/{job_id}",
                "plot_data_url": f"/api/plot_data/{job_id}",
            }
        )

    @app.get("/api/report/<job_id>")
    def api_report(job_id: str):
        job_dir = OUTPUT_ROOT / job_id
        if not job_dir.exists() or not job_dir.is_dir():
            raise ApiError("job not found", status_code=404, details={"job_id": job_id})
        _write_topomap_overlays(job_dir)

        expected = [
            "config.yaml",
            "outline.geojson",
            "dem.tif",
            "dem_preview.png",
            "elevation_plot.png",
            "slope_angle.tif",
            "slope_angle_preview.png",
            "avg_slope.tif",
            "avg_slope_preview.png",
            "flowlines.geojson",
            "flowlines_preview.png",
            "depth.tif",
            "depth_preview.png",
            "depth_opentopo_overlay.png",
            "flow_opentopo_overlay.png",
            "depth_topo_composite.png",
            "flow_topo_composite.png",
            "summary_2x2.png",
            "final_overlay.png",
            "report.json",
        ]
        zip_path = build_zip_bundle(str(job_dir), f"report_{job_id}.zip", expected)
        return send_file(zip_path, mimetype="application/zip", as_attachment=True, download_name=f"report_{job_id}.zip")

    @app.post("/api/select_glacier")
    def api_select_glacier():
        payload = _json_payload()
        lon = payload.get("lon")
        lat = payload.get("lat")
        if lon is None or lat is None:
            raise ApiError("'lon' and 'lat' are required", 400)
        try:
            lon_f = float(lon)
            lat_f = float(lat)
        except (TypeError, ValueError) as exc:
            raise ApiError("'lon' and 'lat' must be numeric", 400) from exc

        radius_m = int(payload.get("radius_m", 5000))
        radius_m = max(100, min(25000, radius_m))
        overpass_url = str(payload.get("overpass_url", "https://overpass-api.de/api/interpreter"))
        timeout_s = int(payload.get("timeout", 20))

        try:
            result = select_glacier_at_point(
                lon=lon_f,
                lat=lat_f,
                overpass_url=overpass_url,
                radius_m=radius_m,
                timeout_s=timeout_s,
            )
        except Exception as exc:
            raise ApiError(f"Glacier selection failed: {exc}", 502) from exc

        return jsonify(result)

    @app.post("/api/select_glacier_relation")
    def api_select_glacier_relation():
        payload = _json_payload()
        relation_text = str(payload.get("relation_text", "")).strip()
        relation_id_raw = payload.get("relation_id")
        overpass_url = str(payload.get("overpass_url", "https://overpass-api.de/api/interpreter"))
        timeout_s = int(payload.get("timeout", 20))

        try:
            if relation_id_raw is not None:
                relation_id = int(relation_id_raw)
            else:
                if not relation_text:
                    raise ValueError("Provide relation_text or relation_id")
                relation_id = parse_relation_id_from_text(relation_text)
        except Exception as exc:
            raise ApiError(f"Failed to parse relation id: {exc}", 400) from exc

        try:
            result = select_glacier_by_relation_id(
                relation_id=relation_id,
                overpass_url=overpass_url,
                timeout_s=timeout_s,
            )
        except Exception as exc:
            glacier_name = parse_glacier_name_from_text(relation_text) if relation_text else None
            if not glacier_name:
                raise ApiError(f"Relation glacier selection failed: {exc}", 502) from exc
            try:
                result = select_glacier_by_name(
                    glacier_name=glacier_name,
                    overpass_url=overpass_url,
                    timeout_s=timeout_s,
                )
            except Exception as exc2:
                raise ApiError(
                    f"Relation glacier selection failed: {exc}. Name fallback '{glacier_name}' also failed: {exc2}",
                    502,
                ) from exc2

        return jsonify(result)

    @app.get("/api/jobs/<job_id>")
    def api_job_metadata(job_id: str):
        report_path = OUTPUT_ROOT / job_id / "report.json"
        if not report_path.exists():
            raise ApiError("job not found", status_code=404, details={"job_id": job_id})
        return jsonify(json.loads(report_path.read_text(encoding="utf-8")))

    @app.get("/api/jobs")
    def api_jobs_list():
        if not OUTPUT_ROOT.exists():
            return jsonify({"jobs": []})
        jobs: List[Dict[str, Any]] = []
        for child in OUTPUT_ROOT.iterdir():
            if not child.is_dir():
                continue
            state = _read_state(child)
            report_payload: Dict[str, Any] = {}
            report_path = child / "report.json"
            if report_path.exists():
                try:
                    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    report_payload = {}
            updated_at = (
                report_payload.get("updated_at")
                or state.get("updated_at")
                or datetime.fromtimestamp(child.stat().st_mtime, tz=UTC).isoformat()
            )
            jobs.append(
                {
                    "job_id": child.name,
                    "updated_at": updated_at,
                    "run_status": state.get("run_status", "unknown"),
                    "dem_source": state.get("dem_source", report_payload.get("dem_source")),
                }
            )
        jobs.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        return jsonify({"jobs": jobs})

    @app.get("/api/load_job/<job_id>")
    def api_load_job(job_id: str):
        job_dir = OUTPUT_ROOT / job_id
        if not job_dir.exists() or not job_dir.is_dir():
            raise ApiError("job not found", status_code=404, details={"job_id": job_id})
        state = _read_state(job_dir)
        report_payload: Dict[str, Any] = {}
        report_path = job_dir / "report.json"
        if report_path.exists():
            try:
                report_payload = json.loads(report_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                report_payload = {}
        summary = {
            "dem_source": state.get("dem_source", report_payload.get("dem_source")),
            "dem_fallback": state.get("dem_fallback", report_payload.get("dem_fallback")),
            "plot_max_dim": state.get("plot_max_dim", report_payload.get("plot_max_dim")),
            "bulk_constant_m": state.get("bulk_constant_m", report_payload.get("bulk_constant_m", report_payload.get("tau_0_kpa"))),
            "f_prime": state.get("f_prime", report_payload.get("f_prime")),
            "depth_scale_m": state.get("depth_scale_m", report_payload.get("depth_scale_m", report_payload.get("tau_f"))),
            "grid_size_m": state.get("grid_size_m", report_payload.get("grid_size_m")),
            "resolution_m": state.get("resolution_m", report_payload.get("resolution_m")),
            "flowline_count": state.get("flowline_count", report_payload.get("flowline_count")),
            "bundle_path": state.get("bundle_path", report_payload.get("bundle_path")),
            "outline_source": state.get("outline_source", report_payload.get("outline_source")),
        }
        return jsonify(
            {
                "job_id": job_id,
                "message": "loaded existing job",
                "artifacts": _artifact_paths(job_dir),
                "summary": summary,
                "report_url": f"/api/report/{job_id}",
                "plot_data_url": f"/api/plot_data/{job_id}",
            }
        )

    @app.get("/api/plot_data/<job_id>")
    def api_plot_data(job_id: str):
        job_dir = OUTPUT_ROOT / job_id
        if not job_dir.exists() or not job_dir.is_dir():
            raise ApiError("job not found", status_code=404, details={"job_id": job_id})

        dem_path = job_dir / "dem.tif"
        flow_field_path = job_dir / "flow_field.tif"
        slope_path = job_dir / "slope_angle.tif"
        avg_slope_path = job_dir / "avg_slope.tif"
        depth_path = job_dir / "depth.tif"
        outline_path = job_dir / "outline.geojson"
        flowlines_path = job_dir / "flowlines.geojson"

        if not dem_path.exists():
            raise ApiError("plot data unavailable; run elevation/full pipeline first", 400)

        state_payload = _read_state(job_dir)
        plot_max_dim = _sanitize_plot_max_dim(request.args.get("max_dim", state_payload.get("plot_max_dim", 220)))

        dem_plot = _sample_raster_for_plot(dem_path, max_dim=plot_max_dim)
        flow_field_plot = _sample_raster_for_plot(flow_field_path, max_dim=plot_max_dim) if flow_field_path.exists() else dem_plot
        slope_plot = _sample_raster_for_plot(slope_path, max_dim=plot_max_dim) if slope_path.exists() else None
        avg_slope_plot = _sample_raster_for_plot(avg_slope_path, max_dim=plot_max_dim) if avg_slope_path.exists() else None
        depth_plot = _sample_raster_for_plot(depth_path, max_dim=plot_max_dim) if depth_path.exists() else None
        outline_geojson = json.loads(outline_path.read_text(encoding="utf-8")) if outline_path.exists() else {}
        report_payload: Dict[str, Any] = {}
        report_path = job_dir / "report.json"
        if report_path.exists():
            report_payload = json.loads(report_path.read_text(encoding="utf-8"))

        outline_traces = _outline_traces_in_raster_crs(outline_geojson, dem_plot["crs"])
        flowlines_traces = _flowline_traces(flowlines_path)
        depth_mask = depth_plot["z"] if depth_plot else None
        vector_field = _vector_field_from_dem_sample(flow_field_plot, valid_mask_z=depth_mask)
        topomap = _topomap_payload(job_id, dem_plot["crs"])

        return jsonify(
            {
                "job_id": job_id,
                "crs": dem_plot["crs"],
                "rasters": {
                    "dem": {
                        "x": dem_plot["x"],
                        "y": dem_plot["y"],
                        "z": dem_plot["z"],
                        "label": "Elevation",
                        "units": "m",
                    },
                    "slope": (
                        {
                            "x": slope_plot["x"],
                            "y": slope_plot["y"],
                            "z": slope_plot["z"],
                            "label": "Slope",
                            "units": "radians/degrees",
                        }
                        if slope_plot
                        else None
                    ),
                    "flow_background": {
                        "x": flow_field_plot["x"],
                        "y": flow_field_plot["y"],
                        "z": flow_field_plot["z"],
                        "label": "Flow Background",
                        "units": "m",
                    },
                    "avg_slope": (
                        {
                            "x": avg_slope_plot["x"],
                            "y": avg_slope_plot["y"],
                            "z": avg_slope_plot["z"],
                            "label": "Average Slope",
                            "units": "radians/degrees",
                        }
                        if avg_slope_plot
                        else None
                    ),
                    "depth": (
                        {
                            "x": depth_plot["x"],
                            "y": depth_plot["y"],
                            "z": depth_plot["z"],
                            "label": "Depth",
                            "units": "m",
                        }
                        if depth_plot
                        else None
                    ),
                },
                "outline_traces": outline_traces,
                "flowlines_traces": flowlines_traces,
                "vector_field": vector_field,
                "topomap": topomap,
                "params": {
                    "grid_size_m": state_payload.get("grid_size_m", report_payload.get("grid_size_m")),
                    "bulk_constant_m": state_payload.get(
                        "bulk_constant_m",
                        report_payload.get("bulk_constant_m", report_payload.get("tau_0_kpa")),
                    ),
                    "f_prime": state_payload.get("f_prime", report_payload.get("f_prime")),
                    "depth_scale_m": state_payload.get(
                        "depth_scale_m",
                        report_payload.get("depth_scale_m", report_payload.get("tau_f")),
                    ),
                    "slope_units": state_payload.get("slope_units", report_payload.get("slope_units")),
                    "plot_max_dim": plot_max_dim,
                },
            }
        )

    return app


def _json_payload() -> Dict[str, Any]:
    payload = request.get_json(silent=True)
    if payload is None:
        raise ApiError("request body must be valid JSON", status_code=400)
    if not isinstance(payload, dict):
        raise ApiError("JSON body must be an object", status_code=400)
    return payload


def _format_fetch_failure(context: str, exc: Exception) -> str:
    msg = str(exc)
    hints: List[str] = []
    lower = msg.lower()

    if "api key" in lower:
        hints.append("verify OPEN_TOPO_KEY in .env or provide api_key in the request")
    if "too small to be valid geotiff" in lower:
        hints.append("the API likely returned an error payload or empty tile")
    if "timeout" in lower or "timed out" in lower:
        hints.append("increase timeout and/or reduce bbox size")
    if "connection" in lower or "max retries exceeded" in lower:
        hints.append("check internet access and endpoint availability")

    hints.append("confirm bbox intersects valid terrain coverage")
    hints.append("try a smaller bbox or coarser resolution")

    deduped: List[str] = []
    for hint in hints:
        if hint not in deduped:
            deduped.append(hint)

    return f"{context}: {msg}. Possible fixes: " + "; ".join(deduped) + "."


def _sanitize_plot_max_dim(value: Any) -> int:
    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        return 220
    return max(1, min(2000, numeric))


def _ensure_job_id(payload: Dict[str, Any]) -> str:
    raw = str(payload.get("job_id", "")).strip()
    if raw:
        return raw
    return f"job_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def _job_dir(job_id: str) -> Path:
    job_dir = OUTPUT_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def _state_path(job_dir: Path) -> Path:
    return job_dir / "state.json"


def _read_state(job_dir: Path) -> Dict[str, Any]:
    path = _state_path(job_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _merge_state(job_dir: Path, patch: Dict[str, Any]) -> None:
    data = _read_state(job_dir)
    data.update(patch)
    write_json(str(_state_path(job_dir)), data)


def _update_report(job_dir: Path, **patch: Any) -> None:
    report_path = job_dir / "report.json"
    data: Dict[str, Any] = {}
    if report_path.exists():
        try:
            data = json.loads(report_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            data = {}
    data.update(patch)
    data["job_id"] = job_dir.name
    data["output_dir"] = str(job_dir)
    data["updated_at"] = datetime.now(UTC).isoformat()
    write_json(str(report_path), data)


def _load_outline_geojson(job_dir: Path) -> Dict[str, Any] | None:
    outline_path = job_dir / "outline.geojson"
    if not outline_path.exists():
        return None
    try:
        payload = json.loads(outline_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _extent_from_transform(transform, shape: tuple[int, int]) -> tuple[float, float, float, float]:
    h, w = shape
    west, south, east, north = array_bounds(h, w, transform)
    return west, east, south, north


def _artifact_paths(job_dir: Path) -> Dict[str, str]:
    names = {
        "outline_geojson": "outline.geojson",
        "dem_path": "dem.tif",
        "dem_preview": "dem_preview.png",
        "elevation_plot": "elevation_plot.png",
        "slope_path": "slope_angle.tif",
        "slope_preview": "slope_angle_preview.png",
        "avg_slope_path": "avg_slope.tif",
        "avg_slope_preview": "avg_slope_preview.png",
        "flowlines_path": "flowlines.geojson",
        "flow_field_path": "flow_field.tif",
        "flow_preview": "flowlines_preview.png",
        "depth_path": "depth.tif",
        "depth_preview": "depth_preview.png",
        "depth_topomap_overlay": "depth_opentopo_overlay.png",
        "flow_topomap_overlay": "flow_opentopo_overlay.png",
        "depth_topo_composite": "depth_topo_composite.png",
        "flow_topo_composite": "flow_topo_composite.png",
        "summary_2x2": "summary_2x2.png",
        "final_overlay": "final_overlay.png",
        "report_json": "report.json",
    }
    out: Dict[str, str] = {}
    for key, filename in names.items():
        path = job_dir / filename
        if path.exists():
            out[key] = str(path)
    return out


def _read_raster(path: str) -> tuple[np.ndarray, Any, Any, Any]:
    raster_path = Path(path)
    if not raster_path.is_absolute():
        raster_path = (BASE_DIR / raster_path).resolve()
    if not raster_path.exists():
        raise ApiError("raster path not found", 400, {"path": str(raster_path)})
    with rasterio.open(raster_path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
        if nodata is not None:
            arr = np.where(arr == nodata, np.nan, arr)
        return arr, src.transform, src.crs, nodata


def _sample_raster_for_plot(path: Path, max_dim: int = 220) -> Dict[str, Any]:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
        if nodata is not None:
            arr = np.where(arr == nodata, np.nan, arr)

        rows, cols = arr.shape
        stride = max(1, int(np.ceil(max(rows, cols) / max_dim)))
        row_ids = np.arange(0, rows, stride, dtype=int)
        col_ids = np.arange(0, cols, stride, dtype=int)
        sampled = arr[::stride, ::stride]

        x_vals = [float(raster_xy(src.transform, 0, int(c), offset="center")[0]) for c in col_ids]
        y_vals = [float(raster_xy(src.transform, int(r), 0, offset="center")[1]) for r in row_ids]

        return {
            "x": x_vals,
            "y": y_vals,
            "z": _array_to_json(sampled),
            "array": sampled,
            "pixel_x": abs(float(src.transform.a)) * stride,
            "pixel_y": abs(float(src.transform.e)) * stride,
            "jacobian": {
                "jx_col": float(src.transform.a) * stride,
                "jy_col": float(src.transform.d) * stride,
                "jx_row": float(src.transform.b) * stride,
                "jy_row": float(src.transform.e) * stride,
            },
            "crs": src.crs.to_string() if src.crs else "",
        }


def _array_to_json(arr: np.ndarray) -> List[List[float | None]]:
    out: List[List[float | None]] = []
    for row in arr:
        json_row: List[float | None] = []
        for value in row:
            if np.isfinite(value):
                json_row.append(float(value))
            else:
                json_row.append(None)
        out.append(json_row)
    return out


def _outline_traces_in_raster_crs(outline_geojson: Dict[str, Any], dst_crs: str) -> List[Dict[str, List[float]]]:
    features = outline_geojson.get("features", [])
    if not features:
        return []

    transformer = Transformer.from_crs("EPSG:4326", dst_crs, always_xy=True) if dst_crs and dst_crs != "EPSG:4326" else None
    traces: List[Dict[str, List[float]]] = []

    for feature in features:
        geom = feature.get("geometry", {})
        gtype = geom.get("type")
        coords = geom.get("coordinates", [])

        rings: List[List[List[float]]] = []
        if gtype == "Polygon":
            rings = coords
        elif gtype == "MultiPolygon":
            for poly in coords:
                rings.extend(poly)

        for ring in rings:
            xs: List[float] = []
            ys: List[float] = []
            for pt in ring:
                if len(pt) < 2:
                    continue
                x, y = float(pt[0]), float(pt[1])
                if transformer is not None:
                    x, y = transformer.transform(x, y)
                if not (np.isfinite(x) and np.isfinite(y)):
                    continue
                xs.append(x)
                ys.append(y)
            if len(xs) >= 3:
                traces.append({"x": xs, "y": ys})
    return traces


def _flowline_traces(flowlines_path: Path) -> List[Dict[str, List[float]]]:
    if not flowlines_path.exists():
        return []
    payload = json.loads(flowlines_path.read_text(encoding="utf-8"))
    traces: List[Dict[str, List[float]]] = []
    for feature in payload.get("features", []):
        geom = feature.get("geometry", {})
        if geom.get("type") != "LineString":
            continue
        coords = geom.get("coordinates", [])
        xs: List[float] = []
        ys: List[float] = []
        for c in coords:
            if len(c) < 2:
                continue
            x = float(c[0])
            y = float(c[1])
            if not (np.isfinite(x) and np.isfinite(y)):
                continue
            xs.append(x)
            ys.append(y)
        if len(xs) >= 2:
            traces.append({"x": xs, "y": ys})
    return traces


def _reanalyze_from_existing_dem(
    job_dir: Path,
    dem_path: str,
    grid_size_m: int,
    depth_scale_m: float,
    slope_units: str,
) -> Dict[str, Any]:
    state = _read_state(job_dir)
    outline_geojson = _load_outline_geojson(job_dir)
    dem_arr, transform, crs, _ = _read_raster(dem_path)
    glacier_mask = _mask_from_outline_if_available(job_dir, dem_arr.shape, transform, crs)
    px = abs(float(transform.a))
    py = abs(float(transform.e))
    pixel_size_m = (px + py) / 2.0
    extent = _extent_from_transform(transform, dem_arr.shape)

    theta_rad = slope_radians(dem_arr, pixel_size_x=px, pixel_size_y=py)
    theta_rad = np.where(glacier_mask, theta_rad, np.nan).astype(np.float32)
    slope_arr = theta_rad if slope_units == "radians" else slope_degrees(theta_rad)
    slope_path = job_dir / "slope_angle.tif"
    write_geotiff(str(slope_path), slope_arr, transform, crs)
    write_raster_preview_png(
        str(job_dir / "slope_angle_preview.png"),
        slope_arr,
        title=f"Slope Angle ({slope_units})",
        cmap="magma",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    avg_theta = average_slope(theta_rad, pixel_size_m=pixel_size_m, grid_size_m=float(grid_size_m))
    avg_theta = np.where(glacier_mask, avg_theta, np.nan).astype(np.float32)
    avg_slope_out = avg_theta if slope_units == "radians" else slope_degrees(avg_theta)
    avg_path = job_dir / "avg_slope.tif"
    write_geotiff(str(avg_path), avg_slope_out, transform, crs)
    write_raster_preview_png(
        str(job_dir / "avg_slope_preview.png"),
        avg_slope_out,
        title=f"Average Slope ({slope_units})",
        cmap="inferno",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    flow_field = average_slope(dem_arr, pixel_size_m=pixel_size_m, grid_size_m=float(grid_size_m))
    flow_field = np.where(glacier_mask, flow_field, np.nan).astype(np.float32)
    flow_field_path = job_dir / "flow_field.tif"
    write_geotiff(str(flow_field_path), flow_field, transform, crs)

    flow_geo = flowlines_geojson(flow_field, transform=transform, glacier_mask=glacier_mask)
    flow_path = job_dir / "flowlines.geojson"
    write_geojson(str(flow_path), flow_geo)
    write_flow_preview_png(
        str(job_dir / "flowlines_preview.png"),
        dem_arr,
        flow_geo,
        title="Flowlines + Vectors",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    depth = depth_from_slope(
        avg_theta,
        depth_scale_m=depth_scale_m,
        epsilon=1e-3,
        depth_min_m=0.0,
        depth_max_m=2000.0,
    )
    depth = np.where(glacier_mask, depth, np.nan).astype(np.float32)
    depth_path = job_dir / "depth.tif"
    write_geotiff(str(depth_path), depth, transform, crs)
    write_raster_preview_png(
        str(job_dir / "depth_preview.png"),
        depth,
        title="Depth (m)",
        cmap="cividis",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )
    write_overlay_png(
        str(job_dir / "final_overlay.png"),
        depth,
        flow_geo,
        title="Final Overlay: Depth + Flow",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    return {
        "dem_path": str(Path(dem_path)),
        "slope_path": str(slope_path),
        "avg_slope_path": str(avg_path),
        "flowlines_path": str(flow_path),
        "flow_field_path": str(flow_field_path),
        "depth_path": str(depth_path),
        "slope_units": slope_units,
        "grid_size_m": grid_size_m,
        "depth_scale_m": depth_scale_m,
        "flowline_count": len(flow_geo.get("features", [])),
        "mean_slope_radians": float(np.nanmean(theta_rad)),
        "mean_slope_degrees": float(np.nanmean(np.degrees(theta_rad))),
        "mean_depth_m": float(np.nanmean(depth)),
        "dem_source": state.get("dem_source", "unknown"),
        "outline_source": state.get("outline_source", "unknown"),
        "dem_fallback": bool(state.get("dem_fallback", False)),
        "reused_dem": True,
    }


def _write_topomap_overlays(job_dir: Path) -> None:
    dem_path = job_dir / "dem.tif"
    depth_path = job_dir / "depth.tif"
    flow_path = job_dir / "flowlines.geojson"
    outline_geojson = _load_outline_geojson(job_dir)
    if not dem_path.exists() or not depth_path.exists():
        return

    with rasterio.open(dem_path) as src:
        rows, cols = src.shape
        transform = src.transform
        crs = src.crs
        dem_arr = src.read(1).astype(np.float32)
        dem_nodata = src.nodata
        if dem_nodata is not None:
            dem_arr = np.where(dem_arr == dem_nodata, np.nan, dem_arr)
        if not crs:
            return
    with rasterio.open(depth_path) as src:
        depth_arr = src.read(1).astype(np.float32)
        nodata = src.nodata
        if nodata is not None:
            depth_arr = np.where(depth_arr == nodata, np.nan, depth_arr)
    avg_slope_arr = None
    avg_slope_path = job_dir / "avg_slope.tif"
    if avg_slope_path.exists():
        with rasterio.open(avg_slope_path) as src:
            avg_slope_arr = src.read(1).astype(np.float32)
            nodata = src.nodata
            if nodata is not None:
                avg_slope_arr = np.where(avg_slope_arr == nodata, np.nan, avg_slope_arr)

    extent = _extent_from_transform(transform, dem_arr.shape)
    write_raster_preview_png(
        str(job_dir / "elevation_plot.png"),
        dem_arr,
        title="Elevation (m)",
        cmap="terrain",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )
    depth_stats = _save_depth_overlay_png(job_dir / "depth_opentopo_overlay.png", depth_arr)
    _merge_state(job_dir, {"depth_legend": depth_stats})
    _save_depth_topo_composite_png(job_dir / "depth_topo_composite.png", dem_arr, depth_arr, transform, outline_geojson, crs)
    if flow_path.exists():
        flow_geo = json.loads(flow_path.read_text(encoding="utf-8"))
        _save_flow_overlay_png(job_dir / "flow_opentopo_overlay.png", flow_geo, transform, rows, cols, dem_arr, depth_arr)
        _save_flow_topo_composite_png(
            job_dir / "flow_topo_composite.png",
            flow_geo,
            transform,
            rows,
            cols,
            dem_arr,
            depth_arr,
            outline_geojson,
            crs,
        )
    else:
        _save_flow_overlay_png(job_dir / "flow_opentopo_overlay.png", {}, transform, rows, cols, dem_arr, depth_arr)
        _save_flow_topo_composite_png(
            job_dir / "flow_topo_composite.png",
            {},
            transform,
            rows,
            cols,
            dem_arr,
            depth_arr,
            outline_geojson,
            crs,
        )
    _save_summary_2x2_png(
        path=job_dir / "summary_2x2.png",
        dem_arr=dem_arr,
        avg_slope_arr=avg_slope_arr,
        depth_arr=depth_arr,
        flow_geo=(json.loads(flow_path.read_text(encoding="utf-8")) if flow_path.exists() else {}),
        transform=transform,
        rows=rows,
        cols=cols,
        outline_geojson=outline_geojson,
        raster_crs=crs,
        grid_size_m=float(_read_state(job_dir).get("grid_size_m", 0.0)),
        slope_units=str(_read_state(job_dir).get("slope_units", "radians")),
    )


def _save_depth_overlay_png(path: Path, depth_arr: np.ndarray) -> Dict[str, float]:
    valid = np.isfinite(depth_arr)
    rgba = np.zeros((*depth_arr.shape, 4), dtype=np.uint8)
    stats: Dict[str, float] = {"min_m": 0.0, "max_m": 0.0}
    if valid.any():
        vmin = float(np.nanpercentile(depth_arr[valid], 5))
        vmax = float(np.nanpercentile(depth_arr[valid], 95))
        if vmax <= vmin:
            vmax = vmin + 1e-6
        stats = {"min_m": vmin, "max_m": vmax}
        scaled = np.clip((depth_arr - vmin) / (vmax - vmin), 0.0, 1.0)
        colors = plt.get_cmap("cividis")(scaled)
        alpha = (0.35 + 0.55 * scaled).astype(np.float32)
        rgba[valid, 0] = (colors[valid, 0] * 255).astype(np.uint8)
        rgba[valid, 1] = (colors[valid, 1] * 255).astype(np.uint8)
        rgba[valid, 2] = (colors[valid, 2] * 255).astype(np.uint8)
        rgba[valid, 3] = (alpha[valid] * 255).astype(np.uint8)
    plt.imsave(path, rgba)
    return stats


def _save_flow_overlay_png(
    path: Path,
    flow_geo: Dict[str, Any],
    transform,
    rows: int,
    cols: int,
    dem_arr: np.ndarray,
    depth_arr: np.ndarray,
) -> None:
    dpi = 100
    fig = plt.figure(figsize=(cols / dpi, rows / dpi), dpi=dpi)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, cols)
    ax.set_ylim(rows, 0)
    ax.axis("off")

    for feature in flow_geo.get("features", []):
        geom = feature.get("geometry", {})
        if geom.get("type") != "LineString":
            continue
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        xs: List[float] = []
        ys: List[float] = []
        for pt in coords:
            if len(pt) < 2:
                continue
            rr, cc = raster_rowcol(transform, float(pt[0]), float(pt[1]))
            xs.append(float(cc))
            ys.append(float(rr))
        if len(xs) >= 2:
            ax.plot(xs, ys, color="#00d1ff", linewidth=1.4, alpha=0.95)

    valid_mask = np.isfinite(depth_arr)
    if rows >= 3 and cols >= 3:
        stride = max(4, int(np.ceil(max(rows, cols) / 28)))
        dz_dr, dz_dc = np.gradient(dem_arr)
        for r in range(1, rows - 1, stride):
            for c in range(1, cols - 1, stride):
                if not np.isfinite(dem_arr[r, c]) or not bool(valid_mask[r, c]):
                    continue
                dr = -float(dz_dr[r, c])
                dc = -float(dz_dc[r, c])
                mag = float(np.hypot(dr, dc))
                if mag < 1e-9:
                    continue
                dr /= mag
                dc /= mag
                scale = stride * 0.75
                x0 = float(c)
                y0 = float(r)
                x1 = x0 + dc * scale
                y1 = y0 + dr * scale
                ax.annotate(
                    "",
                    xy=(x1, y1),
                    xytext=(x0, y0),
                    arrowprops={"arrowstyle": "-|>", "color": "#22d3ee", "lw": 0.9, "alpha": 0.75, "mutation_scale": 8},
                )

    fig.savefig(path, transparent=True, dpi=dpi, bbox_inches=None, pad_inches=0)
    plt.close(fig)


def _terrain_background(dem_arr: np.ndarray) -> np.ndarray:
    valid = np.isfinite(dem_arr)
    bg = np.zeros_like(dem_arr, dtype=np.float32)
    if valid.any():
        vmin = float(np.nanpercentile(dem_arr[valid], 2))
        vmax = float(np.nanpercentile(dem_arr[valid], 98))
        if vmax <= vmin:
            vmax = vmin + 1e-6
        bg[valid] = np.clip((dem_arr[valid] - vmin) / (vmax - vmin), 0.0, 1.0)
    return bg


def _pixel_to_world(transform, r: float, c: float) -> tuple[float, float]:
    x = float(transform.c + (c + 0.5) * transform.a + (r + 0.5) * transform.b)
    y = float(transform.f + (c + 0.5) * transform.d + (r + 0.5) * transform.e)
    return x, y


def _clip_lat_for_webmerc(lat: float) -> float:
    return max(-85.0511, min(85.0511, lat))


def _lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = _clip_lat_for_webmerc(lat)
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y


def _select_otm_zoom(west: float, south: float, east: float, north: float, rows: int, cols: int) -> int:
    lon_span = max(1e-6, abs(east - west))
    zoom_x = math.log2((max(cols, 1) * 360.0) / (lon_span * 256.0))
    lat_top = _clip_lat_for_webmerc(north)
    lat_bot = _clip_lat_for_webmerc(south)
    y_top = math.log(math.tan(math.pi / 4.0 + math.radians(lat_top) / 2.0))
    y_bot = math.log(math.tan(math.pi / 4.0 + math.radians(lat_bot) / 2.0))
    merc_span = max(1e-9, abs(y_top - y_bot))
    zoom_y = math.log2((max(rows, 1) * 2.0 * math.pi) / (merc_span * 256.0))
    z = int(max(6, min(13, math.floor(min(zoom_x, zoom_y)))))
    return z


def _opentopo_underlay(transform, crs, rows: int, cols: int) -> np.ndarray | None:
    try:
        west, south, east, north = array_bounds(rows, cols, transform)
        west, south, east, north = transform_bounds(crs, "EPSG:4326", west, south, east, north, densify_pts=21)
        west = float(max(-180.0, min(180.0, west)))
        east = float(max(-180.0, min(180.0, east)))
        south = float(_clip_lat_for_webmerc(south))
        north = float(_clip_lat_for_webmerc(north))
        if not (east > west and north > south):
            return None

        z = _select_otm_zoom(west, south, east, north, rows, cols)
        x0, y1 = _lonlat_to_tile(west, south, z)
        x1, y0 = _lonlat_to_tile(east, north, z)
        n = 2**z
        x0 = max(0, min(n - 1, x0))
        x1 = max(0, min(n - 1, x1))
        y0 = max(0, min(n - 1, y0))
        y1 = max(0, min(n - 1, y1))
        if x1 < x0:
            x0, x1 = x1, x0
        if y1 < y0:
            y0, y1 = y1, y0

        while z > 6 and (x1 - x0 + 1) * (y1 - y0 + 1) > 64:
            z -= 1
            x0, y1 = _lonlat_to_tile(west, south, z)
            x1, y0 = _lonlat_to_tile(east, north, z)
            n = 2**z
            x0 = max(0, min(n - 1, x0))
            x1 = max(0, min(n - 1, x1))
            y0 = max(0, min(n - 1, y0))
            y1 = max(0, min(n - 1, y1))
            if x1 < x0:
                x0, x1 = x1, x0
            if y1 < y0:
                y0, y1 = y1, y0

        width_tiles = x1 - x0 + 1
        height_tiles = y1 - y0 + 1
        if width_tiles <= 0 or height_tiles <= 0:
            return None

        mosaic = np.full((height_tiles * 256, width_tiles * 256, 3), 235, dtype=np.uint8)
        fetched = 0
        for ty in range(y0, y1 + 1):
            for tx in range(x0, x1 + 1):
                url = f"https://tile.opentopomap.org/{z}/{tx}/{ty}.png"
                try:
                    resp = requests.get(url, timeout=2.5)
                    if resp.status_code != 200:
                        continue
                    tile = np.asarray(Image.open(io.BytesIO(resp.content)).convert("RGB"), dtype=np.uint8)
                    rr = (ty - y0) * 256
                    cc = (tx - x0) * 256
                    mosaic[rr : rr + 256, cc : cc + 256] = tile
                    fetched += 1
                except Exception:
                    continue
        if fetched == 0:
            return None

        tile_span_m = (2.0 * WEB_MERCATOR_HALF_WORLD_M) / (2**z)
        src_transform = Affine(
            tile_span_m / 256.0,
            0.0,
            -WEB_MERCATOR_HALF_WORLD_M + x0 * tile_span_m,
            0.0,
            -tile_span_m / 256.0,
            WEB_MERCATOR_HALF_WORLD_M - y0 * tile_span_m,
        )

        dst = np.zeros((3, rows, cols), dtype=np.uint8)
        for band in range(3):
            reproject(
                source=mosaic[:, :, band],
                destination=dst[band],
                src_transform=src_transform,
                src_crs="EPSG:3857",
                dst_transform=transform,
                dst_crs=crs,
                resampling=Resampling.bilinear,
            )
        return np.transpose(dst, (1, 2, 0)).astype(np.float32) / 255.0
    except Exception:
        return None


def _outline_world_traces(outline_geojson: Dict[str, Any] | None, raster_crs: Any) -> List[Dict[str, List[float]]]:
    if not outline_geojson:
        return []
    features = outline_geojson.get("features", [])
    if not features:
        return []
    dst = str(raster_crs) if raster_crs else ""
    transformer = Transformer.from_crs("EPSG:4326", dst, always_xy=True) if dst and dst != "EPSG:4326" else None
    traces: List[Dict[str, List[float]]] = []
    for feature in features:
        geom = feature.get("geometry", {})
        gtype = geom.get("type")
        coords = geom.get("coordinates", [])
        rings: List[List[List[float]]] = []
        if gtype == "Polygon":
            rings = coords
        elif gtype == "MultiPolygon":
            for poly in coords:
                rings.extend(poly)
        for ring in rings:
            xs: List[float] = []
            ys: List[float] = []
            for pt in ring:
                if len(pt) < 2:
                    continue
                x = float(pt[0])
                y = float(pt[1])
                if transformer is not None:
                    x, y = transformer.transform(x, y)
                if not (np.isfinite(x) and np.isfinite(y)):
                    continue
                xs.append(x)
                ys.append(y)
            if len(xs) >= 3:
                traces.append({"x": xs, "y": ys})
    return traces


def _apply_lonlat_axes(ax, raster_crs: Any) -> None:
    if not raster_crs:
        return
    src = str(raster_crs)
    if not src:
        return
    if src == "EPSG:4326":
        ax.set_xlabel("Longitude (deg)")
        ax.set_ylabel("Latitude (deg)")
        return
    transformer = Transformer.from_crs(src, "EPSG:4326", always_xy=True)

    def _fmt_x(value, _pos):
        lon, _ = transformer.transform(float(value), float(ax.get_ylim()[0]))
        return f"{lon:.4f}"

    def _fmt_y(value, _pos):
        _, lat = transformer.transform(float(ax.get_xlim()[0]), float(value))
        return f"{lat:.4f}"

    ax.xaxis.set_major_formatter(FuncFormatter(_fmt_x))
    ax.yaxis.set_major_formatter(FuncFormatter(_fmt_y))
    ax.set_xlabel("Longitude (deg)")
    ax.set_ylabel("Latitude (deg)")


def _save_depth_topo_composite_png(
    path: Path,
    dem_arr: np.ndarray,
    depth_arr: np.ndarray,
    transform,
    outline_geojson: Dict[str, Any] | None,
    raster_crs: Any,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 7), dpi=120)
    extent = _extent_from_transform(transform, dem_arr.shape)
    underlay = _opentopo_underlay(transform, raster_crs, dem_arr.shape[0], dem_arr.shape[1])
    if underlay is not None:
        ax.imshow(underlay, origin="upper", extent=extent)
    else:
        terrain = _terrain_background(dem_arr)
        ax.imshow(terrain, cmap="terrain", origin="upper", extent=extent)

    valid = np.isfinite(depth_arr)
    if valid.any():
        vmin = float(np.nanpercentile(depth_arr[valid], 5))
        vmax = float(np.nanpercentile(depth_arr[valid], 95))
        if vmax <= vmin:
            vmax = vmin + 1e-6
        depth_img = ax.imshow(depth_arr, cmap="cividis", alpha=0.6, origin="upper", extent=extent, vmin=vmin, vmax=vmax)
        cbar = fig.colorbar(depth_img, ax=ax, fraction=0.046, pad=0.04)
        cbar.set_label("Depth (m)")

    ax.set_title("Depth Overlay on Topography")
    _apply_lonlat_axes(ax, raster_crs)
    for ring in _outline_world_traces(outline_geojson, raster_crs):
        if len(ring["x"]) >= 3:
            ax.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.9, alpha=0.95)
    ax.legend(
        handles=[
            Line2D([0], [0], color="#6b8e23", lw=4, label="Topography underlay"),
            Line2D([0], [0], color="#95a832", lw=4, label="Depth overlay"),
            Line2D([0], [0], color="#f8f36a", lw=2.0, label="Glacier outline"),
        ],
        loc="upper right",
        framealpha=0.9,
    )
    ax.text(
        0.01,
        0.01,
        "Base map: OpenTopoMap / OSM contributors",
        transform=ax.transAxes,
        fontsize=8,
        color="#f8fafc",
        bbox={"facecolor": "#0f172a", "alpha": 0.55, "pad": 3, "edgecolor": "none"},
    )
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _save_flow_topo_composite_png(
    path: Path,
    flow_geo: Dict[str, Any],
    transform,
    rows: int,
    cols: int,
    dem_arr: np.ndarray,
    depth_arr: np.ndarray,
    outline_geojson: Dict[str, Any] | None,
    raster_crs: Any,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 7), dpi=120)
    extent = _extent_from_transform(transform, dem_arr.shape)
    underlay = _opentopo_underlay(transform, raster_crs, dem_arr.shape[0], dem_arr.shape[1])
    if underlay is not None:
        ax.imshow(underlay, origin="upper", extent=extent)
    else:
        terrain = _terrain_background(dem_arr)
        ax.imshow(terrain, cmap="terrain", origin="upper", extent=extent)

    for feature in flow_geo.get("features", []):
        geom = feature.get("geometry", {})
        if geom.get("type") != "LineString":
            continue
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        xs: List[float] = []
        ys: List[float] = []
        for pt in coords:
            if len(pt) < 2:
                continue
            xs.append(float(pt[0]))
            ys.append(float(pt[1]))
        if len(xs) >= 2:
            ax.plot(xs, ys, color="#00d1ff", linewidth=1.3, alpha=0.95)

    valid_mask = np.isfinite(depth_arr)
    if rows >= 3 and cols >= 3:
        stride = max(4, int(np.ceil(max(rows, cols) / 28)))
        dz_dr, dz_dc = np.gradient(dem_arr)
        for r in range(1, rows - 1, stride):
            for c in range(1, cols - 1, stride):
                if not np.isfinite(dem_arr[r, c]) or not bool(valid_mask[r, c]):
                    continue
                dr = -float(dz_dr[r, c])
                dc = -float(dz_dc[r, c])
                mag = float(np.hypot(dr, dc))
                if mag < 1e-9:
                    continue
                dr /= mag
                dc /= mag
                scale = stride * 0.75
                x0 = float(c)
                y0 = float(r)
                x1 = x0 + dc * scale
                y1 = y0 + dr * scale
                wx0, wy0 = _pixel_to_world(transform, y0, x0)
                wx1, wy1 = _pixel_to_world(transform, y1, x1)
                ax.annotate(
                    "",
                    xy=(wx1, wy1),
                    xytext=(wx0, wy0),
                    arrowprops={"arrowstyle": "-|>", "color": "#22d3ee", "lw": 0.9, "alpha": 0.75, "mutation_scale": 8},
                )

    ax.set_title("Flow Overlay on Topography")
    _apply_lonlat_axes(ax, raster_crs)
    for ring in _outline_world_traces(outline_geojson, raster_crs):
        if len(ring["x"]) >= 3:
            ax.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.9, alpha=0.95)
    ax.legend(
        handles=[
            Line2D([0], [0], color="#6b8e23", lw=4, label="Topography underlay"),
            Line2D([0], [0], color="#00d1ff", lw=1.6, label="Flowlines"),
            Line2D([0], [0], color="#22d3ee", lw=1.3, label="Flow vectors (arrows)"),
            Line2D([0], [0], color="#f8f36a", lw=2.0, label="Glacier outline"),
        ],
        loc="upper right",
        framealpha=0.9,
    )
    ax.text(
        0.01,
        0.01,
        "Base map: OpenTopoMap / OSM contributors",
        transform=ax.transAxes,
        fontsize=8,
        color="#f8fafc",
        bbox={"facecolor": "#0f172a", "alpha": 0.55, "pad": 3, "edgecolor": "none"},
    )
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _save_summary_2x2_png(
    path: Path,
    dem_arr: np.ndarray,
    avg_slope_arr: np.ndarray | None,
    depth_arr: np.ndarray,
    flow_geo: Dict[str, Any],
    transform,
    rows: int,
    cols: int,
    outline_geojson: Dict[str, Any] | None,
    raster_crs: Any,
    grid_size_m: float,
    slope_units: str,
) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(13, 11), dpi=120)
    extent = _extent_from_transform(transform, dem_arr.shape)
    underlay = _opentopo_underlay(transform, raster_crs, dem_arr.shape[0], dem_arr.shape[1])

    ax_elev = axes[0, 0]
    elev_img = ax_elev.imshow(dem_arr, cmap="terrain", origin="upper", extent=extent)
    fig.colorbar(elev_img, ax=ax_elev, fraction=0.046, pad=0.04, label="Elevation (m)")
    for ring in _outline_world_traces(outline_geojson, raster_crs):
        ax_elev.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.8, alpha=0.95)
    ax_elev.set_title("Elevation")
    _apply_lonlat_axes(ax_elev, raster_crs)

    ax_slope = axes[0, 1]
    slope_field = avg_slope_arr if avg_slope_arr is not None else np.full_like(dem_arr, np.nan, dtype=np.float32)
    slope_img = ax_slope.imshow(slope_field, cmap="inferno", origin="upper", extent=extent)
    fig.colorbar(slope_img, ax=ax_slope, fraction=0.046, pad=0.04, label=f"Slope ({slope_units})")
    for ring in _outline_world_traces(outline_geojson, raster_crs):
        ax_slope.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.8, alpha=0.95)
    ax_slope.set_title("Average Slope Angle")
    _apply_lonlat_axes(ax_slope, raster_crs)

    ax_flow = axes[1, 0]
    if underlay is not None:
        ax_flow.imshow(underlay, origin="upper", extent=extent)
    else:
        ax_flow.imshow(_terrain_background(dem_arr), cmap="terrain", origin="upper", extent=extent)
    for feature in flow_geo.get("features", []):
        geom = feature.get("geometry", {})
        if geom.get("type") != "LineString":
            continue
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        xs = [float(pt[0]) for pt in coords if len(pt) >= 2]
        ys = [float(pt[1]) for pt in coords if len(pt) >= 2]
        if len(xs) >= 2:
            ax_flow.plot(xs, ys, color="#00d1ff", linewidth=1.2, alpha=0.95)
    valid_mask = np.isfinite(depth_arr)
    if rows >= 3 and cols >= 3:
        stride = max(4, int(np.ceil(max(rows, cols) / 28)))
        dz_dr, dz_dc = np.gradient(dem_arr)
        for r in range(1, rows - 1, stride):
            for c in range(1, cols - 1, stride):
                if not np.isfinite(dem_arr[r, c]) or not bool(valid_mask[r, c]):
                    continue
                dr = -float(dz_dr[r, c])
                dc = -float(dz_dc[r, c])
                mag = float(np.hypot(dr, dc))
                if mag < 1e-9:
                    continue
                dr /= mag
                dc /= mag
                scale = stride * 0.75
                x0 = float(c)
                y0 = float(r)
                x1 = x0 + dc * scale
                y1 = y0 + dr * scale
                wx0, wy0 = _pixel_to_world(transform, y0, x0)
                wx1, wy1 = _pixel_to_world(transform, y1, x1)
                ax_flow.annotate(
                    "",
                    xy=(wx1, wy1),
                    xytext=(wx0, wy0),
                    arrowprops={"arrowstyle": "-|>", "color": "#22d3ee", "lw": 0.8, "alpha": 0.7, "mutation_scale": 8},
                )
    for ring in _outline_world_traces(outline_geojson, raster_crs):
        ax_flow.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.8, alpha=0.95)
    ax_flow.set_title("Flow Overlay on Topo")
    _apply_lonlat_axes(ax_flow, raster_crs)

    ax_depth = axes[1, 1]
    if underlay is not None:
        ax_depth.imshow(underlay, origin="upper", extent=extent)
    else:
        ax_depth.imshow(_terrain_background(dem_arr), cmap="terrain", origin="upper", extent=extent)
    valid_depth = np.isfinite(depth_arr)
    if valid_depth.any():
        dmin = float(np.nanpercentile(depth_arr[valid_depth], 5))
        dmax = float(np.nanpercentile(depth_arr[valid_depth], 95))
        if dmax <= dmin:
            dmax = dmin + 1e-6
        depth_img = ax_depth.imshow(depth_arr, cmap="cividis", alpha=0.6, origin="upper", extent=extent, vmin=dmin, vmax=dmax)
        fig.colorbar(depth_img, ax=ax_depth, fraction=0.046, pad=0.04, label="Depth (m)")
    for ring in _outline_world_traces(outline_geojson, raster_crs):
        ax_depth.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.8, alpha=0.95)
    ax_depth.set_title("Depth Overlay on Topo")
    _apply_lonlat_axes(ax_depth, raster_crs)

    if avg_slope_arr is not None and np.isfinite(avg_slope_arr).any():
        min_s = float(np.nanmin(avg_slope_arr))
        max_s = float(np.nanmax(avg_slope_arr))
    else:
        min_s = float("nan")
        max_s = float("nan")
    if np.isfinite(depth_arr).any():
        min_d = float(np.nanmin(depth_arr))
        max_d = float(np.nanmax(depth_arr))
    else:
        min_d = float("nan")
        max_d = float("nan")
    footer = (
        f"Min slope: {min_s:.4f} {slope_units}   Max slope: {max_s:.4f} {slope_units}   "
        f"Grid size: {grid_size_m:.1f} m   Min depth: {min_d:.2f} m   Max depth: {max_d:.2f} m"
    )
    fig.subplots_adjust(bottom=0.1, hspace=0.22, wspace=0.12)
    fig.text(0.5, 0.02, footer, ha="center", va="bottom", fontsize=10)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _topomap_payload(job_id: str, raster_crs: str) -> Dict[str, Any]:
    job_dir = OUTPUT_ROOT / job_id
    dem_path = job_dir / "dem.tif"
    if not dem_path.exists():
        return {}
    with rasterio.open(dem_path) as src:
        west, south, east, north = array_bounds(src.height, src.width, src.transform)
        if src.crs:
            west, south, east, north = transform_bounds(src.crs, "EPSG:4326", west, south, east, north, densify_pts=21)
    out: Dict[str, Any] = {
        "bounds_wgs84": [float(south), float(west), float(north), float(east)],
        "raster_crs": raster_crs,
    }
    depth_overlay_path = job_dir / "depth_opentopo_overlay.png"
    flow_overlay_path = job_dir / "flow_opentopo_overlay.png"
    if depth_overlay_path.exists():
        out["depth_overlay_path"] = str(depth_overlay_path)
    if flow_overlay_path.exists():
        out["flow_overlay_path"] = str(flow_overlay_path)
    state = _read_state(job_dir)
    depth_legend = state.get("depth_legend")
    if isinstance(depth_legend, dict):
        out["depth_legend"] = depth_legend
    return out


def _mask_from_outline_if_available(job_dir: Path, raster_shape, transform, raster_crs) -> np.ndarray:
    outline_path = job_dir / "outline.geojson"
    if not outline_path.exists():
        return np.ones(raster_shape, dtype=bool)

    payload = json.loads(outline_path.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    if not features:
        return np.ones(raster_shape, dtype=bool)

    transformer = Transformer.from_crs("EPSG:4326", raster_crs, always_xy=True) if raster_crs else None
    projected_geoms = []
    for feature in features:
        geom = feature.get("geometry")
        if not geom:
            continue
        shp = shape(geom)
        if transformer is not None:
            shp = shapely_transform(transformer.transform, shp)
        if not shp.is_empty:
            projected_geoms.append(mapping(shp))

    if not projected_geoms:
        return np.ones(raster_shape, dtype=bool)

    return geometry_mask(projected_geoms, out_shape=raster_shape, transform=transform, invert=True)


def _vector_field_from_dem_sample(
    dem_plot: Dict[str, Any],
    valid_mask_z: List[List[float | None]] | None = None,
    max_vectors_per_axis: int = 28,
) -> Dict[str, Any]:
    arr = np.asarray(dem_plot["array"], dtype=np.float32)
    rows, cols = arr.shape
    if rows < 3 or cols < 3:
        return {"x": [], "y": []}

    vec_stride = max(1, int(np.ceil(max(rows, cols) / max_vectors_per_axis)))
    dz_drow, dz_dcol = np.gradient(arr)

    jac = dem_plot.get("jacobian") or {}
    jx_col = float(jac.get("jx_col", 1.0))
    jy_col = float(jac.get("jy_col", 0.0))
    jx_row = float(jac.get("jx_row", 0.0))
    jy_row = float(jac.get("jy_row", -1.0))

    j = np.array([[jx_col, jx_row], [jy_col, jy_row]], dtype=np.float64)
    det = float(np.linalg.det(j))
    if abs(det) < 1e-12:
        return {"x": [], "y": []}
    inv_t = np.linalg.inv(j).T

    line_x: List[float | None] = []
    line_y: List[float | None] = []
    valid_mask = None
    if valid_mask_z is not None:
        valid_mask = np.array([[v is not None for v in row] for row in valid_mask_z], dtype=bool)
        if valid_mask.shape != arr.shape:
            valid_mask = None

    for r in range(1, rows - 1, vec_stride):
        for c in range(1, cols - 1, vec_stride):
            if not np.isfinite(arr[r, c]):
                continue
            if valid_mask is not None and not bool(valid_mask[r, c]):
                continue
            g_idx = np.array([float(dz_dcol[r, c]), float(dz_drow[r, c])], dtype=np.float64)
            g_world = inv_t @ g_idx
            if not (np.isfinite(g_world[0]) and np.isfinite(g_world[1])):
                continue

            u = -float(g_world[0])
            v = -float(g_world[1])
            mag = float(np.hypot(u, v))
            if mag < 1e-10:
                continue

            u /= mag
            v /= mag
            x0 = float(dem_plot["x"][c])
            y0 = float(dem_plot["y"][r])
            pixel_x = float(dem_plot.get("pixel_x", 1.0))
            pixel_y = float(dem_plot.get("pixel_y", 1.0))
            scale = vec_stride * max(abs(pixel_x), abs(pixel_y)) * 0.75
            x1 = x0 + u * scale
            y1 = y0 + v * scale
            if not (np.isfinite(x0) and np.isfinite(y0) and np.isfinite(x1) and np.isfinite(y1)):
                continue
            line_x.extend([x0, x1, None])
            line_y.extend([y0, y1, None])
    return {"x": line_x, "y": line_y}


app = create_app()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glacier analysis web app")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)
