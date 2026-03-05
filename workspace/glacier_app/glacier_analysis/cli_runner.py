"""Pipeline runner used by CLI and optionally by web endpoints."""

from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any, Callable, Dict

import numpy as np
from pyproj import Transformer
from rasterio.features import geometry_mask
from rasterio.transform import array_bounds
from shapely.geometry import mapping, shape
from shapely.ops import transform as shapely_transform

from .config import PipelineConfig, merge_config, save_config_yaml
from .elevation import get_dem, local_utm_crs
from .geometry import get_outline_geojson
from .processing import average_slope, depth_from_slope, flowlines_geojson, slope_degrees, slope_radians
from .report import (
    build_zip_bundle,
    write_flow_preview_png,
    write_geotiff,
    write_geojson,
    write_json,
    write_overlay_png,
    write_raster_preview_png,
)

def _glacier_mask_from_outline(outline_geojson: Dict[str, Any], dem_shape, transform, dem_crs) -> np.ndarray:
    features = outline_geojson.get("features", [])
    if not features:
        return np.ones(dem_shape, dtype=bool)

    transformer = Transformer.from_crs("EPSG:4326", dem_crs, always_xy=True)
    projected_geoms = []
    for feature in features:
        geom = feature.get("geometry")
        if not geom:
            continue
        shp = shape(geom)
        shp_projected = shapely_transform(transformer.transform, shp)
        if not shp_projected.is_empty:
            projected_geoms.append(mapping(shp_projected))

    if not projected_geoms:
        return np.ones(dem_shape, dtype=bool)

    return geometry_mask(projected_geoms, out_shape=dem_shape, transform=transform, invert=True)


def _extent_from_transform(transform, shape: tuple[int, int]) -> tuple[float, float, float, float]:
    h, w = shape
    west, south, east, north = array_bounds(h, w, transform)
    return west, east, south, north


def run_pipeline(
    config: PipelineConfig,
    outline_geojson_override: Dict[str, Any] | None = None,
    dem_progress_callback: Callable[[int, int, str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> Dict[str, Any]:
    """Execute the full analysis pipeline and return run metadata."""
    out_dir = Path(config.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    save_config_yaml(config, str(out_dir / "config.yaml"))
    if should_cancel is not None and should_cancel():
        raise RuntimeError("Run cancelled by user")

    outline_geojson = outline_geojson_override or get_outline_geojson(
        bbox=config.bbox,
        overpass_url=config.overpass_url,
        timeout_s=config.request_timeout_s,
        allow_fallback=config.allow_outline_fallback,
    )
    write_geojson(str(out_dir / "outline.geojson"), outline_geojson)

    dem_info = get_dem(
        bbox=config.bbox,
        resolution_m=config.resolution_m,
        dst_crs=local_utm_crs(config.bbox),
        dataset=config.dataset,
        api_key=config.opentopo_api_key,
        opentopo_url=config.opentopo_url,
        timeout_s=config.request_timeout_s,
        work_dir=str(out_dir),
        allow_fallback=config.allow_synthetic_dem_fallback,
        progress_callback=dem_progress_callback,
        should_cancel=should_cancel,
    )
    if should_cancel is not None and should_cancel():
        raise RuntimeError("Run cancelled by user")
    dem = np.asarray(dem_info["array"], dtype=np.float32)
    transform = dem_info["transform"]
    crs = dem_info["crs"]

    dem_tif = out_dir / "dem.tif"
    write_geotiff(str(dem_tif), dem, transform, crs)
    extent = _extent_from_transform(transform, dem.shape)
    write_raster_preview_png(
        str(out_dir / "dem_preview.png"),
        dem,
        title="DEM",
        cmap="terrain",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )
    write_raster_preview_png(
        str(out_dir / "elevation_plot.png"),
        dem,
        title="Elevation (m)",
        cmap="terrain",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    glacier_mask = _glacier_mask_from_outline(outline_geojson, dem.shape, transform, crs)
    px = abs(float(transform.a))
    py = abs(float(transform.e))
    theta_rad = slope_radians(dem, pixel_size_x=px, pixel_size_y=py)
    if should_cancel is not None and should_cancel():
        raise RuntimeError("Run cancelled by user")
    theta_rad = np.where(glacier_mask, theta_rad, np.nan).astype(np.float32)
    slope_out = theta_rad if config.slope_units == "radians" else slope_degrees(theta_rad)
    write_geotiff(str(out_dir / "slope_angle.tif"), slope_out, transform, crs)
    write_raster_preview_png(
        str(out_dir / "slope_angle_preview.png"),
        slope_out,
        title=f"Slope Angle ({config.slope_units})",
        cmap="magma",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    avg_theta = average_slope(theta_rad, pixel_size_m=(px + py) / 2.0, grid_size_m=config.grid_size_m)
    avg_theta = np.where(glacier_mask, avg_theta, np.nan).astype(np.float32)
    avg_slope_out = avg_theta if config.slope_units == "radians" else slope_degrees(avg_theta)
    write_geotiff(str(out_dir / "avg_slope.tif"), avg_slope_out, transform, crs)
    write_raster_preview_png(
        str(out_dir / "avg_slope_preview.png"),
        avg_slope_out,
        title=f"Average Slope ({config.slope_units})",
        cmap="inferno",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    flow_geo = flowlines_geojson(
        dem,
        transform=transform,
        seed_spacing_px=config.flow_seed_spacing_px,
        step_px=config.flow_step_px,
        max_steps=config.flow_max_steps,
        glacier_mask=glacier_mask,
    )
    write_geojson(str(out_dir / "flowlines.geojson"), flow_geo)
    if should_cancel is not None and should_cancel():
        raise RuntimeError("Run cancelled by user")

    depth = depth_from_slope(
        avg_theta,
        depth_scale_m=config.tau_f,
        epsilon=config.epsilon_slope_rad,
        depth_min_m=config.depth_min_m,
        depth_max_m=config.depth_max_m,
    )
    depth = np.where(glacier_mask, depth, np.nan).astype(np.float32)
    write_geotiff(str(out_dir / "depth.tif"), depth, transform, crs)
    write_raster_preview_png(
        str(out_dir / "depth_preview.png"),
        depth,
        title="Depth (m)",
        cmap="cividis",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    write_flow_preview_png(
        str(out_dir / "flowlines_preview.png"),
        dem,
        flow_geo,
        title="Flowlines + Vectors",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )
    write_overlay_png(
        str(out_dir / "final_overlay.png"),
        depth,
        flow_geo,
        title="Final Overlay: Depth + Flow",
        extent=extent,
        outline_geojson=outline_geojson,
        raster_crs=crs,
    )

    summary = {
        "job_id": config.job_id,
        "output_dir": str(out_dir),
        "outline_source": outline_geojson.get("features", [{}])[0].get("properties", {}).get("source"),
        "dem_source": dem_info.get("source"),
        "dem_fallback": bool(dem_info.get("fallback")),
        "slope_units": config.slope_units,
        "mean_slope_radians": float(np.nanmean(theta_rad)),
        "mean_slope_degrees": float(np.nanmean(slope_degrees(theta_rad))),
        "mean_depth_m": float(np.nanmean(depth)),
        "flowline_count": len(flow_geo.get("features", [])),
        "tau_f": config.tau_f,
        "grid_size_m": config.grid_size_m,
        "resolution_m": config.resolution_m,
    }

    write_json(str(out_dir / "report.json"), summary)

    bundle_files = [
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
        "final_overlay.png",
        "report.json",
    ]
    bundle_path = build_zip_bundle(str(out_dir), f"report_{config.job_id}.zip", bundle_files)
    summary["bundle_path"] = bundle_path
    write_json(str(out_dir / "report.json"), summary)

    return summary


def run_from_inputs(yaml_path: str | None, cli_values: Dict[str, Any]) -> Dict[str, Any]:
    """Merge YAML + CLI options and execute pipeline."""
    from .config import load_yaml_config

    cfg = merge_config(load_yaml_config(yaml_path), cli_values)
    return run_pipeline(cfg)


def run_from_inputs_safe(yaml_path: str | None, cli_values: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper that returns structured errors for CLI exits."""
    try:
        return {"ok": True, "result": run_from_inputs(yaml_path, cli_values)}
    except Exception as exc:  # pragma: no cover - defensive CLI path
        return {"ok": False, "error": str(exc), "traceback": traceback.format_exc()}
