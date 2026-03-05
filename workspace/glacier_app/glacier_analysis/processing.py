"""Core raster processing algorithms for slope, flowlines, and depth."""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
from rasterio.transform import xy
from scipy.ndimage import uniform_filter


def slope_radians(dem: np.ndarray, pixel_size_x: float, pixel_size_y: float) -> np.ndarray:
    """Compute per-pixel slope angle in radians from DEM."""
    dz_dy, dz_dx = np.gradient(dem, pixel_size_y, pixel_size_x)
    grad = np.hypot(dz_dx, dz_dy)
    return np.arctan(grad).astype(np.float32)


def slope_degrees(theta_rad: np.ndarray) -> np.ndarray:
    """Convert slope radians to degrees."""
    return np.degrees(theta_rad).astype(np.float32)


def average_slope(theta_rad: np.ndarray, pixel_size_m: float, grid_size_m: float) -> np.ndarray:
    """Average slope angles over an approximate meter-sized grid.

    Ignores NaN pixels and preserves NaN where no valid support exists.
    """
    window = max(1, int(round(grid_size_m / max(pixel_size_m, 1e-6))))
    arr = np.asarray(theta_rad, dtype=np.float32)
    valid = np.isfinite(arr).astype(np.float32)
    filled = np.where(np.isfinite(arr), arr, 0.0).astype(np.float32)

    value_sum = uniform_filter(filled, size=window, mode="nearest") * (window * window)
    valid_count = uniform_filter(valid, size=window, mode="nearest") * (window * window)
    out = np.full_like(arr, np.nan, dtype=np.float32)
    mask = valid_count > 0
    out[mask] = (value_sum[mask] / valid_count[mask]).astype(np.float32)
    return out


def _trace_flowline(
    dem: np.ndarray,
    start_r: float,
    start_c: float,
    step_px: float,
    max_steps: int,
    glacier_mask: np.ndarray | None = None,
) -> List[tuple[float, float]]:
    rows, cols = dem.shape
    r, c = float(start_r), float(start_c)
    points: List[tuple[float, float]] = []
    last_z = None

    for _ in range(max_steps):
        ir = int(round(r))
        ic = int(round(c))
        if ir < 1 or ic < 1 or ir >= rows - 1 or ic >= cols - 1:
            break
        if glacier_mask is not None and not bool(glacier_mask[ir, ic]):
            break

        zc = float(dem[ir, ic])
        if not np.isfinite(zc):
            break

        # Steepest descent in raster index space.
        z_up = float(dem[ir - 1, ic])
        z_dn = float(dem[ir + 1, ic])
        z_lt = float(dem[ir, ic - 1])
        z_rt = float(dem[ir, ic + 1])
        if not (np.isfinite(z_up) and np.isfinite(z_dn) and np.isfinite(z_lt) and np.isfinite(z_rt)):
            break

        dz_dr = (z_dn - z_up) / 2.0
        dz_dc = (z_rt - z_lt) / 2.0
        dr = -dz_dr
        dc = -dz_dc
        norm = float(np.hypot(dr, dc))
        if not np.isfinite(norm) or norm < 1e-8:
            break

        if last_z is not None and zc > last_z + 1e-4:
            break
        last_z = zc

        points.append((r, c))
        r += (dr / norm) * step_px
        c += (dc / norm) * step_px

    return points


def flowlines_geojson(
    dem: np.ndarray,
    transform,
    seed_spacing_px: int = 30,
    step_px: float = 1.0,
    max_steps: int = 250,
    glacier_mask: np.ndarray | None = None,
) -> Dict[str, Any]:
    """Build simple steepest-descent flowline polylines as GeoJSON."""
    rows, cols = dem.shape
    spacing = max(5, int(seed_spacing_px))
    features = []

    for r0 in range(spacing, rows - spacing, spacing):
        for c0 in range(spacing, cols - spacing, spacing):
            if glacier_mask is not None and not bool(glacier_mask[r0, c0]):
                continue
            pts_rc = _trace_flowline(
                dem,
                r0,
                c0,
                step_px=step_px,
                max_steps=max_steps,
                glacier_mask=glacier_mask,
            )
            if len(pts_rc) < 5:
                continue
            coords = []
            for r, c in pts_rc:
                x, y = xy(transform, r, c, offset="center")
                coords.append([float(x), float(y)])
            features.append(
                {
                    "type": "Feature",
                    "properties": {"seed_row": r0, "seed_col": c0},
                    "geometry": {"type": "LineString", "coordinates": coords},
                }
            )

    return {"type": "FeatureCollection", "features": features}


def depth_from_slope(
    theta_rad: np.ndarray,
    depth_scale_m: float,
    epsilon: float,
    depth_min_m: float,
    depth_max_m: float,
) -> np.ndarray:
    """Compute depth = depth_scale_m / max(sin(theta), epsilon) with clamps."""
    denom = np.maximum(np.sin(theta_rad), epsilon)
    depth = depth_scale_m / denom
    depth = np.clip(depth, depth_min_m, depth_max_m)
    depth[~np.isfinite(theta_rad)] = np.nan
    return depth.astype(np.float32)
