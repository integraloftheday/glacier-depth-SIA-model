"""Elevation acquisition and synthetic DEM fallback utilities."""

from __future__ import annotations

import math
import json
import time
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

import numpy as np
import rasterio
import requests
from pyproj import CRS, Transformer
from rasterio.enums import Resampling
from rasterio.transform import from_origin
from rasterio.warp import reproject

BBox = Tuple[float, float, float, float]


def local_utm_crs(bbox: BBox) -> CRS:
    """Choose a local UTM CRS based on bbox centroid."""
    minx, miny, maxx, maxy = bbox
    lon = (minx + maxx) / 2.0
    lat = (miny + maxy) / 2.0
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return CRS.from_epsg(epsg)


def _projected_bounds(bbox: BBox, dst_crs: CRS) -> Tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bbox
    tx = Transformer.from_crs("EPSG:4326", dst_crs, always_xy=True)
    xs, ys = tx.transform([minx, maxx, maxx, minx], [miny, miny, maxy, maxy])
    return min(xs), min(ys), max(xs), max(ys)


def generate_synthetic_dem(bbox: BBox, resolution_m: float, dst_crs: CRS) -> Tuple[np.ndarray, rasterio.Affine]:
    """Generate deterministic synthetic DEM over bbox in projected coordinates.

    Pattern: tilted plane plus a few gaussian bumps.
    """
    minx, miny, maxx, maxy = _projected_bounds(bbox, dst_crs)
    width = max(16, int(math.ceil((maxx - minx) / resolution_m)))
    height = max(16, int(math.ceil((maxy - miny) / resolution_m)))
    transform = from_origin(minx, maxy, resolution_m, resolution_m)

    cols = np.arange(width, dtype=np.float64)
    rows = np.arange(height, dtype=np.float64)
    xx, yy = np.meshgrid(cols, rows)
    xn = xx / max(width - 1, 1)
    yn = yy / max(height - 1, 1)

    seed = int(abs(sum(bbox) * 1_000_000)) % (2**32 - 1)
    rng = np.random.default_rng(seed)

    dem = 3200.0 + 700.0 * (0.6 * (1.0 - yn) + 0.4 * xn)
    for _ in range(3):
        cx = rng.uniform(0.2, 0.8)
        cy = rng.uniform(0.2, 0.8)
        sx = rng.uniform(0.07, 0.18)
        sy = rng.uniform(0.07, 0.18)
        amp = rng.uniform(120.0, 300.0)
        dem += amp * np.exp(-(((xn - cx) ** 2) / (2 * sx**2) + ((yn - cy) ** 2) / (2 * sy**2)))

    dem += 30.0 * np.sin(2.5 * np.pi * xn) * np.cos(2.0 * np.pi * yn)
    return dem.astype(np.float32), transform


def _fetch_opentopo_tiff(
    bbox: BBox,
    api_key: str,
    dataset: str,
    opentopo_url: str,
    timeout_s: int,
    download_path: Path,
    max_retries: int = 3,
) -> Path:
    minx, miny, maxx, maxy = bbox
    demtype = "SRTMGL1"
    if dataset.lower() in {"cop30", "copernicus"}:
        demtype = "COP30"
    params = {
        "demtype": demtype,
        "south": miny,
        "north": maxy,
        "west": minx,
        "east": maxx,
        "outputFormat": "GTiff",
        "API_Key": api_key,
    }
    last_error: Optional[Exception] = None
    attempts = max(1, int(max_retries))
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(opentopo_url, params=params, timeout=timeout_s)
            if response.status_code >= 400:
                body = response.text[:1000]
                raise RuntimeError(f"OpenTopography HTTP {response.status_code}: {body}")
            ctype = response.headers.get("content-type", "")
            if "json" in ctype.lower():
                payload = response.json()
                raise RuntimeError(f"OpenTopography error: {payload}")
            download_path.parent.mkdir(parents=True, exist_ok=True)
            download_path.write_bytes(response.content)
            if download_path.stat().st_size < 1024:
                raw = response.content[:2000]
                try:
                    text = raw.decode("utf-8", errors="replace").strip()
                except Exception:
                    text = ""
                parsed_json = None
                if text:
                    try:
                        parsed_json = json.loads(text)
                    except Exception:
                        parsed_json = None
                if parsed_json is not None:
                    raise RuntimeError(f"OpenTopography error (small response): {parsed_json}")
                if text:
                    raise RuntimeError(f"OpenTopography error (small response): {text}")
                raise RuntimeError("OpenTopography response too small to be valid GeoTIFF")
            return download_path
        except (requests.exceptions.RequestException, RuntimeError) as exc:
            last_error = exc
            is_retryable = True
            msg = str(exc).lower()
            if "http 400" in msg or "http 401" in msg or "http 403" in msg:
                is_retryable = False
            if attempt >= attempts or not is_retryable:
                break
            time.sleep(min(2.0 * attempt, 6.0))
    if last_error is not None:
        raise RuntimeError(str(last_error)) from last_error
    return download_path


def _subdivide_bbox_quadrants(bbox: BBox) -> list[BBox]:
    minx, miny, maxx, maxy = bbox
    mx = (minx + maxx) / 2.0
    my = (miny + maxy) / 2.0
    return [
        (minx, miny, mx, my),
        (mx, miny, maxx, my),
        (minx, my, mx, maxy),
        (mx, my, maxx, maxy),
    ]


def _reproject_dem_to_local(
    src_path: Path,
    bbox: BBox,
    dst_crs: CRS,
    resolution_m: float,
) -> Tuple[np.ndarray, rasterio.Affine]:
    with rasterio.open(src_path) as src:
        if src.crs is None:
            raise ValueError("Fetched DEM is missing CRS metadata")

        left, bottom, right, top = _projected_bounds(bbox, dst_crs)
        dst_transform = from_origin(left, top, resolution_m, resolution_m)
        width = max(16, int(math.ceil((right - left) / resolution_m)))
        height = max(16, int(math.ceil((top - bottom) / resolution_m)))
        dst = np.full((height, width), np.nan, dtype=np.float32)

        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            dst_nodata=np.nan,
            resampling=Resampling.bilinear,
        )
    return dst, dst_transform


def _destination_grid(
    bbox: BBox,
    dst_crs: CRS,
    resolution_m: float,
) -> Tuple[float, float, float, float, int, int, rasterio.Affine]:
    left, bottom, right, top = _projected_bounds(bbox, dst_crs)
    width = max(16, int(math.ceil((right - left) / resolution_m)))
    height = max(16, int(math.ceil((top - bottom) / resolution_m)))
    dst_transform = from_origin(left, top, resolution_m, resolution_m)
    return left, bottom, right, top, width, height, dst_transform


def _tile_bboxes_wgs84(bbox: BBox, nx: int, ny: int) -> list[BBox]:
    minx, miny, maxx, maxy = bbox
    tiles: list[BBox] = []
    for iy in range(ny):
        y0 = miny + (maxy - miny) * (iy / ny)
        y1 = miny + (maxy - miny) * ((iy + 1) / ny)
        for ix in range(nx):
            x0 = minx + (maxx - minx) * (ix / nx)
            x1 = minx + (maxx - minx) * ((ix + 1) / nx)
            if x1 > x0 and y1 > y0:
                tiles.append((x0, y0, x1, y1))
    return tiles


def _fetch_opentopo_tiled_dem(
    bbox: BBox,
    api_key: str,
    dataset: str,
    opentopo_url: str,
    timeout_s: int,
    work_dir: Path,
    dst_crs: CRS,
    resolution_m: float,
    max_tile_dim_px: int = 1800,
    max_tile_lon_span_deg: float = 0.08,
    max_tile_lat_span_deg: float = 0.08,
    max_split_depth: int = 3,
    max_tiles_total: int = 220,
    progress_callback: Callable[[int, int, str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> Tuple[np.ndarray, rasterio.Affine, int]:
    _, _, _, _, width, height, dst_transform = _destination_grid(bbox, dst_crs, resolution_m)
    total_cells = width * height
    # Guardrail to avoid pathological memory/runtime blowups.
    if total_cells > 60_000_000:
        raise RuntimeError(
            f"Requested DEM grid is too large ({width}x{height}={total_cells:,} cells); "
            "increase resolution or split glacier into smaller regions."
        )

    minx, miny, maxx, maxy = bbox
    lon_span = maxx - minx
    lat_span = maxy - miny
    nx = max(1, int(math.ceil(width / max_tile_dim_px)), int(math.ceil(lon_span / max_tile_lon_span_deg)))
    ny = max(1, int(math.ceil(height / max_tile_dim_px)), int(math.ceil(lat_span / max_tile_lat_span_deg)))
    tile_bboxes = _tile_bboxes_wgs84(bbox, nx=nx, ny=ny)
    if not tile_bboxes:
        raise RuntimeError("Tiling produced no valid sub-bboxes")

    merged = np.full((height, width), np.nan, dtype=np.float32)
    tile_root = work_dir / "dem_tiles"
    tile_root.mkdir(parents=True, exist_ok=True)

    queue: list[Tuple[BBox, int, str]] = [(tb, 0, f"{i:03d}") for i, tb in enumerate(tile_bboxes)]
    completed_tiles = 0
    failures: list[str] = []
    first_failure: str | None = None
    if progress_callback is not None:
        progress_callback(0, len(queue), "Starting DEM tile fetch")
    while queue:
        if should_cancel is not None and should_cancel():
            raise RuntimeError("DEM fetch cancelled by user")
        if len(queue) > max_tiles_total:
            detail = first_failure or (failures[0] if failures else "unknown tile failure")
            raise RuntimeError(
                f"Too many failing/split tiles ({len(queue)} queued). "
                f"This usually indicates DEM coverage/API constraints for this bbox. First failure: {detail}"
            )
        tile_bbox, split_depth, tile_label = queue.pop(0)
        tile_path = tile_root / f"tile_{tile_label}.tif"
        try:
            _fetch_opentopo_tiff(
                bbox=tile_bbox,
                api_key=api_key,
                dataset=dataset,
                opentopo_url=opentopo_url,
                timeout_s=timeout_s,
                download_path=tile_path,
            )
            with rasterio.open(tile_path) as src:
                if src.crs is None:
                    raise RuntimeError(f"Tile {tile_label} missing CRS metadata")
                reprojected_tile = np.full((height, width), np.nan, dtype=np.float32)
                reproject(
                    source=rasterio.band(src, 1),
                    destination=reprojected_tile,
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    dst_nodata=np.nan,
                    resampling=Resampling.bilinear,
                )
                mask = np.isfinite(reprojected_tile)
                merged[mask] = reprojected_tile[mask]
                completed_tiles += 1
                if progress_callback is not None:
                    total_known = completed_tiles + len(queue)
                    msg = f"Fetched DEM tile {completed_tiles}/{max(total_known, completed_tiles)}"
                    progress_callback(completed_tiles, max(total_known, completed_tiles), msg)
        except Exception as exc:
            msg = str(exc)
            if first_failure is None:
                first_failure = msg
            is_split_candidate = any(
                token in msg.lower()
                for token in (
                    "http 429",
                    "http 500",
                    "http 502",
                    "http 503",
                    "http 504",
                    "timed out",
                    "timeout",
                    "too small",
                    "json",
                    "gateway",
                )
            )
            if is_split_candidate and split_depth < max_split_depth:
                for child_idx, child_bbox in enumerate(_subdivide_bbox_quadrants(tile_bbox)):
                    queue.append((child_bbox, split_depth + 1, f"{tile_label}_{child_idx}"))
                if progress_callback is not None:
                    total_known = completed_tiles + len(queue)
                    msg = f"Splitting failed tile {tile_label}; queued {len(queue)} remaining"
                    progress_callback(completed_tiles, max(total_known, completed_tiles), msg)
                continue
            failures.append(f"tile={tile_label} depth={split_depth}: {msg}")
            if progress_callback is not None:
                total_known = completed_tiles + len(queue)
                progress_callback(
                    completed_tiles,
                    max(total_known, completed_tiles),
                    f"Tile failed ({tile_label}): {msg[:140]}",
                )

    if not np.isfinite(merged).any():
        detail = first_failure or ("; ".join(failures[:4]) if failures else "no successful tiles")
        raise RuntimeError(f"All tiles returned nodata after reprojection/mosaic ({detail})")
    if failures and completed_tiles == 0:
        detail = "; ".join(failures[:4])
        raise RuntimeError(f"DEM tiling failed with no successful tiles ({detail})")
    if progress_callback is not None:
        progress_callback(completed_tiles, completed_tiles, "DEM tile fetch complete")
    return merged, dst_transform, completed_tiles


def get_dem(
    bbox: BBox,
    resolution_m: float,
    dst_crs: Optional[CRS],
    dataset: str,
    api_key: Optional[str],
    opentopo_url: str,
    timeout_s: int,
    work_dir: str,
    allow_fallback: bool = True,
    progress_callback: Callable[[int, int, str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> Dict[str, object]:
    """Get DEM with online attempt first, then deterministic synthetic fallback."""
    dst = dst_crs or local_utm_crs(bbox)
    work = Path(work_dir)

    if not api_key and not allow_fallback:
        raise RuntimeError("OpenTopography API key required (set OPEN_TOPO_KEY or provide api_key)")

    if api_key:
        try:
            dem_arr, transform, tile_count = _fetch_opentopo_tiled_dem(
                bbox=bbox,
                api_key=api_key,
                dataset=dataset,
                opentopo_url=opentopo_url,
                timeout_s=timeout_s,
                dst_crs=dst,
                resolution_m=resolution_m,
                work_dir=work,
                progress_callback=progress_callback,
                should_cancel=should_cancel,
            )
            return {
                "array": dem_arr,
                "transform": transform,
                "crs": dst,
                "source": "opentopography",
                "fallback": False,
                "tile_count": tile_count,
            }
        except Exception as exc:
            if not allow_fallback:
                raise RuntimeError(f"OpenTopography DEM fetch failed: {exc}") from exc

    if not allow_fallback:
        raise RuntimeError("OpenTopography DEM fetch failed and synthetic fallback is disabled")
    dem_arr, transform = generate_synthetic_dem(bbox, resolution_m=resolution_m, dst_crs=dst)
    return {
        "array": dem_arr,
        "transform": transform,
        "crs": dst,
        "source": "synthetic_dem",
        "fallback": True,
    }
