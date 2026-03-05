"""Output writers for rasters, vectors, previews, and run bundles."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List

import matplotlib.pyplot as plt
import numpy as np
import rasterio
from matplotlib.ticker import FuncFormatter
from pyproj import Transformer
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from rasterio.transform import rowcol as raster_rowcol


def write_geotiff(path: str, array: np.ndarray, transform, crs, nodata: float = -9999.0) -> None:
    """Write a single-band float32 GeoTIFF."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    arr = array.astype(np.float32, copy=True)
    arr[~np.isfinite(arr)] = nodata
    with rasterio.open(
        out,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=nodata,
        compress="deflate",
    ) as dst:
        dst.write(arr, 1)


def write_geojson(path: str, payload: Dict[str, Any]) -> None:
    """Write GeoJSON dictionary to disk."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_json(path: str, payload: Dict[str, Any]) -> None:
    """Write JSON metadata to disk."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _outline_world_traces(outline_geojson: Dict[str, Any], raster_crs: Any) -> List[Dict[str, List[float]]]:
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


def _outline_pixel_traces(outline_geojson: Dict[str, Any], transform, raster_crs: Any) -> List[Dict[str, List[float]]]:
    world = _outline_world_traces(outline_geojson, raster_crs)
    traces: List[Dict[str, List[float]]] = []
    for ring in world:
        xs: List[float] = []
        ys: List[float] = []
        for x, y in zip(ring["x"], ring["y"]):
            rr, cc = raster_rowcol(transform, x, y)
            xs.append(float(cc))
            ys.append(float(rr))
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


def write_raster_preview_png(
    path: str,
    array: np.ndarray,
    title: str,
    cmap: str = "viridis",
    extent: tuple[float, float, float, float] | None = None,
    outline_geojson: Dict[str, Any] | None = None,
    transform=None,
    raster_crs: Any = None,
) -> None:
    """Render array preview image as PNG."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    arr = np.array(array, dtype=float)
    valid = np.isfinite(arr)
    if not valid.any():
        arr = np.zeros_like(arr)
    else:
        vmin = np.nanpercentile(arr[valid], 2)
        vmax = np.nanpercentile(arr[valid], 98)
        if vmax <= vmin:
            vmax = vmin + 1e-6
        arr = np.clip(arr, vmin, vmax)

    fig, ax = plt.subplots(figsize=(8, 6), dpi=120)
    imshow_kwargs = {"cmap": cmap, "origin": "upper"}
    if extent is not None:
        imshow_kwargs["extent"] = extent
    image = ax.imshow(arr, **imshow_kwargs)
    ax.set_title(title)
    if extent is None:
        ax.set_xlabel("X (pixel index)")
        ax.set_ylabel("Y (pixel index)")
    else:
        _apply_lonlat_axes(ax, raster_crs)

    drew_outline = False
    if outline_geojson:
        if extent is not None:
            for ring in _outline_world_traces(outline_geojson, raster_crs):
                ax.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.8, alpha=0.95)
                drew_outline = True
        elif transform is not None:
            for ring in _outline_pixel_traces(outline_geojson, transform, raster_crs):
                ax.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.8, alpha=0.95)
                drew_outline = True
    cbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Value")
    handles = [Patch(facecolor=plt.get_cmap(cmap)(0.7), edgecolor="black", label="Raster values")]
    if drew_outline:
        handles.append(Line2D([0], [0], color="#f8f36a", lw=2.0, label="Glacier outline"))
    ax.legend(handles=handles, loc="upper right", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)


def _draw_line_arrows(ax, xs: List[float], ys: List[float], color: str, base_step: int = 8) -> None:
    if len(xs) < 3:
        return
    step = max(base_step, int(np.ceil(len(xs) / 20)))
    for i in range(step, len(xs), step):
        x0 = float(xs[i - 1])
        y0 = float(ys[i - 1])
        x1 = float(xs[i])
        y1 = float(ys[i])
        if not (np.isfinite(x0) and np.isfinite(y0) and np.isfinite(x1) and np.isfinite(y1)):
            continue
        if abs(x1 - x0) < 1e-12 and abs(y1 - y0) < 1e-12:
            continue
        ax.annotate(
            "",
            xy=(x1, y1),
            xytext=(x0, y0),
            arrowprops={"arrowstyle": "-|>", "color": color, "lw": 1.0, "alpha": 0.95, "mutation_scale": 9},
        )


def write_flow_preview_png(
    path: str,
    background: np.ndarray,
    flowlines_geojson: Dict[str, Any],
    title: str,
    extent: tuple[float, float, float, float] | None = None,
    outline_geojson: Dict[str, Any] | None = None,
    raster_crs: Any = None,
) -> None:
    """Render flowlines over a background raster."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    bg = np.array(background, dtype=float)
    fig, ax = plt.subplots(figsize=(8, 6), dpi=120)
    imshow_kwargs = {"cmap": "gray", "origin": "upper"}
    if extent is not None:
        imshow_kwargs["extent"] = extent
    image = ax.imshow(bg, **imshow_kwargs)

    for feat in flowlines_geojson.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") != "LineString":
            continue
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        xs = [pt[0] for pt in coords]
        ys = [pt[1] for pt in coords]
        ax.plot(xs, ys, linewidth=0.9, color="#16a34a", alpha=0.9)
        _draw_line_arrows(ax, xs, ys, color="#00d1ff")

    ax.set_title(title)
    if extent is None:
        ax.set_xlabel("X (pixel index)")
        ax.set_ylabel("Y (pixel index)")
    else:
        _apply_lonlat_axes(ax, raster_crs)

    drew_outline = False
    if outline_geojson:
        for ring in _outline_world_traces(outline_geojson, raster_crs):
            ax.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.9, alpha=0.95)
            drew_outline = True
    cbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Background elevation")
    handles = [
        Patch(facecolor=plt.get_cmap("gray")(0.6), edgecolor="black", label="Background DEM"),
        Line2D([0], [0], color="#16a34a", lw=1.4, label="Flowlines"),
        Line2D([0], [0], color="#00d1ff", lw=1.2, label="Flow vectors (arrows)"),
    ]
    if drew_outline:
        handles.append(Line2D([0], [0], color="#f8f36a", lw=2.0, label="Glacier outline"))
    ax.legend(handles=handles, loc="upper right", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)


def write_overlay_png(
    path: str,
    depth: np.ndarray,
    flowlines_geojson: Dict[str, Any],
    title: str = "Depth + Flow",
    extent: tuple[float, float, float, float] | None = None,
    outline_geojson: Dict[str, Any] | None = None,
    raster_crs: Any = None,
) -> None:
    """Render final overlay preview by plotting depth and flowline pixel coordinates."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(9, 7), dpi=120)
    imshow_kwargs = {"cmap": "cividis", "origin": "upper"}
    if extent is not None:
        imshow_kwargs["extent"] = extent
    image = ax.imshow(depth, **imshow_kwargs)

    for feat in flowlines_geojson.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") != "LineString":
            continue
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        xs = [pt[0] for pt in coords]
        ys = [pt[1] for pt in coords]
        ax.plot(xs, ys, color="black", linewidth=0.9, alpha=0.9)
        _draw_line_arrows(ax, xs, ys, color="#22d3ee")

    ax.set_title(title)
    if extent is None:
        ax.set_xlabel("X (pixel index)")
        ax.set_ylabel("Y (pixel index)")
    else:
        _apply_lonlat_axes(ax, raster_crs)

    drew_outline = False
    if outline_geojson:
        for ring in _outline_world_traces(outline_geojson, raster_crs):
            ax.plot(ring["x"], ring["y"], color="#f8f36a", linewidth=1.9, alpha=0.95)
            drew_outline = True
    cbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Depth (m)")
    handles = [
        Patch(facecolor=plt.get_cmap("cividis")(0.7), edgecolor="black", label="Depth raster"),
        Line2D([0], [0], color="black", lw=1.4, label="Flowlines"),
        Line2D([0], [0], color="#22d3ee", lw=1.2, label="Flow vectors (arrows)"),
    ]
    if drew_outline:
        handles.append(Line2D([0], [0], color="#f8f36a", lw=2.0, label="Glacier outline"))
    ax.legend(handles=handles, loc="upper right", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)


def build_zip_bundle(output_dir: str, zip_name: str, files: Iterable[str]) -> str:
    """Create ZIP bundle containing selected files from output directory."""
    out_dir = Path(output_dir)
    bundle_path = out_dir / zip_name
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel in files:
            full = out_dir / rel
            if full.exists():
                zf.write(full, arcname=rel)
    return str(bundle_path)
