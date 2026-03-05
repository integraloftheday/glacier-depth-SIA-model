"""Geometry helpers for glacier outline retrieval and fallback synthesis."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import requests
import math
import re

from shapely.geometry import MultiPoint, Point, Polygon, mapping
from shapely.ops import unary_union

BBox = Tuple[float, float, float, float]
DEFAULT_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]


def _bbox_polygon(bbox: BBox) -> Polygon:
    minx, miny, maxx, maxy = bbox
    return Polygon([(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)])


def synthetic_outline_geojson(bbox: BBox) -> Dict[str, Any]:
    """Create a deterministic non-rectangular fallback glacier-like outline."""
    minx, miny, maxx, maxy = bbox
    cx = (minx + maxx) / 2.0
    cy = (miny + maxy) / 2.0
    rx = (maxx - minx) * 0.35
    ry = (maxy - miny) * 0.4

    ring = []
    for i in range(72):
        t = (2.0 * math.pi * i) / 72.0
        jitter = 1.0 + 0.12 * math.sin(3.0 * t) - 0.08 * math.cos(5.0 * t)
        x = cx + rx * jitter * math.cos(t)
        y = cy + ry * jitter * math.sin(t)
        ring.append((x, y))
    ring.append(ring[0])
    poly = Polygon(ring)
    if not poly.is_valid or poly.area <= 0:
        poly = _bbox_polygon(bbox)
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"source": "synthetic_fallback", "fallback": True},
                "geometry": mapping(poly),
            }
        ],
    }


def _overpass_query(bbox: BBox) -> str:
    minx, miny, maxx, maxy = bbox
    south, west, north, east = miny, minx, maxy, maxx
    return f"""
[out:json][timeout:25];
(
  way["natural"="glacier"]({south},{west},{north},{east});
  relation["natural"="glacier"]({south},{west},{north},{east});
);
out geom;
""".strip()


def _overpass_query_point(lat: float, lon: float, radius_m: int) -> str:
    return f"""
[out:json][timeout:25];
(
  way["natural"="glacier"](around:{radius_m},{lat},{lon});
  relation["natural"="glacier"](around:{radius_m},{lat},{lon});
);
out geom;
""".strip()


def _overpass_query_relation(relation_id: int) -> str:
    return f"""
[out:json][timeout:25];
(
  relation({relation_id});
);
out body;
>;
out geom;
""".strip()


def _overpass_query_relation_or_way(osm_id: int) -> str:
    return f"""
[out:json][timeout:25];
relation({osm_id});
(._;>;);
out body geom;
""".strip()


def _overpass_query_way_only(osm_id: int) -> str:
    return f"""
[out:json][timeout:25];
(
  way({osm_id});
);
out body geom;
""".strip()


def _overpass_query_name(glacier_name: str) -> str:
    safe_name = glacier_name.replace('"', '\\"')
    return f"""
[out:json][timeout:25];
(
  way["natural"="glacier"]["name"="{safe_name}"];
  relation["natural"="glacier"]["name"="{safe_name}"];
);
out body;
>;
out geom;
""".strip()


def _post_overpass(query: str, overpass_url: str, timeout_s: int) -> Dict[str, Any]:
    endpoints = [overpass_url] + [u for u in DEFAULT_OVERPASS_ENDPOINTS if u != overpass_url]
    last_exc: Exception | None = None
    for endpoint in endpoints:
        try:
            response = requests.post(endpoint, data={"data": query}, timeout=timeout_s)
            if response.status_code == 429:
                last_exc = RuntimeError(f"Overpass rate limit at {endpoint}: HTTP 429")
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # pragma: no cover - network variance
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Overpass request failed with no additional error detail")


def _polygon_from_way_geometry(coords: List[Dict[str, float]]) -> Polygon | None:
    if len(coords) < 3:
        return None
    ring = [(c["lon"], c["lat"]) for c in coords if "lon" in c and "lat" in c]
    if len(ring) < 3:
        return None
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    poly = Polygon(ring)
    if poly.is_valid and poly.area > 0:
        return poly

    repaired = poly.buffer(0)
    if repaired.geom_type == "Polygon" and repaired.is_valid and repaired.area > 0:
        return repaired

    # Fallback for non-closed/noisy rings: convex hull of way vertices.
    hull = MultiPoint(ring).convex_hull
    if hull.geom_type == "Polygon" and hull.area > 0:
        return hull
    return None


def fetch_osm_outline_geojson(bbox: BBox, overpass_url: str, timeout_s: int = 20) -> Dict[str, Any]:
    """Fetch glacier outlines from Overpass API and return a dissolved GeoJSON polygon.

    Raises requests exceptions on network errors, ValueError for malformed payloads,
    and LookupError when no glacier geometry is found.
    """
    data = _post_overpass(_overpass_query(bbox), overpass_url=overpass_url, timeout_s=timeout_s)
    elements = data.get("elements", [])
    if not isinstance(elements, list):
        raise ValueError("Overpass response missing valid elements list")

    polys: List[Polygon] = []
    for el in elements:
        if el.get("type") == "way" and "geometry" in el:
            poly = _polygon_from_way_geometry(el["geometry"])
            if poly is not None:
                polys.append(poly)

    if not polys:
        raise LookupError("No glacier outlines found in Overpass response")

    merged = unary_union(polys)
    if merged.is_empty:
        raise LookupError("Glacier outlines dissolved to empty geometry")

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"source": "osm", "fallback": False, "feature_count": len(polys)},
                "geometry": mapping(merged),
            }
        ],
    }


def get_outline_geojson(
    bbox: BBox,
    overpass_url: str,
    timeout_s: int = 20,
    allow_fallback: bool = True,
) -> Dict[str, Any]:
    """Return OSM outline when possible, otherwise deterministic bbox fallback."""
    try:
        return fetch_osm_outline_geojson(bbox, overpass_url=overpass_url, timeout_s=timeout_s)
    except Exception:
        if not allow_fallback:
            raise
        return synthetic_outline_geojson(bbox)


def parse_relation_id_from_text(text: str) -> int:
    """Extract relation id from pasted OSM snippets."""
    patterns = [
        r"Relation:\s*.+\((\d+)\)",
        r"\brelation\s*[#: ]\s*(\d+)\b",
        r"\brelation\s*\(\s*(\d+)\s*\)",
        r"\((\d{4,})\)",
        r"^\s*(\d{4,})\s*$",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return int(m.group(1))
    raise ValueError("Could not parse relation id from pasted text")


def parse_glacier_name_from_text(text: str) -> str | None:
    """Best-effort extraction of glacier name from pasted snippets."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if lines:
        m = re.match(r"(.+?)\s*\(\d{4,}\)", lines[0], flags=re.IGNORECASE)
        if m:
            candidate = re.sub(r"^Relation:\s*", "", m.group(1), flags=re.IGNORECASE).strip()
            if candidate:
                return candidate
    m = re.search(r"Relation:\s*([^(\\n]+)", text, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate
    m = re.search(r"\bname\s+([^\n]+)", text, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate
    return None


def select_glacier_by_relation_id(
    relation_id: int,
    overpass_url: str,
    timeout_s: int = 20,
) -> Dict[str, Any]:
    """Fetch a glacier relation by id and return dissolved polygon + bbox."""
    data = _post_overpass(_overpass_query_relation_or_way(relation_id), overpass_url=overpass_url, timeout_s=timeout_s)
    elements = data.get("elements", [])
    if not isinstance(elements, list):
        raise ValueError("Overpass response missing valid elements list")

    polys: List[Polygon] = []
    relation_name = None
    selection_source = "osm_relation"
    for el in elements:
        if el.get("type") == "relation":
            tags = el.get("tags", {})
            if isinstance(tags, dict):
                relation_name = tags.get("name")
        if el.get("type") == "way":
            selection_source = "osm_way"
        if el.get("type") == "way" and "geometry" in el:
            poly = _polygon_from_way_geometry(el["geometry"])
            if poly is not None:
                polys.append(poly)

    if not polys:
        # Fallback for plain way IDs where no relation exists.
        data_way = _post_overpass(_overpass_query_way_only(relation_id), overpass_url=overpass_url, timeout_s=timeout_s)
        elements_way = data_way.get("elements", [])
        if isinstance(elements_way, list):
            for el in elements_way:
                if el.get("type") == "way":
                    selection_source = "osm_way"
                if el.get("type") == "way" and "geometry" in el:
                    poly = _polygon_from_way_geometry(el["geometry"])
                    if poly is not None:
                        polys.append(poly)
        if not polys:
            raise LookupError(f"No polygon geometry found for relation/way id {relation_id}")

    merged = unary_union(polys)
    if merged.is_empty:
        raise LookupError(f"Relation {relation_id} dissolved to empty geometry")

    minx, miny, maxx, maxy = merged.bounds
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "source": "osm_relation",
                    "selection_source": selection_source,
                    "relation_id": relation_id,
                    "name": relation_name,
                    "fallback": False,
                },
                "geometry": mapping(merged),
            }
        ],
        "bbox": [float(minx), float(miny), float(maxx), float(maxy)],
    }


def select_glacier_by_name(
    glacier_name: str,
    overpass_url: str,
    timeout_s: int = 20,
) -> Dict[str, Any]:
    """Fetch glacier polygon by exact OSM name match."""
    data = _post_overpass(_overpass_query_name(glacier_name), overpass_url=overpass_url, timeout_s=timeout_s)
    elements = data.get("elements", [])
    if not isinstance(elements, list):
        raise ValueError("Overpass response missing valid elements list")

    polys: List[Polygon] = []
    matched_name = glacier_name
    for el in elements:
        if el.get("type") in {"relation", "way"}:
            tags = el.get("tags", {})
            if isinstance(tags, dict) and tags.get("name"):
                matched_name = str(tags.get("name"))
        if el.get("type") == "way" and "geometry" in el:
            poly = _polygon_from_way_geometry(el["geometry"])
            if poly is not None:
                polys.append(poly)

    if not polys:
        raise LookupError(f"No polygon geometry found for glacier name '{glacier_name}'")

    merged = unary_union(polys)
    if merged.is_empty:
        raise LookupError(f"Name '{glacier_name}' dissolved to empty geometry")

    minx, miny, maxx, maxy = merged.bounds
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "source": "osm_name",
                    "name": matched_name,
                    "query_name": glacier_name,
                    "fallback": False,
                },
                "geometry": mapping(merged),
            }
        ],
        "bbox": [float(minx), float(miny), float(maxx), float(maxy)],
    }


def select_glacier_at_point(
    lon: float,
    lat: float,
    overpass_url: str,
    radius_m: int = 5000,
    timeout_s: int = 20,
) -> Dict[str, Any]:
    """Return a single best-matching glacier polygon near the clicked point."""
    data = _post_overpass(_overpass_query_point(lat=lat, lon=lon, radius_m=radius_m), overpass_url=overpass_url, timeout_s=timeout_s)
    elements = data.get("elements", [])
    if not isinstance(elements, list):
        raise ValueError("Overpass response missing valid elements list")

    point = Point(lon, lat)
    candidates: List[Polygon] = []
    for el in elements:
        if el.get("type") == "way" and "geometry" in el:
            poly = _polygon_from_way_geometry(el["geometry"])
            if poly is not None:
                candidates.append(poly)

    if not candidates:
        raise LookupError("No glacier found near clicked point")

    containing = [p for p in candidates if p.contains(point)]
    if containing:
        selected = max(containing, key=lambda p: p.area)
    else:
        selected = min(candidates, key=lambda p: p.centroid.distance(point))

    minx, miny, maxx, maxy = selected.bounds
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"source": "osm_selected", "fallback": False},
                "geometry": mapping(selected),
            }
        ],
        "bbox": [float(minx), float(miny), float(maxx), float(maxy)],
    }
