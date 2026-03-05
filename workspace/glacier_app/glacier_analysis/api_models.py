from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class ValidationError(ValueError):
    """Raised when request payload validation fails."""


def _parse_bbox(raw: Any) -> List[float]:
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) != 4:
            raise ValidationError("bbox string must have 4 comma-separated values")
        try:
            vals = [float(x) for x in parts]
        except ValueError as exc:
            raise ValidationError("bbox string contains non-numeric values") from exc
    elif isinstance(raw, (list, tuple)) and len(raw) == 4:
        try:
            vals = [float(x) for x in raw]
        except (TypeError, ValueError) as exc:
            raise ValidationError("bbox must contain 4 numeric values") from exc
    else:
        raise ValidationError("bbox must be a list of 4 numbers or a comma-separated string")

    minx, miny, maxx, maxy = vals
    if minx >= maxx or miny >= maxy:
        raise ValidationError("bbox must satisfy minx < maxx and miny < maxy")
    if minx < -180 or maxx > 180 or miny < -90 or maxy > 90:
        raise ValidationError("bbox coordinates must be within WGS84 bounds")
    return vals


def _as_float(payload: Dict[str, Any], key: str, default: Optional[float] = None) -> float:
    raw = payload.get(key, default)
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        if default is not None:
            return float(default)
        raise ValidationError(f"'{key}' is required")
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"'{key}' must be numeric") from exc


def _as_int(payload: Dict[str, Any], key: str, default: Optional[int] = None) -> int:
    raw = payload.get(key, default)
    if raw is None:
        raise ValidationError(f"'{key}' is required")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"'{key}' must be an integer") from exc


@dataclass
class AreaRequest:
    bbox: List[float]
    source: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "AreaRequest":
        bbox = _parse_bbox(payload.get("bbox"))
        source = str(payload.get("source", "osm")).strip().lower()
        if source not in {"osm", "rgi"}:
            raise ValidationError("'source' must be either 'osm' or 'rgi'")
        return cls(bbox=bbox, source=source)


@dataclass
class ElevationRequest:
    bbox: List[float]
    resolution: float
    dataset: str
    api_key: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "ElevationRequest":
        bbox = _parse_bbox(payload.get("bbox"))
        resolution = _as_float(payload, "resolution", 30.0)
        if resolution <= 0:
            raise ValidationError("'resolution' must be > 0")
        dataset = str(payload.get("dataset", "opentopo")).strip().lower()
        api_key = str(payload.get("api_key", "")).strip()
        return cls(bbox=bbox, resolution=resolution, dataset=dataset, api_key=api_key)


@dataclass
class SlopeRequest:
    dem_path: str
    units: str
    method: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "SlopeRequest":
        dem_path = str(payload.get("dem_path", "")).strip()
        if not dem_path:
            raise ValidationError("'dem_path' is required")
        units = str(payload.get("units", "radians")).strip().lower()
        if units not in {"radians", "degrees"}:
            raise ValidationError("'units' must be 'radians' or 'degrees'")
        method = str(payload.get("method", "central_diff")).strip()
        return cls(dem_path=dem_path, units=units, method=method)


@dataclass
class AverageSlopeRequest:
    slope_path: str
    grid_size_m: int

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "AverageSlopeRequest":
        slope_path = str(payload.get("slope_path", "")).strip()
        if not slope_path:
            raise ValidationError("'slope_path' is required")
        grid_size_m = _as_int(payload, "grid_size_m", 100)
        if grid_size_m <= 0:
            raise ValidationError("'grid_size_m' must be > 0")
        return cls(slope_path=slope_path, grid_size_m=grid_size_m)


@dataclass
class FlowRequest:
    slope_path: str
    method: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "FlowRequest":
        slope_path = str(payload.get("slope_path", "")).strip()
        if not slope_path:
            raise ValidationError("'slope_path' is required")
        method = str(payload.get("method", "gradient")).strip()
        return cls(slope_path=slope_path, method=method)


@dataclass
class DepthRequest:
    avg_slope_path: str
    bulk_constant_m: float
    f_prime: float

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "DepthRequest":
        avg_slope_path = str(payload.get("avg_slope_path", "")).strip()
        if not avg_slope_path:
            raise ValidationError("'avg_slope_path' is required")
        bulk_constant_m = _as_float(payload, "bulk_constant_m", _as_float(payload, "tau_0_kpa", 11.0))
        if bulk_constant_m <= 0:
            raise ValidationError("'bulk_constant_m' must be > 0")
        f_prime = _as_float(payload, "f_prime", 1.0)
        if f_prime <= 0:
            raise ValidationError("'f_prime' must be > 0")
        return cls(avg_slope_path=avg_slope_path, bulk_constant_m=bulk_constant_m, f_prime=f_prime)


@dataclass
class RunFullRequest:
    bbox: List[float]
    source: str
    resolution: float
    grid_size_m: int
    bulk_constant_m: float
    f_prime: float
    slope_units: str
    api_key: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "RunFullRequest":
        area_req = AreaRequest.from_payload(payload)
        resolution = _as_float(payload, "resolution", 30.0)
        if resolution <= 0:
            raise ValidationError("'resolution' must be > 0")
        grid_size_m = _as_int(payload, "grid_size_m", 100)
        if grid_size_m <= 0:
            raise ValidationError("'grid_size_m' must be > 0")
        bulk_constant_m = _as_float(payload, "bulk_constant_m", _as_float(payload, "tau_0_kpa", 11.0))
        if bulk_constant_m <= 0:
            raise ValidationError("'bulk_constant_m' must be > 0")
        f_prime = _as_float(payload, "f_prime", 1.0)
        if f_prime <= 0:
            raise ValidationError("'f_prime' must be > 0")
        slope_units = str(payload.get("slope_units", "radians")).strip().lower()
        if slope_units not in {"radians", "degrees"}:
            raise ValidationError("'slope_units' must be 'radians' or 'degrees'")
        api_key = str(payload.get("api_key", "")).strip()
        return cls(
            bbox=area_req.bbox,
            source=area_req.source,
            resolution=resolution,
            grid_size_m=grid_size_m,
            bulk_constant_m=bulk_constant_m,
            f_prime=f_prime,
            slope_units=slope_units,
            api_key=api_key,
        )
