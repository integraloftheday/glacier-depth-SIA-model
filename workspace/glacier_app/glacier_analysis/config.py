"""Configuration models and loading helpers for glacier analysis pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import yaml


BBox = Tuple[float, float, float, float]


@dataclass
class PipelineConfig:
    """Runtime configuration for a single glacier analysis run."""

    job_id: str
    bbox: BBox
    crs: str = "EPSG:4326"
    dataset: str = "opentopography"
    opentopo_api_key: Optional[str] = None
    grid_size_m: float = 100.0
    resolution_m: float = 30.0
    tau_f: float = 100.0
    tau_f_units: str = "m*rad"
    slope_units: str = "radians"
    output_dir: str = "./outputs"
    source: str = "osm"
    overpass_url: str = "https://overpass-api.de/api/interpreter"
    opentopo_url: str = "https://portal.opentopography.org/API/globaldem"
    request_timeout_s: int = 20
    epsilon_slope_rad: float = 1e-3
    depth_min_m: float = 0.0
    depth_max_m: float = 2000.0
    flow_seed_spacing_px: int = 30
    flow_step_px: float = 1.0
    flow_max_steps: int = 250
    allow_outline_fallback: bool = True
    allow_synthetic_dem_fallback: bool = True

    def validate(self) -> None:
        """Validate fields and raise ValueError on invalid inputs."""
        if len(self.bbox) != 4:
            raise ValueError("bbox must contain 4 values: minx,miny,maxx,maxy")
        minx, miny, maxx, maxy = self.bbox
        if minx >= maxx or miny >= maxy:
            raise ValueError("bbox is invalid; expected minx<maxx and miny<maxy")
        if self.grid_size_m <= 0:
            raise ValueError("grid_size_m must be > 0")
        if self.resolution_m <= 0:
            raise ValueError("resolution_m must be > 0")
        if self.tau_f <= 0:
            raise ValueError("tau_f must be > 0")
        if self.epsilon_slope_rad <= 0:
            raise ValueError("epsilon_slope_rad must be > 0")
        if self.depth_min_m < 0 or self.depth_max_m <= 0 or self.depth_min_m > self.depth_max_m:
            raise ValueError("depth clamps are invalid")
        if self.slope_units not in {"radians", "degrees"}:
            raise ValueError("slope_units must be 'radians' or 'degrees'")
        if self.source not in {"osm", "rgi"}:
            raise ValueError("source must be 'osm' or 'rgi'")

    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as a plain dictionary."""
        return asdict(self)


_FIELD_NAMES = {f.name for f in fields(PipelineConfig)}


def _default_job_id() -> str:
    return datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")


def _coerce_bbox(value: Any) -> BBox:
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",")]
        value = [float(p) for p in parts if p]
    if not isinstance(value, Iterable):
        raise ValueError("bbox must be an iterable or comma-separated string")
    vals = tuple(float(v) for v in value)
    if len(vals) != 4:
        raise ValueError("bbox must include exactly 4 numbers")
    return vals  # type: ignore[return-value]


def _normalize_config_dict(raw: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(raw)
    if "bbox" in cfg and cfg["bbox"] is not None:
        cfg["bbox"] = _coerce_bbox(cfg["bbox"])
    if "grid_size" in cfg and "grid_size_m" not in cfg:
        cfg["grid_size_m"] = cfg.pop("grid_size")
    if "outdir" in cfg and "output_dir" not in cfg:
        cfg["output_dir"] = cfg.pop("outdir")
    if "opentopo_key" in cfg and "opentopo_api_key" not in cfg:
        cfg["opentopo_api_key"] = cfg.pop("opentopo_key")
    return {k: v for k, v in cfg.items() if k in _FIELD_NAMES and v is not None}


def load_yaml_config(path: Optional[str]) -> Dict[str, Any]:
    """Load YAML config from path and return normalized dict.

    Returns empty dict when path is None.
    """
    if path is None:
        return {}
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("Top-level YAML config must be a mapping")
    return _normalize_config_dict(raw)


def merge_config(yaml_cfg: Dict[str, Any], cli_cfg: Dict[str, Any]) -> PipelineConfig:
    """Merge YAML config with CLI overrides and return validated config."""
    merged: Dict[str, Any] = {}
    merged.update(yaml_cfg)
    merged.update(_normalize_config_dict(cli_cfg))

    if "job_id" not in merged or not merged.get("job_id"):
        merged["job_id"] = _default_job_id()
    if "bbox" not in merged:
        raise ValueError("bbox is required (set in YAML or --bbox)")

    if "output_dir" not in merged or not merged.get("output_dir"):
        merged["output_dir"] = f"./outputs/{merged['job_id']}"
    else:
        out_path = Path(str(merged["output_dir"]))
        if out_path.name in {"outputs", "output"}:
            merged["output_dir"] = str(out_path / str(merged["job_id"]))

    cfg = PipelineConfig(**merged)
    cfg.validate()
    return cfg


def save_config_yaml(config: PipelineConfig, path: str) -> None:
    """Write effective config to disk."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config.to_dict(), f, sort_keys=False)
