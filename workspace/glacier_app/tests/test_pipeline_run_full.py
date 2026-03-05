from __future__ import annotations

from pathlib import Path

from glacier_analysis.cli_runner import run_pipeline
from glacier_analysis.config import PipelineConfig


def test_run_pipeline_offline_smoke(tmp_path: Path) -> None:
    out_dir = tmp_path / "run_full"
    cfg = PipelineConfig(
        job_id="test_run_full",
        bbox=(-122.53, 37.6, -122.35, 37.83),
        source="osm",
        resolution_m=60.0,
        grid_size_m=120.0,
        tau_f=110.0,
        slope_units="radians",
        output_dir=str(out_dir),
        overpass_url="http://127.0.0.1:9",
        request_timeout_s=1,
    )

    summary = run_pipeline(cfg)

    assert summary["job_id"] == "test_run_full"
    assert summary["dem_source"] in {"synthetic_dem", "opentopography"}
    assert (out_dir / "dem.tif").exists()
    assert (out_dir / "slope_angle.tif").exists()
    assert (out_dir / "avg_slope.tif").exists()
    assert (out_dir / "flowlines.geojson").exists()
    assert (out_dir / "depth.tif").exists()
    assert (out_dir / "report_test_run_full.zip").exists()
