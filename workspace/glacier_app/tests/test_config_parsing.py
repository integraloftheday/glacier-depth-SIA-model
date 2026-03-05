from __future__ import annotations

from pathlib import Path

from glacier_analysis.config import load_yaml_config, merge_config


def test_config_parsing_and_merge(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "job_id: run_test_001",
                "bbox: [-122.53, 37.6, -122.35, 37.83]",
                "crs: EPSG:4326",
                "dataset: opentopography",
                "grid_size_m: 100",
                "resolution_m: 30",
                "tau_f: 100.0",
                "slope_units: radians",
                "output_dir: ./outputs/run_test_001",
            ]
        ),
        encoding="utf-8",
    )

    yaml_cfg = load_yaml_config(str(config_path))
    merged = merge_config(yaml_cfg, {"tau_f": 120.0})

    assert merged.job_id == "run_test_001"
    assert merged.bbox == (-122.53, 37.6, -122.35, 37.83)
    assert merged.grid_size_m == 100
    assert merged.tau_f == 120.0
