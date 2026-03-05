from __future__ import annotations

from pathlib import Path
import zipfile

from glacier_analysis.report import build_zip_bundle


def test_report_zip_creation(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir(parents=True, exist_ok=True)

    artifacts = ["config.yaml", "outline.geojson", "report.json"]
    for name in artifacts:
        (run_dir / name).write_text(f"placeholder for {name}\n", encoding="utf-8")

    zip_path = Path(build_zip_bundle(str(run_dir), "report_run_001.zip", artifacts))
    assert zip_path.exists()

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
        assert "config.yaml" in names
        assert "outline.geojson" in names
        assert "report.json" in names
