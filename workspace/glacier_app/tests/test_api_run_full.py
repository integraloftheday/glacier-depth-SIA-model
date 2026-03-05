from __future__ import annotations

from pathlib import Path
import zipfile

from app import create_app


def test_api_run_full_smoke() -> None:
    app = create_app()
    client = app.test_client()

    payload = {
        "job_id": "test_api_run_full",
        "bbox": [-122.53, 37.6, -122.35, 37.83],
        "source": "osm",
        "resolution": 60,
        "grid_size_m": 120,
        "bulk_constant_m": 11,
        "slope_units": "radians",
        "api_key": "",
        "allow_synthetic_fallback": True,
        "overpass_url": "http://127.0.0.1:9",
        "timeout": 1,
    }

    resp = client.post("/api/run_full", json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data is not None
    assert data["job_id"] == "test_api_run_full"
    assert "artifacts" in data
    assert "dem_path" in data["artifacts"]
    assert "depth_topomap_overlay" in data["artifacts"]
    assert "flow_topomap_overlay" in data["artifacts"]
    assert "report_url" in data


def test_api_reanalyze_reuses_existing_dem() -> None:
    app = create_app()
    client = app.test_client()

    payload = {
        "job_id": "test_api_reanalyze",
        "bbox": [-122.53, 37.6, -122.35, 37.83],
        "source": "osm",
        "resolution": 60,
        "grid_size_m": 120,
        "bulk_constant_m": 11,
        "slope_units": "radians",
        "api_key": "",
        "allow_synthetic_fallback": True,
        "overpass_url": "http://127.0.0.1:9",
        "timeout": 1,
    }
    first = client.post("/api/run_full", json=payload)
    assert first.status_code == 200
    first_data = first.get_json()
    assert first_data is not None
    dem_path = first_data["artifacts"]["dem_path"]

    second = client.post(
        "/api/reanalyze",
        json={
            "job_id": "test_api_reanalyze",
            "grid_size_m": 60,
            "bulk_constant_m": 200,
            "slope_units": "degrees",
        },
    )
    assert second.status_code == 200
    second_data = second.get_json()
    assert second_data is not None
    assert second_data["summary"]["reused_dem"] is True
    assert second_data["summary"]["grid_size_m"] == 60
    assert second_data["summary"]["depth_scale_m"] == 200.0
    assert second_data["summary"]["dem_path"] == dem_path


def test_api_run_full_uses_default_resolution_when_missing() -> None:
    app = create_app()
    client = app.test_client()

    payload = {
        "job_id": "test_api_default_resolution",
        "bbox": [-122.53, 37.6, -122.35, 37.83],
        "source": "osm",
        "grid_size_m": 120,
        "bulk_constant_m": 11,
        "slope_units": "radians",
        "api_key": "",
        "allow_synthetic_fallback": True,
        "overpass_url": "http://127.0.0.1:9",
        "timeout": 1,
    }
    resp = client.post("/api/run_full", json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data is not None
    assert data["summary"]["resolution_m"] == 30.0


def test_api_run_full_uses_open_topo_key_from_env(monkeypatch) -> None:
    monkeypatch.setenv("OPEN_TOPO_KEY", "from_env_key")
    app = create_app()
    client = app.test_client()

    payload = {
        "job_id": "test_api_env_key",
        "bbox": [-122.53, 37.6, -122.35, 37.83],
        "source": "osm",
        "resolution": 60,
        "grid_size_m": 120,
        "bulk_constant_m": 11,
        "slope_units": "radians",
        "api_key": "",
        "allow_synthetic_fallback": True,
        "overpass_url": "http://127.0.0.1:9",
        "timeout": 1,
    }
    resp = client.post("/api/run_full", json=payload)
    assert resp.status_code == 200
    config_path = Path("/workspace/glacier_app/outputs/test_api_env_key/config.yaml")
    assert config_path.exists()
    cfg_text = config_path.read_text(encoding="utf-8")
    assert "opentopo_api_key: from_env_key" in cfg_text


def test_api_list_and_load_jobs() -> None:
    app = create_app()
    client = app.test_client()

    payload = {
        "job_id": "test_api_load_job",
        "bbox": [-122.53, 37.6, -122.35, 37.83],
        "source": "osm",
        "resolution": 60,
        "grid_size_m": 120,
        "bulk_constant_m": 11,
        "slope_units": "radians",
        "api_key": "",
        "allow_synthetic_fallback": True,
        "overpass_url": "http://127.0.0.1:9",
        "timeout": 1,
    }
    run_resp = client.post("/api/run_full", json=payload)
    assert run_resp.status_code == 200

    jobs_resp = client.get("/api/jobs")
    assert jobs_resp.status_code == 200
    jobs_data = jobs_resp.get_json()
    assert jobs_data is not None
    job_ids = {row["job_id"] for row in jobs_data.get("jobs", [])}
    assert "test_api_load_job" in job_ids

    load_resp = client.get("/api/load_job/test_api_load_job")
    assert load_resp.status_code == 200
    load_data = load_resp.get_json()
    assert load_data is not None
    assert load_data["job_id"] == "test_api_load_job"
    assert "artifacts" in load_data
    assert "dem_path" in load_data["artifacts"]
    assert load_data["report_url"] == "/api/report/test_api_load_job"


def test_api_report_bundle_contains_topo_composites_and_elevation_plot() -> None:
    app = create_app()
    client = app.test_client()

    payload = {
        "job_id": "test_api_report_bundle_topo",
        "bbox": [-122.53, 37.6, -122.35, 37.83],
        "source": "osm",
        "resolution": 60,
        "grid_size_m": 120,
        "bulk_constant_m": 11,
        "slope_units": "radians",
        "api_key": "",
        "allow_synthetic_fallback": True,
        "overpass_url": "http://127.0.0.1:9",
        "timeout": 1,
    }
    run_resp = client.post("/api/run_full", json=payload)
    assert run_resp.status_code == 200

    report_resp = client.get("/api/report/test_api_report_bundle_topo")
    assert report_resp.status_code == 200
    zip_path = Path("/workspace/glacier_app/outputs/test_api_report_bundle_topo/report_test_api_report_bundle_topo.zip")
    assert zip_path.exists()
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
        assert "elevation_plot.png" in names
        assert "depth_topo_composite.png" in names
        assert "flow_topo_composite.png" in names
        assert "summary_2x2.png" in names
