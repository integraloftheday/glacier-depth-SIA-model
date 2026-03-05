from __future__ import annotations

import numpy as np
from pyproj import CRS

from glacier_analysis.elevation import generate_synthetic_dem


def test_synthetic_dem_generation_shape() -> None:
    bbox = (-122.53, 37.6, -122.35, 37.83)
    dem, transform = generate_synthetic_dem(bbox=bbox, resolution_m=60.0, dst_crs=CRS.from_epsg(32610))

    assert isinstance(dem, np.ndarray)
    assert dem.ndim == 2
    assert dem.shape[0] >= 16 and dem.shape[1] >= 16
    assert np.isfinite(dem).all()
    assert transform.a > 0
