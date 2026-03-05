from __future__ import annotations

import numpy as np

from glacier_analysis.processing import depth_from_slope, slope_radians


def test_slope_and_depth_math_stability() -> None:
    dem = np.full((32, 32), 2500.0, dtype=np.float32)
    dem += np.linspace(0.0, 1e-4, dem.size, dtype=np.float32).reshape(dem.shape)

    slope = slope_radians(dem, pixel_size_x=30.0, pixel_size_y=30.0)
    depth = depth_from_slope(slope, tau_f=100.0, epsilon=1e-3, depth_min_m=0.0, depth_max_m=2000.0)

    assert slope.shape == dem.shape
    assert depth.shape == dem.shape
    assert np.isfinite(slope).all()
    assert np.isfinite(depth).all()
    assert (depth >= 0).all()
