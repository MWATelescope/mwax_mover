"""Tests for calibration.fitting: phase-ramp/gain fitting and their
numeric helpers.

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg). _FREQS_HZ/_make_phase_ramp and the coarse-channel
constants are shared fixtures for the fit_phase_line/fit_gain tests only
-- confirmed by checking actual usage before splitting, since the same
module also has test_outliers.py's own, unrelated _make_phase_fits_df
sharing this file in the original.
"""

import numpy as np
import pytest
from astropy import units as u
from astropy.constants import c as speed_of_light  # ty: ignore[unresolved-import]

from mwax_mover.calibration.fitting import (
    ensure_system_byte_order,
    fit_gain,
    fit_phase_line,
    pad_gain_fit_info,
    pad_gains_to_full_coarse,
    parse_csv_header,
    wrap_angle,
)
from mwax_mover.calibration.models import GainFitInfo, PhaseFitInfo


# Realistic MWA-like frequency array: 100 chanblocks from 140 to 170 MHz
_FREQS_HZ = np.linspace(140e6, 170e6, 100)


# 24 coarse channels × 4 chanblocks each = 96 total chanblocks
_N_COARSE = 24


_CHANBLOCKS_PER_COARSE = 4


_N_CHANBLOCKS = _N_COARSE * _CHANBLOCKS_PER_COARSE


_GAIN_FREQS = np.linspace(138e6, 170e6, _N_CHANBLOCKS)


def _make_phase_ramp(freqs_hz: np.ndarray, length_m: float, intercept_rad: float) -> np.ndarray:
    """Construct a complex array representing a pure phase ramp.

    Args:
        freqs_hz: Array of frequencies in Hz.
        length_m: Equivalent cable length in metres.
        intercept_rad: Phase intercept in radians.

    Returns:
        Complex array with unit amplitude and the specified phase ramp.
    """
    slope = (2 * np.pi * u.rad * (length_m * u.m) / speed_of_light).to(u.rad / u.Hz).value
    phase = slope * freqs_hz + intercept_rad
    return np.exp(1j * phase)


def test_wrap_angle_zero():
    assert wrap_angle(0.0) == pytest.approx(0.0)


def test_wrap_angle_at_pi():
    """wrap_angle(pi) should return a value in (-pi, pi]."""
    result = wrap_angle(np.pi)
    assert -np.pi <= result <= np.pi


def test_wrap_angle_beyond_pi():
    result = wrap_angle(4.0)
    assert -np.pi <= result <= np.pi


def test_wrap_angle_below_neg_pi():
    result = wrap_angle(-4.0)
    assert -np.pi <= result <= np.pi


def test_wrap_angle_array():
    angles = np.linspace(-2 * np.pi, 2 * np.pi, 50)
    results = wrap_angle(angles)
    assert np.all(results >= -np.pi)
    assert np.all(results <= np.pi)


def test_wrap_angle_identity_near_zero():
    """Small angles near zero should pass through unchanged."""
    small = np.array([0.1, -0.1, 0.5, -0.5])
    np.testing.assert_allclose(wrap_angle(small), small, atol=1e-12)


def test_parse_csv_header_ints():
    result = parse_csv_header("1,2,3", int)
    np.testing.assert_array_equal(result, np.array([1, 2, 3]))
    assert result.dtype == int


def test_parse_csv_header_floats():
    result = parse_csv_header("1.5,2.5,3.5", float)
    np.testing.assert_allclose(result, np.array([1.5, 2.5, 3.5]))


def test_parse_csv_header_single_value():
    result = parse_csv_header("42", int)
    np.testing.assert_array_equal(result, np.array([42]))


def test_ensure_system_byte_order_native():
    """A native-order float64 array should pass through with the same values."""
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    result = ensure_system_byte_order(arr)
    np.testing.assert_array_equal(result, arr)


def test_ensure_system_byte_order_swapped():
    """A genuinely non-native-byte-order array must be converted to the
    correct native values, not just have its dtype label changed.

    Uses .astype() to build the fixture (an actual byte-swap, changing the
    underlying bytes), not .view() (which only relabels the dtype while
    leaving the bytes untouched) -- .view() would make this test pass
    trivially without ever exercising a real byte-swapped array, the way
    real big-endian FITS data actually arrives.
    """
    native = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    swapped_dt = native.dtype.newbyteorder("S")
    genuinely_swapped = native.astype(swapped_dt)
    assert genuinely_swapped.tobytes() != native.tobytes()  # sanity: bytes actually differ

    result = ensure_system_byte_order(genuinely_swapped)
    np.testing.assert_array_equal(result, native)


def test_fit_phase_line_recovers_length():
    """A 10m cable should be recovered to within ±0.5m."""
    target_length = 10.0
    solns = _make_phase_ramp(_FREQS_HZ, target_length, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert abs(result.length - target_length) < 0.5, f"Expected length ≈ {target_length}m, got {result.length:.3f}m"


def test_fit_phase_line_recovers_intercept():
    """Intercept of 0.5 rad should be recovered to within ±0.1 rad."""
    target_intercept = 0.5
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=target_intercept)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert abs(result.intercept - target_intercept) < 0.1, (
        f"Expected intercept ≈ {target_intercept} rad, got {result.intercept:.4f} rad"
    )


def test_fit_phase_line_zero_slope():
    """A flat phase (zero cable) should yield a length near 0."""
    solns = np.ones(len(_FREQS_HZ), dtype=np.complex128)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert abs(result.length) < 0.5, f"Expected length ≈ 0m, got {result.length:.3f}m"


def test_fit_phase_line_negative_length():
    """A negative cable length (reversed slope) should be recovered with correct sign."""
    target_length = -8.0
    solns = _make_phase_ramp(_FREQS_HZ, target_length, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert result.length < 0, f"Expected negative length, got {result.length:.3f}m"
    assert abs(result.length - target_length) < 0.5


def test_fit_phase_line_quality_is_one_all_valid():
    """With all weights=1 and no NaNs, quality should equal 1.0."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert result.quality == pytest.approx(1.0)


def test_fit_phase_line_quality_partial_weights():
    """With half the weights zeroed, quality should be < 1.0."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    weights[: len(_FREQS_HZ) // 2] = 0.0
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert result.quality < 1.0


def test_fit_phase_line_all_nan_raises():
    """All-NaN solutions should raise RuntimeError."""
    solns = np.full(len(_FREQS_HZ), np.nan, dtype=np.complex128)
    weights = np.ones(len(_FREQS_HZ))
    with pytest.raises(RuntimeError):
        fit_phase_line(_FREQS_HZ, solns, weights)


def test_fit_phase_line_all_zero_weights_raises():
    """All-zero weights should raise RuntimeError."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.zeros(len(_FREQS_HZ))
    with pytest.raises(RuntimeError):
        fit_phase_line(_FREQS_HZ, solns, weights)


def test_fit_phase_line_niter_zero_raises():
    """niter=0 is invalid and should raise ValueError immediately."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    with pytest.raises(ValueError, match="niter must be >= 1"):
        fit_phase_line(_FREQS_HZ, solns, weights, niter=0)


def test_fit_phase_line_niter_greater_than_one_converges():
    """niter=3 on a clean ramp should still recover the cable length accurately."""
    target_length = 10.0
    solns = _make_phase_ramp(_FREQS_HZ, target_length, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights, niter=3)
    assert abs(result.length - target_length) < 0.5, (
        f"Expected length ≈ {target_length}m with niter=3, got {result.length:.3f}m"
    )


def test_fit_phase_line_niter_stops_if_too_few_points():
    """With heavy outlier contamination, the loop should exit early rather than crash.

    Inject 90% outlier noise so that after one outlier-rejection pass fewer
    than 2 points remain. The function must return (not raise) in that case.
    """
    rng = np.random.default_rng(42)
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    # Overwrite 95 of 100 points with random noise — only 5 valid points remain
    noise_indices = rng.choice(len(_FREQS_HZ), size=95, replace=False)
    solns[noise_indices] = np.exp(1j * rng.uniform(-np.pi, np.pi, size=95))
    weights = np.ones(len(_FREQS_HZ))
    # Should complete without raising, even with niter=5
    result = fit_phase_line(_FREQS_HZ, solns, weights, niter=5)
    assert isinstance(result, PhaseFitInfo)


def test_fit_phase_line_niter_greater_than_one_with_realistic_clipping():
    """A modest, realistic level of contamination must not crash across iterations.

    Regression test: freqs_hz_qty (the astropy-Quantity frequency array used
    inside the niter loop) must be re-narrowed by the sigma-clip mask on
    every iteration, exactly like solution is. It previously wasn't -- a
    leftover, never-read `freqs_hz = freqs_hz[mask]` line updated the wrong
    (pre-Quantity-conversion, dead) variable -- so as soon as any iteration
    clipped even one channel, the next iteration's minimize()/model() calls
    received mismatched-length frequency and solution arrays and raised
    ValueError ("operands could not be broadcast together"). That exception
    is caught by callers (see HyperfitsSolutionGroup._phase_fit_one) and
    silently turns into a dropped phase fit for that tile/pol -- so this
    isn't a hypothetical: with niter=3 (production's configured value) and
    any realistic RFI-contaminated data, this fired for essentially every
    tile that had so much as one bad channel, which is the normal case,
    not an edge case.

    A handful of outlier channels plus modest per-channel phase noise
    (unlike the other two niter tests here, which use either zero noise --
    nothing is ever clipped, so the bug can't surface -- or 95% outlier
    contamination, which breaks out of the loop via the len(mask) < 2 exit
    on the very first iteration, before a second iteration's mismatched
    arrays would ever be reached) is exactly the gap those two didn't cover.
    """
    rng = np.random.default_rng(1)
    target_length = 5.0
    solns = _make_phase_ramp(_FREQS_HZ, length_m=target_length, intercept_rad=0.0)
    noise = rng.normal(scale=0.05, size=len(_FREQS_HZ))
    solns *= np.exp(1j * noise)
    outlier_indices = rng.choice(len(_FREQS_HZ), size=5, replace=False)
    solns[outlier_indices] = np.exp(1j * rng.uniform(-np.pi, np.pi, size=5))
    weights = np.ones(len(_FREQS_HZ))

    # Must not raise, and must still recover a sensible fit.
    result = fit_phase_line(_FREQS_HZ, solns, weights, niter=3)
    assert abs(result.length - target_length) < 0.5, (
        f"Expected length ≈ {target_length}m with realistic clipping, got {result.length:.3f}m"
    )
    # The 5 injected outliers (at minimum) should have been clipped by the
    # sigma-clip across the 3 iterations.
    assert result.quality < 1.0


def test_fit_gain_uniform_amps_inverted():
    """All amps=2.0 → inverted weighted mean = 0.5 for every coarse channel."""
    solns = np.full(_N_CHANBLOCKS, 2.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert len(result.gains) == _N_COARSE
    for g in result.gains:
        assert g == pytest.approx(0.5, abs=1e-6)


def test_fit_gain_weighted_mean_correctness():
    """Manually verify weighted mean of 1/amp matches the output for coarse channel 0."""
    amps = np.array([1.0, 2.0, 4.0, 8.0] * _N_COARSE, dtype=np.complex128)
    weights = np.array([1.0, 2.0, 1.0, 2.0] * _N_COARSE)
    result = fit_gain(_GAIN_FREQS, amps, weights, _CHANBLOCKS_PER_COARSE)
    coarse_amps = np.abs(amps[:_CHANBLOCKS_PER_COARSE])
    coarse_weights = weights[:_CHANBLOCKS_PER_COARSE]
    expected = np.sum((1.0 / coarse_amps) * coarse_weights) / np.sum(coarse_weights)
    assert result.gains[0] == pytest.approx(expected, rel=1e-6)


def test_fit_gain_nan_coarse_channel_skipped():
    """A coarse channel with all-NaN solutions should produce NaN gain for that slot."""
    solns = np.full(_N_CHANBLOCKS, 2.0, dtype=np.complex128)
    start = 5 * _CHANBLOCKS_PER_COARSE
    solns[start : start + _CHANBLOCKS_PER_COARSE] = np.nan
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert np.isnan(result.gains[5]), "NaN coarse channel should produce NaN gain"
    for i, g in enumerate(result.gains):
        if i != 5:
            assert np.isfinite(g), f"Channel {i} should have a finite gain"


def test_fit_gain_too_few_valid_coarse_skipped():
    """Coarse channel with fewer than 2 valid points should remain NaN without raising."""
    solns = np.full(_N_CHANBLOCKS, 2.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    start = 3 * _CHANBLOCKS_PER_COARSE
    weights[start : start + _CHANBLOCKS_PER_COARSE] = 0.0
    weights[start] = 1.0  # exactly 1 valid
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert np.isnan(result.gains[3]), "Channel with <2 valid points should have NaN gain"


def test_fit_gain_quality_always_one():
    """Current implementation always returns quality=1.0."""
    solns = np.full(_N_CHANBLOCKS, 1.5, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert result.quality == pytest.approx(1.0)


def test_fit_gain_output_list_lengths():
    """All output lists must have length == n_coarse."""
    solns = np.full(_N_CHANBLOCKS, 1.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert len(result.gains) == _N_COARSE
    assert len(result.pol0) == _N_COARSE
    assert len(result.pol1) == _N_COARSE
    assert len(result.sigma_resid) == _N_COARSE


def test_fit_gain_pol0_pol1_sigma_resid_zero_for_valid_channels():
    """pol0 should be the polynomial intercept (~1/amplitude), pol1 should be
    ~0 (flat gains), and sigma_resid should be ~0 for perfectly flat data."""
    solns = np.full(_N_CHANBLOCKS, 1.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    for v in result.pol0:
        assert v == pytest.approx(1.0, rel=1e-5)  # intercept ~ 1/1.0
    for v in result.pol1:
        assert v == pytest.approx(0.0, abs=1e-20)  # slope ~ 0 for flat data
    for v in result.sigma_resid:
        assert v == pytest.approx(0.0, abs=1e-10)  # no residuals for perfect fit


def test_fit_gain_length_mismatch_raises():
    """Mismatched array lengths should raise AssertionError."""
    freqs = np.linspace(138e6, 170e6, 10)
    solns = np.ones(11, dtype=np.complex128)  # wrong length
    weights = np.ones(10)
    with pytest.raises(AssertionError):
        fit_gain(freqs, solns, weights, chanblocks_per_coarse=2)


def test_fit_gain_basic_weighted_mean():
    """Gains should be the weighted mean of 1/amplitude per coarse channel."""
    chanblocks_per_coarse = 4
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    # Flat amplitudes of 2.0 -> inverted gains should all be 0.5
    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert len(result.gains) == n_coarse
    for g in result.gains:
        assert g == pytest.approx(0.5, rel=1e-6)


def test_fit_gain_pol0_pol1_flat_amps():
    """With flat amplitudes, the polynomial slope (pol1) should be ~0 and
    intercept (pol0) should be ~0.25 (the inverted amplitude)."""
    chanblocks_per_coarse = 8
    n_coarse = 2
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(150e6, 170e6, n_freqs)
    solns = np.full(n_freqs, 4.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    for i in range(n_coarse):
        assert result.pol0[i] == pytest.approx(0.25, rel=1e-5)  # intercept ~ 1/4.0
        assert result.pol1[i] == pytest.approx(0.0, abs=1e-20)  # slope ~ 0


def test_fit_gain_sigma_resid_flat_amps():
    """With perfectly flat amplitudes, residuals from the poly fit should be ~0."""
    chanblocks_per_coarse = 4
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    for i in range(n_coarse):
        assert result.sigma_resid[i] == pytest.approx(0.0, abs=1e-10)


def test_fit_gain_quality_all_valid():
    """With clean data and no outliers, quality should be 1.0."""
    chanblocks_per_coarse = 4
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert result.quality == pytest.approx(1.0, rel=1e-6)


def test_fit_gain_quality_reduced_by_flagged_channels():
    """Channels with zero weight are excluded from fitting; quality should
    reflect the fraction of all channels within 2*sigma of the fit."""
    chanblocks_per_coarse = 4
    n_coarse = 2
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    # Flag one entire coarse channel by zeroing its weights
    weights[:chanblocks_per_coarse] = 0.0

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    # Flagged coarse channel should produce nan gain
    assert np.isnan(result.gains[0])
    # quality should be < 1.0 since half the channels had no valid data
    assert result.quality < 1.0
    assert 0.0 <= result.quality <= 1.0


def test_fit_gain_nan_solns_skipped():
    """NaN solutions should be masked out; coarse channels with fewer than
    2 valid points should produce nan gains."""
    chanblocks_per_coarse = 4
    n_coarse = 2
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0 + 0j)
    solns[:chanblocks_per_coarse] = np.nan  # entire first coarse channel is NaN
    weights = np.ones(n_freqs)

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert np.isnan(result.gains[0])
    assert not np.isnan(result.gains[1])


def test_fit_gain_output_lengths():
    """All output arrays should have length == n_coarse."""
    chanblocks_per_coarse = 8
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.ones(n_freqs, dtype=complex)
    weights = np.ones(n_freqs)

    result = fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert len(result.gains) == n_coarse
    assert len(result.pol0) == n_coarse
    assert len(result.pol1) == n_coarse
    assert len(result.sigma_resid) == n_coarse


def test_fit_phase_line_recovers_known_length():
    """fit_phase_line should recover a known cable length from a synthetic phase ramp."""
    known_length_m = 10.0
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * known_length_m / speed_of_light.value
    phases = slope * freqs_hz + 0.3  # arbitrary intercept
    solns = np.exp(1j * phases)
    weights = np.ones(len(freqs_hz))

    result = fit_phase_line(freqs_hz, solns, weights)

    assert result.length == pytest.approx(known_length_m, rel=1e-3)


def test_fit_phase_line_recovers_known_intercept():
    """fit_phase_line should recover the phase intercept of a synthetic ramp."""
    known_length_m = 5.0
    known_intercept = 0.7  # radians
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * known_length_m / speed_of_light.value
    phases = slope * freqs_hz + known_intercept
    solns = np.exp(1j * phases)
    weights = np.ones(len(freqs_hz))

    result = fit_phase_line(freqs_hz, solns, weights)

    assert result.intercept == pytest.approx(known_intercept, abs=1e-3)


def test_fit_phase_line_chi2dof_near_one_for_noisy_data():
    """chi2dof should be in a reasonable range for mildly noisy data."""
    rng = np.random.default_rng(42)
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    noise = rng.normal(0, 0.05, len(freqs_hz))
    solns = np.exp(1j * (slope * freqs_hz + 0.1 + noise))
    weights = np.ones(len(freqs_hz))

    result = fit_phase_line(freqs_hz, solns, weights)

    assert 0.0 < result.chi2dof < 10.0


def test_fit_phase_line_sigma_resid_low_for_clean_data():
    """sigma_resid should be near zero for a perfect synthetic phase ramp."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    solns = np.exp(1j * (slope * freqs_hz + 0.1))
    weights = np.ones(len(freqs_hz))

    result = fit_phase_line(freqs_hz, solns, weights)

    assert result.sigma_resid == pytest.approx(0.0, abs=1e-3)


def test_fit_phase_line_quality_reduced_by_outliers():
    """Injecting large phase outliers should reduce quality below 1.0."""
    rng = np.random.default_rng(7)
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    phases = slope * freqs_hz + 0.1
    # Inject obvious outliers into ~25% of channels
    outlier_idx = rng.choice(len(freqs_hz), size=16, replace=False)
    phases[outlier_idx] += np.pi  # flip phase by 180 degrees
    solns = np.exp(1j * phases)
    weights = np.ones(len(freqs_hz))

    result = fit_phase_line(freqs_hz, solns, weights)

    assert result.quality < 1.0
    assert 0.0 <= result.quality <= 1.0


def test_fit_phase_line_stderr_is_positive():
    """stderr should always be a positive finite value for valid input."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    solns = np.exp(1j * (slope * freqs_hz + 0.2))
    weights = np.ones(len(freqs_hz))

    result = fit_phase_line(freqs_hz, solns, weights)

    assert np.isfinite(result.stderr)
    assert result.stderr > 0.0


def test_fit_phase_line_too_few_valid_raises():
    """fit_phase_line should raise RuntimeError if fewer than 2 valid phases exist."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    solns = np.full(64, np.nan + 0j)  # all NaN - no valid phases
    weights = np.ones(64)

    with pytest.raises(RuntimeError, match="Not enough valid phases"):
        fit_phase_line(freqs_hz, solns, weights)


class TestPadGainsToFullCoarse:
    """Tests for the pad_gains_to_full_coarse() helper."""

    def test_no_padding_needed_all_channels_present(self):
        """When actual == expected, values are placed at correct positions unchanged."""
        expected = np.array([100, 101, 102, 103])
        actual = [100, 101, 102, 103]
        values = [1.0, 2.0, 3.0, 4.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == 4
        assert result[0] == pytest.approx(1.0)
        assert result[1] == pytest.approx(2.0)
        assert result[2] == pytest.approx(3.0)
        assert result[3] == pytest.approx(4.0)

    def test_last_channel_missing(self):
        """Missing last channel produces NaN at the final position."""
        expected = np.array([100, 101, 102, 103])
        actual = [100, 101, 102]
        values = [1.0, 2.0, 3.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == 4
        assert result[0] == pytest.approx(1.0)
        assert result[1] == pytest.approx(2.0)
        assert result[2] == pytest.approx(3.0)
        assert np.isnan(result[3])

    def test_first_channel_missing(self):
        """Missing first channel produces NaN at position 0."""
        expected = np.array([100, 101, 102, 103])
        actual = [101, 102, 103]
        values = [2.0, 3.0, 4.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == 4
        assert np.isnan(result[0])
        assert result[1] == pytest.approx(2.0)
        assert result[2] == pytest.approx(3.0)
        assert result[3] == pytest.approx(4.0)

    def test_middle_channel_missing(self):
        """Missing middle channel produces NaN at the correct interior position."""
        expected = np.array([100, 101, 102, 103])
        actual = [100, 102, 103]
        values = [1.0, 3.0, 4.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == 4
        assert result[0] == pytest.approx(1.0)
        assert np.isnan(result[1])
        assert result[2] == pytest.approx(3.0)
        assert result[3] == pytest.approx(4.0)

    def test_multiple_channels_missing(self):
        """Multiple missing channels all become NaN at their respective positions."""
        expected = np.array([100, 101, 102, 103, 104, 105])
        actual = [101, 104]
        values = [10.0, 40.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == 6
        assert np.isnan(result[0])  # ch100 missing
        assert result[1] == pytest.approx(10.0)  # ch101
        assert np.isnan(result[2])  # ch102 missing
        assert np.isnan(result[3])  # ch103 missing
        assert result[4] == pytest.approx(40.0)  # ch104
        assert np.isnan(result[5])  # ch105 missing

    def test_output_length_equals_expected_chans(self):
        """Output length always equals len(expected_chans), not len(actual_chans)."""
        expected = np.array([56, 57, 58, 59, 60, 61, 62, 63])
        actual = [56, 57]
        values = [1.0, 2.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == len(expected)
        assert result[0] == pytest.approx(1.0)
        assert result[1] == pytest.approx(2.0)
        for i in range(2, 8):
            assert np.isnan(result[i])

    def test_nan_values_in_input_are_preserved(self):
        """NaN values already in the input list are placed at their positions."""
        expected = np.array([100, 101, 102])
        actual = [100, 101, 102]
        values = [1.0, np.nan, 3.0]

        result = pad_gains_to_full_coarse(values, actual, expected)

        assert len(result) == 3
        assert result[0] == pytest.approx(1.0)
        assert np.isnan(result[1])
        assert result[2] == pytest.approx(3.0)


class TestPadGainFitInfo:
    """Tests for the pad_gain_fit_info() helper."""

    def _make_gain_fit(self, n: int, value: float = 1.0) -> GainFitInfo:
        """Return a GainFitInfo with n-element arrays filled with *value*."""
        return GainFitInfo(
            quality=0.9,
            gains=[value] * n,
            pol0=[value * 0.1] * n,
            pol1=[value * 0.2] * n,
            sigma_resid=[value * 0.01] * n,
        )

    def test_all_channels_present_is_noop(self):
        """When actual == expected channels, all arrays are returned unchanged."""
        expected = np.array([100, 101, 102])
        actual = [100, 101, 102]
        gf = self._make_gain_fit(3, value=2.5)

        result = pad_gain_fit_info(gf, actual, expected)

        assert len(result.gains) == 3
        assert all(v == pytest.approx(2.5) for v in result.gains)
        assert result.quality == pytest.approx(0.9)

    def test_missing_last_channel_pads_all_arrays(self):
        """All four per-channel arrays are padded consistently for a missing last channel."""
        expected = np.array([100, 101, 102, 103])
        actual = [100, 101, 102]
        gf = self._make_gain_fit(3, value=1.0)

        result = pad_gain_fit_info(gf, actual, expected)

        assert len(result.gains) == 4
        assert len(result.pol0) == 4
        assert len(result.pol1) == 4
        assert len(result.sigma_resid) == 4

        # Last position is NaN in every array
        assert np.isnan(result.gains[3])
        assert np.isnan(result.pol0[3])
        assert np.isnan(result.pol1[3])
        assert np.isnan(result.sigma_resid[3])

        # First three positions have real values
        for i in range(3):
            assert np.isfinite(result.gains[i])
            assert np.isfinite(result.pol0[i])

    def test_quality_scalar_is_preserved(self):
        """The quality scalar is carried through unchanged."""
        expected = np.array([100, 101, 102, 103])
        actual = [100, 101]
        gf = GainFitInfo(
            quality=0.75,
            gains=[1.0, 1.0],
            pol0=[0.1, 0.1],
            pol1=[0.2, 0.2],
            sigma_resid=[0.01, 0.01],
        )

        result = pad_gain_fit_info(gf, actual, expected)

        assert result.quality == pytest.approx(0.75)

    def test_returns_gain_fit_info_instance(self):
        """Return value is a proper GainFitInfo NamedTuple."""
        expected = np.array([100, 101])
        actual = [100]
        gf = self._make_gain_fit(1)

        result = pad_gain_fit_info(gf, actual, expected)

        assert isinstance(result, GainFitInfo)
