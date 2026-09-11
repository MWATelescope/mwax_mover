"""Tests for calvin.hyperdrive's run/stats functions.

Covers estimate_di_calibrate_peak_ram_bytes and _uvfits_num_coarse_chans,
the memory-estimation support added for parallelising run_hyperdrive across
picket-fence bands (see docs/HYPERDRIVE_PARALLELISM.md Phases 2-4).
run_hyperdrive/write_hyperdrive_stats/get_convergence_summary themselves
have no test coverage yet -- see that plan document for why.
"""

from unittest.mock import patch

import mwalib
import pytest
from astropy.io import fits

from tests_common import obs_metafits_path

from mwax_mover.calvin.hyperdrive import (
    _max_hyperdrive_workers,
    _uvfits_num_coarse_chans,
    estimate_di_calibrate_peak_ram_bytes,
)

# Same fixture calvin/test_birli.py uses for its own memory-estimate test.
TEST_METAFITS_PATH = obs_metafits_path(1244973688)


@pytest.fixture
def metafits_context() -> mwalib.MetafitsContext:
    return mwalib.MetafitsContext(TEST_METAFITS_PATH, None)


# ===========================================================================
# estimate_di_calibrate_peak_ram_bytes
# ===========================================================================


def test_estimate_di_calibrate_peak_ram_bytes_matches_manual_calculation(metafits_context):
    """Regression test for the estimate, including the n_points bug fix.

    n_points must equal num_sources exactly (not num_sources // anything) --
    the original code had a typo (`//` where a `#` comment was intended)
    that would have silently divided it. This test fails if that typo
    reappears.
    """
    num_sources = 1000
    edge_width_hz = 80000
    coarse_chan_start = 1
    coarse_chan_end = 1

    result = estimate_di_calibrate_peak_ram_bytes(
        metafits_context, edge_width_hz, num_sources, coarse_chan_start, coarse_chan_end
    )

    # Manual calculation mirroring the function's own formula, using this
    # fixture's real metafits values (num_corr_fine_chans_per_coarse=128,
    # corr_fine_chan_width_hz=10000, num_metafits_timesteps=120,
    # 127 unflagged X-pol tiles).
    n_points = num_sources
    n_gaussians = num_sources // 4
    n_shapelets = num_sources // 8
    n_unflagged_tiles = 127
    n_cross_baselines = n_unflagged_tiles * (n_unflagged_tiles - 1) // 2
    n_coarse_channels = 1
    n_chanblocks = n_coarse_channels * (128 - 2 * (80000 // 10000))
    n_timesteps = 120
    n_unique_beam_freqs = 2 * n_coarse_channels
    n_components_total = n_points + n_gaussians + n_shapelets
    n_components_max = max(n_points, n_gaussians, n_shapelets)

    vis_arrays = n_timesteps * n_chanblocks * n_cross_baselines * (2 * 32 + 4)
    sky_model_components = n_chanblocks * n_components_total * 64
    beam_response_cache = n_unique_beam_freqs * n_components_max * 64
    solutions_array = n_unflagged_tiles * n_chanblocks * 64
    expected = vis_arrays + sky_model_components + beam_response_cache + solutions_array

    assert result == expected == 7_323_168_256


def test_estimate_di_calibrate_peak_ram_bytes_scales_with_coarse_channels(metafits_context):
    """Doubling the coarse-channel count roughly doubles the estimate.

    Not exactly double -- n_unique_beam_freqs and n_chanblocks both scale
    with n_coarse_channels, so the total scales faster than linear -- but
    it must at least increase, and the wider band's estimate must exceed
    the sum of two single-channel estimates' chanblock-dependent terms
    scaling correctly. Kept simple: just assert strictly increasing.
    """
    one_channel = estimate_di_calibrate_peak_ram_bytes(metafits_context, 80000, 1000, 1, 1)
    four_channels = estimate_di_calibrate_peak_ram_bytes(metafits_context, 80000, 1000, 1, 4)

    assert four_channels > one_channel


def test_estimate_di_calibrate_peak_ram_bytes_zero_sources(metafits_context):
    """num_sources=0 still returns a sane (non-negative, non-zero) estimate.

    The beam-response-cache and vis-array terms are independent of source
    count, so the estimate should not collapse to zero.
    """
    result = estimate_di_calibrate_peak_ram_bytes(metafits_context, 80000, 0, 1, 1)

    assert result > 0


# ===========================================================================
# _uvfits_num_coarse_chans
# ===========================================================================


def _header_with_freq_axis(freq_axis: int, naxis_freq: int, cdelt_freq: float, total_naxis: int) -> fits.Header:
    """Build a minimal synthetic uvfits-style primary HDU header.

    Only the keys _uvfits_num_coarse_chans actually reads: NAXIS, the FREQ
    axis's own CTYPE/NAXIS/CDELT, and enough other CTYPEn placeholders that
    the FREQ-axis scan has non-FREQ axes to skip past (matching a real
    uvfits file's COMPLEX/STOKES/FREQ/RA/DEC axis layout).
    """
    header = fits.Header()
    header["NAXIS"] = total_naxis
    for i in range(2, total_naxis + 1):
        if i == freq_axis:
            header[f"CTYPE{i}"] = "FREQ"
            header[f"NAXIS{i}"] = naxis_freq
            header[f"CDELT{i}"] = cdelt_freq
        else:
            header[f"CTYPE{i}"] = "STOKES" if i == 3 else "COMPLEX"
            header[f"NAXIS{i}"] = 1
            header[f"CDELT{i}"] = 1.0
    return header


def test_uvfits_num_coarse_chans_matches_real_header(metafits_context):
    """Verified against a real Birli-produced uvfits header (1061316544.uvfits).

    NAXIS4=32, CDELT4=40000 Hz -> 1.28 MHz bandwidth == exactly one MWA
    coarse channel. This fixture's own metafits has coarse_chan_width_hz
    of 1,280,000 (the fixed MWA value), matching the real file's band.
    """
    header = _header_with_freq_axis(freq_axis=4, naxis_freq=32, cdelt_freq=40000.0, total_naxis=6)

    with patch("mwax_mover.calvin.hyperdrive.fits.getheader", return_value=header):
        result = _uvfits_num_coarse_chans("fake.uvfits", metafits_context)

    assert result == 1


def test_uvfits_num_coarse_chans_freq_axis_not_hardcoded(metafits_context):
    """The FREQ axis can be at a different index -- must not assume CTYPE4."""
    header = _header_with_freq_axis(freq_axis=3, naxis_freq=64, cdelt_freq=40000.0, total_naxis=5)

    with patch("mwax_mover.calvin.hyperdrive.fits.getheader", return_value=header):
        result = _uvfits_num_coarse_chans("fake.uvfits", metafits_context)

    # 64 * 40000 = 2,560,000 Hz = 2 coarse channels.
    assert result == 2


def test_uvfits_num_coarse_chans_no_freq_axis_raises(metafits_context):
    """A malformed header with no FREQ axis raises rather than guessing."""
    header = fits.Header()
    header["NAXIS"] = 3
    header["CTYPE2"] = "COMPLEX"
    header["CTYPE3"] = "STOKES"

    with patch("mwax_mover.calvin.hyperdrive.fits.getheader", return_value=header):
        with pytest.raises(StopIteration):
            _uvfits_num_coarse_chans("fake.uvfits", metafits_context)


# ===========================================================================
# _max_hyperdrive_workers
# ===========================================================================


def test_max_hyperdrive_workers_exact_fit():
    """Available memory exactly n times the worst-case run allows n workers."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=1000):
        # Budget after 15% headroom: 850. 3 pickets at 850/3 rounds down to 283 each -- but
        # worst_case here is fixed at 200, so budget // worst_case = 850 // 200 = 4, capped
        # by len(per_run_bytes) = 3.
        workers = _max_hyperdrive_workers([200, 200, 200])

    assert workers == 3


def test_max_hyperdrive_workers_rounds_down():
    """Memory that doesn't divide evenly rounds down, never up."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=1000):
        # Budget: 850. worst_case: 300. 850 // 300 == 2, not 3, even though 3 pickets exist.
        workers = _max_hyperdrive_workers([300, 300, 300])

    assert workers == 2


def test_max_hyperdrive_workers_single_picket():
    """A single picket (non-picket-fence observation) always gets exactly 1 worker."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=10**12):
        workers = _max_hyperdrive_workers([1_000_000])

    assert workers == 1


def test_max_hyperdrive_workers_unknown_memory_falls_back():
    """available_memory_bytes() returning None uses the fallback constant."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=None):
        workers = _max_hyperdrive_workers([1, 2, 3, 4, 5])

    assert workers == 1  # HYPERDRIVE_FALLBACK_WORKERS


def test_max_hyperdrive_workers_worst_case_dominates():
    """One large picket among several small ones caps everything, per-run.

    Sizing against the average or the smallest picket would overcommit --
    every concurrent slot must be able to fit the largest picket, since any
    of them could land in any slot.
    """
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=1000):
        # Budget: 850. worst_case: 850 (one huge picket). 850 // 850 == 1.
        workers = _max_hyperdrive_workers([10, 10, 10, 850])

    assert workers == 1


def test_max_hyperdrive_workers_never_exceeds_picket_count():
    """Plenty of memory still caps workers at the number of pickets -- no idle workers."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=10**15):
        workers = _max_hyperdrive_workers([100, 100])

    assert workers == 2
