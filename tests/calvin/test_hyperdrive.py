"""Tests for calvin.hyperdrive's run/stats functions.

Covers estimate_di_calibrate_peak_ram_bytes, _uvfits_num_coarse_chans, and
_max_hyperdrive_workers -- the memory-estimation and concurrency-sizing
support for parallelising run_hyperdrive across picket-fence bands -- plus
run_hyperdrive itself now that it's wired up (see
docs/HYPERDRIVE_PARALLELISM.md Phases 2-4). write_hyperdrive_stats/
get_convergence_summary still have no test coverage.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import mwalib
import pytest
from astropy.io import fits
from tests_common import obs_metafits_path

from mwax_mover.calvin.hyperdrive import (
    _max_hyperdrive_workers,
    _uvfits_num_coarse_chans,
    estimate_di_calibrate_peak_ram_bytes,
    run_hyperdrive,
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
    num_coarse_chan = 1

    result = estimate_di_calibrate_peak_ram_bytes(metafits_context, edge_width_hz, num_sources, num_coarse_chan)

    # Manual calculation mirroring the function's own formula, using this
    # fixture's real metafits values (num_corr_fine_chans_per_coarse=128,
    # corr_fine_chan_width_hz=10000, num_metafits_timesteps=120,
    # 127 unflagged X-pol tiles).
    n_points = num_sources
    n_gaussians = num_sources // 4
    n_shapelets = 0  # no shapelets in the current skymodels -- matches the function's own hardcoded 0
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

    assert result == expected == 7_322_272_256


def test_estimate_di_calibrate_peak_ram_bytes_scales_with_coarse_channels(metafits_context):
    """Doubling the coarse-channel count roughly doubles the estimate.

    Not exactly double -- n_unique_beam_freqs and n_chanblocks both scale
    with n_coarse_channels, so the total scales faster than linear -- but
    it must at least increase, and the wider band's estimate must exceed
    the sum of two single-channel estimates' chanblock-dependent terms
    scaling correctly. Kept simple: just assert strictly increasing.
    """
    one_channel = estimate_di_calibrate_peak_ram_bytes(metafits_context, 80000, 1000, 1)
    four_channels = estimate_di_calibrate_peak_ram_bytes(metafits_context, 80000, 1000, 4)

    assert four_channels > one_channel


def test_estimate_di_calibrate_peak_ram_bytes_zero_sources(metafits_context):
    """num_sources=0 still returns a sane (non-negative, non-zero) estimate.

    The beam-response-cache and vis-array terms are independent of source
    count, so the estimate should not collapse to zero.
    """
    result = estimate_di_calibrate_peak_ram_bytes(metafits_context, 80000, 0, 1)

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


# ===========================================================================
# run_hyperdrive
# ===========================================================================


def test_run_hyperdrive_runs_every_picket_regardless_of_earlier_failures(tmp_path):
    """Every picket runs even if an earlier one (by input order) fails.

    Unlike the old serial implementation (which stopped at the first
    failure), every picket must be attempted, and only the aggregate result
    (not all succeeded) triggers the error-dir move + readme_error.txt.

    Uses non-FITS placeholder uvfits files, so the per-picket memory
    estimate can't be computed and run_hyperdrive falls back to serial
    (workers=1) -- this also exercises that fallback path. Serial execution
    still must not stop early, since that behaviour comes from removing the
    old loop's `break`, not from concurrency itself.
    """
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()

    uvfits_files = []
    for i in range(4):
        f = tmp_path / f"1234567890_ch{i}.uvfits"
        f.write_text("not a real fits file")
        uvfits_files.append(str(f))

    # Picket 0 fails; the other three succeed. Proves later pickets still
    # run even though (by input order) an earlier one failed.
    exit_codes = {uvfits_files[0]: 1, uvfits_files[1]: 0, uvfits_files[2]: 0, uvfits_files[3]: 0}
    attempted = []

    def fake_start_command(cmdline, *args, **kwargs):
        return cmdline  # the "popen process" placeholder is just the cmdline string

    def fake_check_popen_finished(popen_process, timeout):
        matching = next(f for f in uvfits_files if f in popen_process)
        attempted.append(matching)
        return exit_codes[matching], "stdout", "stderr"

    with (
        patch("mwax_mover.calvin.hyperdrive.start_command", side_effect=fake_start_command),
        patch("mwax_mover.calvin.hyperdrive.check_popen_finished", side_effect=fake_check_popen_finished),
    ):
        success, calibration_command = run_hyperdrive(
            uvfits_files,
            "fake_metafits.fits",
            MagicMock(),  # metafits_context -- never reached; the memory estimate fails first
            str(job_output_path),
            1234567890,
            "/bin/hyperdrive",
            "srclist.txt",
            "gleam",
            500,
            60,
            "",
            80000,
        )

    # All four pickets were attempted, not just up to the first failure.
    assert sorted(attempted) == sorted(uvfits_files)
    assert success is False
    assert "--num-sources 500" in calibration_command

    # The three successful pickets each got their own readme; picket 0 (the
    # failure) did not, since that write only happens on the success path.
    for i in (1, 2, 3):
        assert (job_output_path / f"1234567890_ch{i}_hyperdrive_readme.txt").exists()
    assert not (job_output_path / "1234567890_ch0_hyperdrive_readme.txt").exists()

    # Aggregate failure: every uvfits file moved to the error dir, one
    # combined readme_error.txt written.
    for i in range(4):
        assert (job_output_path / f"1234567890_ch{i}.uvfits").exists()
        assert not Path(uvfits_files[i]).exists()
    assert (job_output_path / "readme_error.txt").exists()


def test_run_hyperdrive_all_succeed_no_error_dir(tmp_path):
    """When every picket succeeds, nothing is moved and no readme_error.txt is written."""
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()

    uvfits_files = []
    for i in range(2):
        f = tmp_path / f"1234567890_ch{i}.uvfits"
        f.write_text("not a real fits file")
        uvfits_files.append(str(f))

    with (
        patch("mwax_mover.calvin.hyperdrive.start_command", return_value="popen"),
        patch("mwax_mover.calvin.hyperdrive.check_popen_finished", return_value=(0, "stdout", "stderr")),
    ):
        success, calibration_command = run_hyperdrive(
            uvfits_files,
            "fake_metafits.fits",
            MagicMock(),
            str(job_output_path),
            1234567890,
            "/bin/hyperdrive",
            "srclist.txt",
            "gleam",
            500,
            60,
            "",
            80000,
        )

    assert success is True
    assert not (job_output_path / "readme_error.txt").exists()
    for uvfits_file in uvfits_files:
        assert Path(uvfits_file).exists()  # not moved


def test_run_hyperdrive_empty_input_is_vacuously_successful():
    """An empty uvfits-file list returns success with no calibration command."""
    success, calibration_command = run_hyperdrive(
        [],
        "fake_metafits.fits",
        MagicMock(),
        "/tmp/nonexistent",
        1234567890,
        "/bin/hyperdrive",
        "srclist.txt",
        "gleam",
        500,
        60,
        "",
        80000,
    )

    assert success is True
    assert calibration_command == ""


def test_run_hyperdrive_worker_count_comes_from_max_hyperdrive_workers(tmp_path):
    """The ThreadPoolExecutor is actually sized by _max_hyperdrive_workers's decision.

    Mocks the whole memory-estimate chain so the wiring can be checked
    directly, without depending on real uvfits/metafits fixtures.
    """
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()
    uvfits_files = [str(tmp_path / f"1234567890_ch{i}.uvfits") for i in range(3)]
    for f in uvfits_files:
        Path(f).write_text("not a real fits file")

    with (
        patch("mwax_mover.calvin.hyperdrive._uvfits_num_coarse_chans", return_value=1),
        patch("mwax_mover.calvin.hyperdrive.estimate_di_calibrate_peak_ram_bytes", return_value=1000),
        patch("mwax_mover.calvin.hyperdrive._max_hyperdrive_workers", return_value=3) as mock_max_workers,
        patch("mwax_mover.calvin.hyperdrive.ThreadPoolExecutor", wraps=ThreadPoolExecutor) as mock_executor,
        patch("mwax_mover.calvin.hyperdrive.start_command", return_value="popen"),
        patch("mwax_mover.calvin.hyperdrive.check_popen_finished", return_value=(0, "stdout", "stderr")),
    ):
        run_hyperdrive(
            uvfits_files,
            "fake_metafits.fits",
            MagicMock(),
            str(job_output_path),
            1234567890,
            "/bin/hyperdrive",
            "srclist.txt",
            "gleam",
            500,
            60,
            "",
            80000,
        )

    mock_max_workers.assert_called_once_with([1000, 1000, 1000])
    mock_executor.assert_called_once_with(max_workers=3)
