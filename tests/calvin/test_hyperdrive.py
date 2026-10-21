"""Tests for calvin.hyperdrive's run/stats functions.

Covers estimate_di_calibrate_peak_ram_bytes, estimate_di_calibrate_peak_gpu_bytes,
_ms_num_coarse_chans, and _max_hyperdrive_workers -- the memory-estimation
and concurrency-sizing support for parallelising run_hyperdrive across
picket-fence bands -- plus run_hyperdrive itself now that it's wired up.
write_hyperdrive_stats/get_convergence_summary still have no test coverage.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import mwalib
import pytest
from tests_common import obs_metafits_path

from mwax_mover.calvin.hyperdrive import (
    _max_hyperdrive_workers,
    _ms_num_coarse_chans,
    estimate_di_calibrate_peak_gpu_bytes,
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
# estimate_di_calibrate_peak_gpu_bytes
# ===========================================================================


def test_estimate_di_calibrate_peak_gpu_bytes_matches_manual_calculation(metafits_context):
    """Regression test for the GPU estimate, mirroring the host-RAM one above.

    Uses this fixture's real metafits values (same fixture/values as
    test_estimate_di_calibrate_peak_ram_bytes_matches_manual_calculation).
    """
    num_sources = 1000
    edge_width_hz = 80000
    num_coarse_chan = 1

    result = estimate_di_calibrate_peak_gpu_bytes(metafits_context, edge_width_hz, num_sources, num_coarse_chan)

    n_points = num_sources
    n_gaussians = num_sources // 4
    n_unflagged_tiles = 127
    n_cross_baselines = n_unflagged_tiles * (n_unflagged_tiles - 1) // 2
    n_coarse_channels = 1
    n_chanblocks = n_coarse_channels * (128 - 2 * (80000 // 10000))

    point_bytes_per_component = 24 + 64 + 8  # LMN + JonesF64 flux + f64 spectral index
    gaussian_bytes_per_component = point_bytes_per_component + 24  # + GaussianParams
    sky_model_gpu_bytes = n_points * point_bytes_per_component + n_gaussians * gaussian_bytes_per_component
    transient_gpu_bytes = n_chanblocks * n_cross_baselines * 32  # Jones<f32>, always, regardless of gpu-single
    expected = (2905 * 1024**2) + sky_model_gpu_bytes + transient_gpu_bytes

    assert result == expected


def test_estimate_di_calibrate_peak_gpu_bytes_scales_with_coarse_channels(metafits_context):
    """Doubling the coarse-channel count increases the GPU estimate (transient term).

    Unlike the host RAM estimate, the resident fixed baseline dominates and
    doesn't scale with coarse-channel count at all -- only the small
    transient per-timestep buffer term does -- but the total must still
    strictly increase.
    """
    one_channel = estimate_di_calibrate_peak_gpu_bytes(metafits_context, 80000, 1000, 1)
    four_channels = estimate_di_calibrate_peak_gpu_bytes(metafits_context, 80000, 1000, 4)

    assert four_channels > one_channel


def test_estimate_di_calibrate_peak_gpu_bytes_zero_sources(metafits_context):
    """num_sources=0 still returns a sane, non-zero estimate.

    The resident fixed baseline (CUDA context + hyperbeam upload) is
    independent of source count, so the estimate should not collapse to
    zero even with an empty sky model.
    """
    result = estimate_di_calibrate_peak_gpu_bytes(metafits_context, 80000, 0, 1)

    assert result > 0


def test_estimate_di_calibrate_peak_gpu_bytes_dominated_by_resident_baseline(metafits_context):
    """The empirically-measured resident baseline dwarfs the sky-model term.

    At a realistic --num-sources (a few hundred to a few thousand), the
    sky-model contribution is at most a few MB -- nowhere near the ~2.8GB
    resident baseline. Guards against a units/scaling mistake that would
    make the sky-model term unrealistically large.
    """
    result = estimate_di_calibrate_peak_gpu_bytes(metafits_context, 80000, 2000, 1)

    assert result < 2 * (2905 * 1024**2)


# ===========================================================================
# _ms_num_coarse_chans
# ===========================================================================


def test_ms_num_coarse_chans_single_channel_suffix(metafits_context):
    """Birli names a one coarse channel band _chN.ms."""
    assert _ms_num_coarse_chans("/data/jobs/1244973688_ch75.ms", metafits_context) == 1


def test_ms_num_coarse_chans_channel_range_suffix_is_inclusive(metafits_context):
    """_chN-M covers receiver channels N to M inclusive."""
    assert _ms_num_coarse_chans("/data/jobs/1244973688_ch75-76.ms", metafits_context) == 2
    assert _ms_num_coarse_chans("/data/jobs/1244973688_ch110-121.ms", metafits_context) == 12


def test_ms_num_coarse_chans_no_suffix_uses_metafits_count(metafits_context):
    """No suffix means the whole observation, i.e. every coarse channel in the metafits."""
    assert metafits_context.num_metafits_coarse_chans == 24  # guards the fixture, so this test stays meaningful

    assert _ms_num_coarse_chans("/data/jobs/1244973688.ms", metafits_context) == 24


# ===========================================================================
# _max_hyperdrive_workers
# ===========================================================================

# Deliberately tiny/huge placeholder GPU-bytes lists in the host-memory-only
# tests below (well under any real per-run estimate) so the new GPU-memory
# and fixed-ceiling caps never bind -- these tests are unchanged in intent
# from before this function also considered the GPU, just satisfying the
# now-required second argument.
_TRIVIAL_GPU_BYTES = [1] * 10


def test_max_hyperdrive_workers_exact_fit():
    """Available memory exactly n times the worst-case run allows n workers."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=1000):
        # Budget after 15% headroom: 850. 3 pickets at 850/3 rounds down to 283 each -- but
        # worst_case here is fixed at 200, so budget // worst_case = 850 // 200 = 4, capped
        # by len(per_run_host_bytes) = 3.
        workers = _max_hyperdrive_workers([200, 200, 200], _TRIVIAL_GPU_BYTES[:3])

    assert workers == 3


def test_max_hyperdrive_workers_rounds_down():
    """Memory that doesn't divide evenly rounds down, never up."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=1000):
        # Budget: 850. worst_case: 300. 850 // 300 == 2, not 3, even though 3 pickets exist.
        workers = _max_hyperdrive_workers([300, 300, 300], _TRIVIAL_GPU_BYTES[:3])

    assert workers == 2


def test_max_hyperdrive_workers_single_picket():
    """A single picket (non-picket-fence observation) always gets exactly 1 worker."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=10**12):
        workers = _max_hyperdrive_workers([1_000_000], _TRIVIAL_GPU_BYTES[:1])

    assert workers == 1


def test_max_hyperdrive_workers_unknown_memory_falls_back():
    """available_memory_bytes() returning None uses the fallback constant."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=None):
        workers = _max_hyperdrive_workers([1, 2, 3, 4, 5], _TRIVIAL_GPU_BYTES[:5])

    assert workers == 1  # HYPERDRIVE_FALLBACK_WORKERS


def test_max_hyperdrive_workers_worst_case_dominates():
    """One large picket among several small ones caps everything, per-run.

    Sizing against the average or the smallest picket would overcommit --
    every concurrent slot must be able to fit the largest picket, since any
    of them could land in any slot.
    """
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=1000):
        # Budget: 850. worst_case: 850 (one huge picket). 850 // 850 == 1.
        workers = _max_hyperdrive_workers([10, 10, 10, 850], _TRIVIAL_GPU_BYTES[:4])

    assert workers == 1


def test_max_hyperdrive_workers_never_exceeds_picket_count():
    """Plenty of memory still caps workers at the number of pickets -- no idle workers."""
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=10**15):
        workers = _max_hyperdrive_workers([100, 100], _TRIVIAL_GPU_BYTES[:2])

    assert workers == 2


def test_max_hyperdrive_workers_gpu_memory_dominates():
    """Generous host RAM doesn't help if the shared GPU's memory is the tight one.

    Host budget is effectively unlimited; each picket's GPU estimate (20GB)
    means only 2 fit in the A40's ~40.8GB post-headroom budget
    (48GB * 0.85 // 20GB == 2), even though 3 pickets exist and host memory
    would allow all 3.
    """
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=10**15):
        workers = _max_hyperdrive_workers([10, 10, 10], [20_000_000_000, 20_000_000_000, 20_000_000_000])

    assert workers == 2


def test_max_hyperdrive_workers_fixed_ceiling_dominates():
    """HYPERDRIVE_MAX_CONCURRENT_GPU_WORKERS caps concurrency even when memory allows more.

    Empirically-measured GPU-compute contention, not memory, is the binding
    constraint here: both host RAM and GPU memory are generous enough to fit
    every one of 8 pickets, but the fixed ceiling still caps it at 6.
    """
    with patch("mwax_mover.calvin.hyperdrive.available_memory_bytes", return_value=10**15):
        workers = _max_hyperdrive_workers([1] * 8, [1] * 8)

    assert workers == 6  # HYPERDRIVE_MAX_CONCURRENT_GPU_WORKERS


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
        success, _calibration_command = run_hyperdrive(
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
    directly, without depending on real measurement set/metafits fixtures.
    """
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()
    uvfits_files = [str(tmp_path / f"1234567890_ch{i}.uvfits") for i in range(3)]
    for f in uvfits_files:
        Path(f).write_text("not a real fits file")

    with (
        patch("mwax_mover.calvin.hyperdrive._ms_num_coarse_chans", return_value=1),
        patch("mwax_mover.calvin.hyperdrive.estimate_di_calibrate_peak_ram_bytes", return_value=1000),
        patch("mwax_mover.calvin.hyperdrive.estimate_di_calibrate_peak_gpu_bytes", return_value=2000),
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

    mock_max_workers.assert_called_once_with([1000, 1000, 1000], [2000, 2000, 2000])
    mock_executor.assert_called_once_with(max_workers=3)


def _make_ms_dirs(parent: Path, names: list[str]) -> list[str]:
    """Create placeholder CASA measurement set directories (a dir with a file inside)."""
    ms_dirs = []
    for name in names:
        ms_dir = parent / name
        ms_dir.mkdir()
        (ms_dir / "table.dat").write_text("placeholder")
        ms_dirs.append(str(ms_dir))
    return ms_dirs


def test_run_hyperdrive_names_outputs_after_the_ms_band(tmp_path):
    """Solutions and readme are named from the .ms name, and hyperdrive is given the .ms directory."""
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()
    ms_dirs = _make_ms_dirs(tmp_path, ["1234567890_ch75-76.ms", "1234567890_ch159.ms"])
    cmdlines = []

    def fake_start_command(cmdline, *args, **kwargs):
        cmdlines.append(cmdline)
        return "popen"

    with (
        patch("mwax_mover.calvin.hyperdrive.start_command", side_effect=fake_start_command),
        patch("mwax_mover.calvin.hyperdrive.check_popen_finished", return_value=(0, "stdout", "stderr")),
    ):
        success, _calibration_command = run_hyperdrive(
            ms_dirs,
            "fake_metafits.fits",
            MagicMock(),  # memory estimate fails on this, so it runs serially
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
    assert (job_output_path / "1234567890_ch75-76_hyperdrive_readme.txt").exists()
    assert (job_output_path / "1234567890_ch159_hyperdrive_readme.txt").exists()
    assert any(f"--data {ms_dirs[0]} " in c and "1234567890_ch75-76_solutions.fits" in c for c in cmdlines)
    assert any(f"--data {ms_dirs[1]} " in c and "1234567890_ch159_solutions.fits" in c for c in cmdlines)


def test_run_hyperdrive_failure_moves_ms_directories_with_their_contents(tmp_path):
    """On failure the measurement set directories are moved whole into the job output dir."""
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()
    working_path = tmp_path / "working"
    working_path.mkdir()
    ms_dirs = _make_ms_dirs(working_path, ["1234567890_ch75-76.ms", "1234567890_ch159.ms"])

    with (
        patch("mwax_mover.calvin.hyperdrive.start_command", return_value="popen"),
        patch("mwax_mover.calvin.hyperdrive.check_popen_finished", return_value=(1, "stdout", "stderr")),
    ):
        success, _calibration_command = run_hyperdrive(
            ms_dirs,
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

    assert success is False
    for name in ("1234567890_ch75-76.ms", "1234567890_ch159.ms"):
        assert (job_output_path / name / "table.dat").read_text() == "placeholder"
        assert not (working_path / name).exists()
    assert (job_output_path / "readme_error.txt").exists()


def test_run_hyperdrive_failure_leaves_ms_already_in_job_output_path(tmp_path):
    """Large observations have Birli write straight into the job output dir.

    On failure those measurement sets are already where they would be moved
    to, and shutil.move() raises when the destination exists. The failure
    path must not raise, and must not lose the data.
    """
    job_output_path = tmp_path / "output"
    job_output_path.mkdir()
    ms_dirs = _make_ms_dirs(job_output_path, ["1234567890_ch75-76.ms", "1234567890_ch159.ms"])

    with (
        patch("mwax_mover.calvin.hyperdrive.start_command", return_value="popen"),
        patch("mwax_mover.calvin.hyperdrive.check_popen_finished", return_value=(1, "stdout", "stderr")),
    ):
        success, _calibration_command = run_hyperdrive(
            ms_dirs,
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

    assert success is False
    for ms_dir in ms_dirs:
        assert (Path(ms_dir) / "table.dat").read_text() == "placeholder"
    assert (job_output_path / "readme_error.txt").exists()
