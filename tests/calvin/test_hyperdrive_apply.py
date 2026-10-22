"""Tests for calvin.hyperdrive_apply: applying solutions to make calibrated measurement sets."""

from pathlib import Path
from unittest.mock import patch

from mwax_mover.calvin.hyperdrive_apply import run_solutions_apply

OBS_ID = 1234567890


def _make_ms_dirs(parent: Path, names: list[str]) -> list[str]:
    """Create placeholder CASA measurement set directories (a dir with a file inside)."""
    ms_dirs = []
    for name in names:
        ms_dir = parent / name
        ms_dir.mkdir()
        (ms_dir / "table.dat").write_text("placeholder")
        ms_dirs.append(str(ms_dir))
    return ms_dirs


def _make_solutions(job_output_path: Path, stems: list[str]) -> None:
    """Create placeholder solutions files, named like the ones hyperdrive di-calibrate writes."""
    for stem in stems:
        (job_output_path / f"{stem}_solutions.fits").write_text("placeholder")


def _setup(tmp_path: Path, names: list[str]) -> tuple[Path, Path, list[str]]:
    job_output_path = tmp_path / "output"
    working_path = tmp_path / "work"
    job_output_path.mkdir()
    working_path.mkdir()
    ms_dirs = _make_ms_dirs(working_path, names)
    return job_output_path, working_path, ms_dirs


def _run(ms_dirs, job_output_path, working_path, remove_input_ms=False):
    return run_solutions_apply(
        ms_dirs,
        "fake_metafits.fits",
        str(job_output_path),
        str(working_path),
        OBS_ID,
        "/bin/hyperdrive",
        60,
        remove_input_ms,
    )


def test_run_solutions_apply_builds_command_for_each_band(tmp_path):
    """Each band is given its own measurement set and its own (modified) solutions, one after the other."""
    job_output_path, working_path, ms_dirs = _setup(tmp_path, [f"{OBS_ID}_ch75-76.ms", f"{OBS_ID}_ch159.ms"])
    _make_solutions(job_output_path, [f"{OBS_ID}_ch75-76", f"{OBS_ID}_ch159"])
    cmdlines = []

    def fake_start_command(cmdline, *args, **kwargs):
        cmdlines.append(cmdline)
        return "popen"

    with (
        patch("mwax_mover.calvin.hyperdrive_apply.start_command", side_effect=fake_start_command),
        patch("mwax_mover.calvin.hyperdrive_apply.check_popen_finished", return_value=(0, "stdout", "stderr")),
    ):
        success, cal_ms_files = _run(ms_dirs, job_output_path, working_path)

    assert success is True
    assert cal_ms_files == [
        str(working_path / f"{OBS_ID}_ch75-76_cal.ms"),
        str(working_path / f"{OBS_ID}_ch159_cal.ms"),
    ]
    assert len(cmdlines) == 2
    assert cmdlines[0].startswith("/bin/hyperdrive solutions-apply --no-progress-bars")
    assert f"--data {ms_dirs[0]} fake_metafits.fits" in cmdlines[0]
    assert f"--solutions {job_output_path / f'{OBS_ID}_ch75-76_solutions.fits'}" in cmdlines[0]
    assert f"--outputs {cal_ms_files[0]}" in cmdlines[0]
    assert f"--data {ms_dirs[1]} fake_metafits.fits" in cmdlines[1]
    assert f"--solutions {job_output_path / f'{OBS_ID}_ch159_solutions.fits'}" in cmdlines[1]
    assert (job_output_path / f"{OBS_ID}_ch75-76_solutions_apply_readme.txt").exists()
    assert (job_output_path / f"{OBS_ID}_ch159_solutions_apply_readme.txt").exists()


def test_run_solutions_apply_not_picket_fence_uses_obs_id_as_stem(tmp_path):
    job_output_path, working_path, ms_dirs = _setup(tmp_path, [f"{OBS_ID}.ms"])
    _make_solutions(job_output_path, [str(OBS_ID)])

    with (
        patch("mwax_mover.calvin.hyperdrive_apply.start_command", return_value="popen"),
        patch("mwax_mover.calvin.hyperdrive_apply.check_popen_finished", return_value=(0, "", "")),
    ):
        success, cal_ms_files = _run(ms_dirs, job_output_path, working_path)

    assert success is True
    assert cal_ms_files == [str(working_path / f"{OBS_ID}_cal.ms")]


def test_run_solutions_apply_missing_solutions_fails_without_running_hyperdrive(tmp_path):
    job_output_path, working_path, ms_dirs = _setup(tmp_path, [f"{OBS_ID}_ch75-76.ms", f"{OBS_ID}_ch159.ms"])
    _make_solutions(job_output_path, [f"{OBS_ID}_ch159"])  # none for the first band

    with (
        patch("mwax_mover.calvin.hyperdrive_apply.start_command") as mock_start,
        patch("mwax_mover.calvin.hyperdrive_apply.check_popen_finished"),
    ):
        success, cal_ms_files = _run(ms_dirs, job_output_path, working_path)

    assert success is False
    assert cal_ms_files == []
    mock_start.assert_not_called()  # stops at the first band, so the second never runs either


def test_run_solutions_apply_stops_at_first_failed_band_and_removes_partial_output(tmp_path):
    job_output_path, working_path, ms_dirs = _setup(tmp_path, [f"{OBS_ID}_ch75-76.ms", f"{OBS_ID}_ch159.ms"])
    _make_solutions(job_output_path, [f"{OBS_ID}_ch75-76", f"{OBS_ID}_ch159"])
    partial = working_path / f"{OBS_ID}_ch75-76_cal.ms"
    partial.mkdir()
    (partial / "table.dat").write_text("partial")

    with (
        patch("mwax_mover.calvin.hyperdrive_apply.start_command", return_value="popen") as mock_start,
        patch("mwax_mover.calvin.hyperdrive_apply.check_popen_finished", return_value=(1, "stdout", "stderr")),
    ):
        success, cal_ms_files = _run(ms_dirs, job_output_path, working_path, remove_input_ms=True)

    assert success is False
    assert cal_ms_files == []
    assert mock_start.call_count == 1
    assert not partial.exists()
    assert Path(ms_dirs[0]).exists()  # input is kept when apply fails
    assert "This run failed" in (job_output_path / f"{OBS_ID}_ch75-76_solutions_apply_readme.txt").read_text()


def test_run_solutions_apply_removes_input_ms_after_success_only_when_asked(tmp_path):
    job_output_path, working_path, ms_dirs = _setup(tmp_path, [f"{OBS_ID}_ch75-76.ms"])
    _make_solutions(job_output_path, [f"{OBS_ID}_ch75-76"])

    with (
        patch("mwax_mover.calvin.hyperdrive_apply.start_command", return_value="popen"),
        patch("mwax_mover.calvin.hyperdrive_apply.check_popen_finished", return_value=(0, "", "")),
    ):
        _run(ms_dirs, job_output_path, working_path, remove_input_ms=False)
        assert Path(ms_dirs[0]).exists()

        success, _ = _run(ms_dirs, job_output_path, working_path, remove_input_ms=True)

    assert success is True
    assert not Path(ms_dirs[0]).exists()


def test_run_solutions_apply_unhandled_exception_is_a_failure(tmp_path):
    job_output_path, working_path, ms_dirs = _setup(tmp_path, [f"{OBS_ID}.ms"])
    _make_solutions(job_output_path, [str(OBS_ID)])

    with patch("mwax_mover.calvin.hyperdrive_apply.start_command", side_effect=OSError("boom")):
        success, cal_ms_files = _run(ms_dirs, job_output_path, working_path)

    assert success is False
    assert cal_ms_files == []
    assert "boom" in (job_output_path / f"{OBS_ID}_solutions_apply_readme.txt").read_text()


def test_run_solutions_apply_no_bands_is_vacuously_successful(tmp_path):
    job_output_path, working_path, _ = _setup(tmp_path, [])
    assert _run([], job_output_path, working_path) == (True, [])
