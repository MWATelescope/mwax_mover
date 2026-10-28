"""Tests for calvin.imaging.run_imaging_steps(), the imaging steps shared by the processor and the CLI tool."""

import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
from astropy.io import fits

from mwax_mover.calvin.imaging import ImagingSettings, run_imaging_steps
from mwax_mover.calvin.wsclean import WSCleanArgs

OBS = 1234567890


def _settings(**overrides) -> ImagingSettings:
    values: dict[str, Any] = {
        "hyperdrive_binary_path": "/bin/hyperdrive",
        "apply_timeout": 77,
        "wsclean_binary_path": "/bin/wsclean",
        "wsclean_timeout": 99,
        "wsclean_args": WSCleanArgs(name="", temp_dir="", size=(3072, 3072), multiscale=True),
        "wsclean_abs_mem_gib": 737,
        "remove_input_ms": True,
    }
    values.update(overrides)
    return ImagingSettings(**values)


def _dirs(tmp_path: Path, ms_names: list[str]) -> tuple[Path, Path]:
    work = tmp_path / "work"
    out = tmp_path / "out"
    work.mkdir()
    out.mkdir()
    for name in ms_names:
        (work / name).mkdir()
        (work / name / "table.dat").write_text("x")
    return work, out


def _run(work: Path, out: Path, settings: ImagingSettings, on_task=None) -> bool:
    return run_imaging_steps(OBS, "meta.fits", str(out), str(work), settings, on_task)


def test_applies_solutions_then_runs_wsclean_on_the_calibrated_sets(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms", f"{OBS}_ch159.ms", f"{OBS}_ch159_cal.ms"])
    cal_ms = [str(work / f"{OBS}_ch159_cal.ms"), str(work / f"{OBS}_ch75_cal.ms")]
    tasks: list[str] = []

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)) as mock_apply,
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True) as mock_wsclean,
        patch("mwax_mover.calvin.imaging.make_wsclean_preview") as mock_preview,
    ):
        assert _run(work, out, _settings(), tasks.append) is True

    # Only the Birli sets go in, sorted
    assert mock_apply.call_args.args[0] == [str(work / f"{OBS}_ch159.ms"), str(work / f"{OBS}_ch75.ms")]
    assert mock_apply.call_args.args[1:] == ("meta.fits", str(out), str(work), OBS, "/bin/hyperdrive", 77, True)
    wsclean_call = mock_wsclean.call_args.args
    assert wsclean_call[0] == cal_ms
    assert wsclean_call[1:5] == (str(out), OBS, "/bin/wsclean", 99)
    assert wsclean_call[5].name == str(out / f"{OBS}_wsclean")
    assert wsclean_call[5].temp_dir == str(work)
    assert wsclean_call[5].abs_mem_gib == 737
    assert wsclean_call[5].size == (3072, 3072)
    mock_preview.assert_called_once_with(str(out), OBS)
    assert tasks == ["Applying solutions", "WSClean", "Preview PNG"]


def test_does_not_change_the_settings_it_was_given(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])
    settings = _settings()

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
        patch("mwax_mover.calvin.imaging.make_wsclean_preview"),
    ):
        _run(work, out, settings)

    assert settings.wsclean_args.name == ""
    assert settings.wsclean_args.abs_mem_gib is None


def test_wsclean_does_not_run_if_solutions_apply_fails(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(False, [])),
        patch("mwax_mover.calvin.imaging.run_wsclean") as mock_wsclean,
    ):
        assert _run(work, out, _settings()) is False

    mock_wsclean.assert_not_called()


def test_no_preview_if_wsclean_fails(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=False),
        patch("mwax_mover.calvin.imaging.make_wsclean_preview") as mock_preview,
    ):
        assert _run(work, out, _settings()) is False

    mock_preview.assert_not_called()


def test_skipped_before_solutions_apply_if_no_memory_is_left_for_wsclean(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])

    with patch("mwax_mover.calvin.imaging.run_solutions_apply") as mock_apply:
        assert _run(work, out, _settings(wsclean_abs_mem_gib=0)) is False

    mock_apply.assert_not_called()


def test_no_memory_limit_is_allowed(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True) as mock_wsclean,
        patch("mwax_mover.calvin.imaging.make_wsclean_preview"),
    ):
        assert _run(work, out, _settings(wsclean_abs_mem_gib=None)) is True

    assert mock_wsclean.call_args.args[5].abs_mem_gib is None


def test_skipped_if_there_are_no_birli_measurement_sets(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75_cal.ms"])  # only a calibrated set

    with patch("mwax_mover.calvin.imaging.run_solutions_apply") as mock_apply:
        assert _run(work, out, _settings()) is False

    mock_apply.assert_not_called()


def test_unexpected_exceptions_are_logged_not_raised(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])

    with patch("mwax_mover.calvin.imaging.run_solutions_apply", side_effect=RuntimeError("boom")):
        assert _run(work, out, _settings()) is False


def test_a_failed_preview_does_not_fail_the_imaging(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])
    # There is a real image, but it is not valid, so the real preview code fails and logs
    (out / f"{OBS}_wsclean-MFS-image.fits").write_text("not a fits file")

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
    ):
        assert _run(work, out, _settings()) is True

    assert not (out / f"{OBS}_wsclean-MFS-image.png").exists()


def test_makes_the_real_preview_png_after_wsclean_works(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])
    fits.PrimaryHDU(np.random.default_rng(1).normal(0.0, 0.01, (1, 1, 32, 32)).astype(np.float32)).writeto(
        out / f"{OBS}_wsclean-MFS-image.fits"
    )

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
    ):
        assert _run(work, out, _settings()) is True

    assert os.path.exists(out / f"{OBS}_wsclean-MFS-image.png")


def test_per_channel_images_are_removed_and_mfs_images_are_kept(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])
    fits.PrimaryHDU(np.zeros((1, 1, 8, 8), dtype=np.float32)).writeto(out / f"{OBS}_wsclean-MFS-image.fits")
    for name in (f"{OBS}_wsclean-MFS-dirty.fits", f"{OBS}_wsclean-0000-image.fits", f"{OBS}_wsclean-0003-dirty.fits"):
        (out / name).write_text("x")

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
    ):
        assert _run(work, out, _settings()) is True

    assert (out / f"{OBS}_wsclean-MFS-image.fits").exists()
    assert (out / f"{OBS}_wsclean-MFS-dirty.fits").exists()
    assert (out / f"{OBS}_wsclean-MFS-image.png").exists()
    assert not (out / f"{OBS}_wsclean-0000-image.fits").exists()
    assert not (out / f"{OBS}_wsclean-0003-dirty.fits").exists()


def test_per_channel_images_are_kept_if_wsclean_fails(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])
    (out / f"{OBS}_wsclean-MFS-image.fits").write_text("x")
    (out / f"{OBS}_wsclean-0000-image.fits").write_text("x")

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=False),
    ):
        assert _run(work, out, _settings()) is False

    assert (out / f"{OBS}_wsclean-0000-image.fits").exists()


def test_reuse_calibrated_images_the_calibrated_sets_and_does_not_apply_solutions(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms", f"{OBS}_ch75_cal.ms", f"{OBS}_ch159_cal.ms"])
    tasks: list[str] = []

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply") as mock_apply,
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True) as mock_wsclean,
        patch("mwax_mover.calvin.imaging.make_wsclean_preview") as mock_preview,
    ):
        assert _run(work, out, _settings(reuse_calibrated=True), tasks.append) is True

    mock_apply.assert_not_called()
    # Only the calibrated sets go to WSClean, sorted
    assert mock_wsclean.call_args.args[0] == [str(work / f"{OBS}_ch159_cal.ms"), str(work / f"{OBS}_ch75_cal.ms")]
    assert mock_wsclean.call_args.args[5].abs_mem_gib == 737
    mock_preview.assert_called_once_with(str(out), OBS)
    assert tasks == ["WSClean", "Preview PNG"]


def test_reuse_calibrated_does_not_remove_anything(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms", f"{OBS}_ch75_cal.ms"])

    with (
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
        patch("mwax_mover.calvin.imaging.make_wsclean_preview"),
    ):
        assert _run(work, out, _settings(reuse_calibrated=True, remove_input_ms=True)) is True

    assert (work / f"{OBS}_ch75.ms").is_dir()
    assert (work / f"{OBS}_ch75_cal.ms").is_dir()


def test_reuse_calibrated_fails_without_calibrated_sets(tmp_path):
    work, out = _dirs(tmp_path, [f"{OBS}_ch75.ms"])  # only a Birli set

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply") as mock_apply,
        patch("mwax_mover.calvin.imaging.run_wsclean") as mock_wsclean,
    ):
        assert _run(work, out, _settings(reuse_calibrated=True)) is False

    mock_apply.assert_not_called()
    mock_wsclean.assert_not_called()


def test_reuse_calibrated_is_off_by_default():
    assert _settings().reuse_calibrated is False
