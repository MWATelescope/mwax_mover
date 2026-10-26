"""Tests for calvin.wsclean: the WSClean options and running WSClean."""

from pathlib import Path
from unittest.mock import patch

from mwax_mover.calvin.wsclean import WSCleanArgs, remove_wsclean_channel_images, run_wsclean

OBS_ID = 1234567890


def _args(tmp_path: Path) -> WSCleanArgs:
    return WSCleanArgs(name=str(tmp_path / f"{OBS_ID}_wsclean"), temp_dir=str(tmp_path / "work"))


def test_wsclean_args_to_cmdline_args():
    args = WSCleanArgs(name="/out/123_wsclean", temp_dir="/work")
    assert args.to_cmdline_args() == ["-name", "/out/123_wsclean", "-temp-dir", "/work", "-no-update-model-required"]


def test_wsclean_args_can_leave_model_update_on():
    args = WSCleanArgs(name="/out/123_wsclean", temp_dir="/work", no_update_model_required=False)
    assert "-no-update-model-required" not in args.to_cmdline_args()


def test_run_wsclean_gives_each_ms_as_a_separate_argument(tmp_path):
    cmdlines = []

    def fake_start_command(cmdline, *args, **kwargs):
        cmdlines.append(cmdline)
        return "popen"

    with (
        patch("mwax_mover.calvin.wsclean.start_command", side_effect=fake_start_command),
        patch("mwax_mover.calvin.wsclean.check_popen_finished", return_value=(0, "stdout", "stderr")),
    ):
        success = run_wsclean(
            ["/w/a_cal.ms", "/w/b_cal.ms"], str(tmp_path), OBS_ID, "/bin/wsclean", 60, _args(tmp_path)
        )

    assert success is True
    assert cmdlines[0].startswith("/bin/wsclean -name ")
    assert cmdlines[0].endswith("-no-update-model-required /w/a_cal.ms /w/b_cal.ms")
    assert "a_cal.ms,b_cal.ms" not in cmdlines[0]
    assert "This run succeeded" in (tmp_path / f"{OBS_ID}_wsclean_readme.txt").read_text()


def test_run_wsclean_failure_moves_nothing_and_writes_only_the_normal_readme(tmp_path):
    ms_dir = tmp_path / "work" / "a_cal.ms"
    ms_dir.mkdir(parents=True)
    out = tmp_path / "out"
    out.mkdir()

    with (
        patch("mwax_mover.calvin.wsclean.start_command", return_value="popen"),
        patch("mwax_mover.calvin.wsclean.check_popen_finished", return_value=(1, "stdout", "stderr")),
    ):
        success = run_wsclean([str(ms_dir)], str(out), OBS_ID, "/bin/wsclean", 60, _args(tmp_path))

    assert success is False
    assert ms_dir.exists()
    assert not (out / "a_cal.ms").exists()
    assert not (out / "readme_error.txt").exists()
    assert "This run failed" in (out / f"{OBS_ID}_wsclean_readme.txt").read_text()


def test_run_wsclean_unhandled_exception_is_a_failure(tmp_path):
    with patch("mwax_mover.calvin.wsclean.start_command", side_effect=OSError("boom")):
        success = run_wsclean(["/w/a_cal.ms"], str(tmp_path), OBS_ID, "/bin/wsclean", 60, _args(tmp_path))

    assert success is False
    assert "boom" in (tmp_path / f"{OBS_ID}_wsclean_readme.txt").read_text()


def _touch(directory: Path, names: list[str]) -> None:
    for name in names:
        (directory / name).write_text("x")


def test_remove_wsclean_channel_images_keeps_only_the_mfs_files(tmp_path):
    mfs = [f"{OBS_ID}_wsclean-MFS-{kind}.fits" for kind in ("image", "dirty", "model", "residual", "psf")]
    channels = [f"{OBS_ID}_wsclean-000{n}-{kind}.fits" for n in range(4) for kind in ("image", "dirty", "psf")]
    others = [f"{OBS_ID}_wsclean-MFS-image.png", f"{OBS_ID}_wsclean_readme.txt", f"{OBS_ID}_ch75.ms.fits"]
    _touch(tmp_path, mfs + channels + others)

    assert remove_wsclean_channel_images(str(tmp_path), OBS_ID) == len(channels)

    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(mfs + others)


def test_remove_wsclean_channel_images_removes_nothing_without_mfs_files(tmp_path):
    names = [f"{OBS_ID}_wsclean-0000-image.fits", f"{OBS_ID}_wsclean-0001-image.fits"]
    _touch(tmp_path, names)

    assert remove_wsclean_channel_images(str(tmp_path), OBS_ID) == 0

    assert sorted(p.name for p in tmp_path.iterdir()) == names


def test_remove_wsclean_channel_images_keeps_single_channel_output(tmp_path):
    names = [f"{OBS_ID}_wsclean-dirty.fits", f"{OBS_ID}_wsclean-image.fits"]
    _touch(tmp_path, names)

    assert remove_wsclean_channel_images(str(tmp_path), OBS_ID) == 0

    assert sorted(p.name for p in tmp_path.iterdir()) == names


def test_remove_wsclean_channel_images_only_touches_this_obs(tmp_path):
    _touch(tmp_path, [f"{OBS_ID}_wsclean-MFS-image.fits", f"{OBS_ID}_wsclean-0000-image.fits"])
    other = ["999_wsclean-MFS-image.fits", "999_wsclean-0000-image.fits"]
    _touch(tmp_path, other)

    assert remove_wsclean_channel_images(str(tmp_path), OBS_ID) == 1

    assert (tmp_path / other[1]).exists()


def test_remove_wsclean_channel_images_does_not_raise_if_a_file_cannot_be_removed(tmp_path):
    _touch(tmp_path, [f"{OBS_ID}_wsclean-MFS-image.fits", f"{OBS_ID}_wsclean-0000-image.fits"])

    with patch("mwax_mover.calvin.wsclean.os.remove", side_effect=PermissionError("nope")):
        assert remove_wsclean_channel_images(str(tmp_path), OBS_ID) == 0
