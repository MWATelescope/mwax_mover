"""Tests for calvin.wsclean: the WSClean options and running WSClean."""

from pathlib import Path
from unittest.mock import patch

from mwax_mover.calvin.wsclean import WSCleanArgs, run_wsclean

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
