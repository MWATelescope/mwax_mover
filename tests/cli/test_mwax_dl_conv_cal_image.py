"""Tests for mwax_dl_conv_cal_image: arguments, the order of the steps, failures, and what it must not touch."""

import ast
import os
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import tests_common

from mwax_mover.calvin.wsclean import ArrayImageSettings
from mwax_mover.cli import mwax_dl_conv_cal_image as tool
from mwax_mover.mwa_asvo.signed_url import DownloadError

OBS = 1234567890
VIS_URL = f"https://projects.pawsey.org.au/mwa-asvo/{OBS}_1021186_vis.tar?Signature=abc&Expires=1"
MS_URL = f"https://projects.pawsey.org.au/mwa-asvo/{OBS}_1021186_ms.tar?Signature=abc&Expires=1"
MODULE = "mwax_mover.cli.mwax_dl_conv_cal_image"


@pytest.fixture
def cfg_filename() -> str:
    """A processor config (from the test data), with the stub programs it points to."""
    tests_common.setup_test_directories("mwax_calvin_processor")
    return tests_common.render_test_config("mwax_calvin_processor")


@pytest.fixture(autouse=True)
def _nothing_may_use_the_database_or_an_upload():
    """Every way that the processor uses the database, the upload or the MWAX servers fails the test."""
    forbidden = [
        "mwax_mover.db.handler.MWAXDBHandler.start_database_pool",
        "mwax_mover.net.s3.rclone_move",
        "mwax_mover.calvin.solution_files.upload_plot_files",
        "mwax_mover.calvin.solution_files.upload_image_files",
        "mwax_mover.calvin.pipeline.insert_calibration_fits_row",
        "mwax_mover.calvin.pipeline.insert_calibration_solutions_row",
        "mwax_mover.calvin.pipeline.process_solutions",
    ]
    with ExitStack() as stack:
        for target in forbidden:
            stack.enter_context(patch(target, side_effect=AssertionError(f"{target} must not be used")))
        yield


class Mocks(SimpleNamespace):
    """The functions the tool calls, patched. Each is a MagicMock with a believable side effect."""


@contextmanager
def patched_pipeline(base_dir: Path, work_dir: Path | None = None, kind: str = "vis", **overrides) -> Generator[Mocks]:
    """Patch everything the tool runs (downloads and programs), so only its own logic runs."""
    work = work_dir or base_dir / "work"
    input_dir = base_dir / "input"
    output_dir = base_dir / "output"

    def fake_download(url, dest, *args, **kwargs):
        Path(dest).write_bytes(b"tar")
        return 3 * 10**9

    def fake_extract(tar, dest):
        if kind == "ms":
            (Path(dest) / f"{OBS}.ms").mkdir()
            (Path(dest) / f"{OBS}.ms" / "table.dat").write_text("x")
        else:
            (Path(dest) / f"{OBS}_20260101000000_gpubox01_00.fits").write_text("x")

    def fake_metafits(obs_id, path):
        target = Path(path) / f"{obs_id}_metafits.fits"
        target.write_text("metafits")
        return str(target)

    def fake_birli(*args, **kwargs):
        (work / f"{OBS}.ms").mkdir()
        (work / f"{OBS}.ms" / "table.dat").write_text("x")
        return True

    def fake_imaging(obs_id, metafits, out, work_path, settings, on_task=None):
        (Path(work_path) / f"{OBS}_cal.ms").mkdir()
        (Path(out) / f"{OBS}_wsclean-MFS-image.png").write_text("png")
        return True

    mocks = Mocks(
        download=MagicMock(side_effect=fake_download),
        extract=MagicMock(side_effect=fake_extract),
        metafits=MagicMock(side_effect=fake_metafits),
        context=MagicMock(return_value=SimpleNamespace(oversampled=False)),
        estimate=MagicMock(return_value=10 * 10**9),
        birli=MagicMock(side_effect=fake_birli),
        hyperdrive=MagicMock(return_value=(True, "hyperdrive command")),
        solutions=MagicMock(return_value=SimpleNamespace(soln_tile_ids=[1, 2, 3])),
        imaging=MagicMock(side_effect=fake_imaging),
        ram_backed=MagicMock(return_value=False),
        image_settings=MagicMock(
            return_value=ArrayImageSettings("extended", (8000, 8000), "0.0045113", 133, 128, 5302.9)
        ),
    )
    for name, value in overrides.items():
        setattr(mocks, name, value)

    targets = {
        "download": "download_signed_url",
        "extract": "extract_tar",
        "metafits": "download_metafits_file",
        "context": "MetafitsContext",
        "estimate": "estimate_birli_output_bytes",
        "birli": "run_birli",
        "hyperdrive": "run_hyperdrive",
        "solutions": "process_solutions_files",
        "imaging": "run_imaging_steps",
        "ram_backed": "is_ram_backed",
        "image_settings": "array_image_settings_from_metafits",
    }
    plenty_of_room = SimpleNamespace(total=10**15, used=0, free=10**15)
    with ExitStack() as stack:
        for attribute, name in targets.items():
            stack.enter_context(patch(f"{MODULE}.{name}", getattr(mocks, attribute)))
        stack.enter_context(patch(f"{MODULE}.shutil.disk_usage", return_value=plenty_of_room))
        yield mocks
    del input_dir, output_dir


def _run(cfg: str, base_dir: Path, url: str = VIS_URL, *extra: str) -> int:
    return tool.run(["--cfg", cfg, "--url", url, "--dir", str(base_dir), *extra])


def _step_status(capsys) -> dict[str, str]:
    """The step statuses from the summary the tool printed."""
    statuses = {}
    for line in capsys.readouterr().out.splitlines():
        for name in (*tool.STOP_AFTER_CHOICES, tool.STEP_DISK_SPACE, tool.STEP_CLEANUP):
            if line.startswith(f"{name:<14}"):
                statuses[name] = line[14:23].strip()
    return statuses


# ---------------------------------------------------------------------------
# arguments and checks before anything runs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("missing", ["--cfg", "--url", "--dir"])
def test_the_three_main_arguments_are_required(missing, cfg_filename, tmp_path):
    args = {"--cfg": cfg_filename, "--url": VIS_URL, "--dir": str(tmp_path)}
    del args[missing]

    with pytest.raises(SystemExit) as exit_info:
        tool.parse_args([x for pair in args.items() for x in pair])

    assert exit_info.value.code == tool.EXIT_USAGE


def test_stop_after_must_be_a_step(cfg_filename, tmp_path):
    with pytest.raises(SystemExit) as exit_info:
        tool.parse_args(["--cfg", cfg_filename, "--url", VIS_URL, "--dir", str(tmp_path), "--stop-after", "apply"])

    assert exit_info.value.code == tool.EXIT_USAGE


def test_a_url_with_a_bad_file_name_is_a_usage_error_and_nothing_is_made(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"
    url = "https://projects.pawsey.org.au/mwa-asvo/1444927824_1021186_vis_ppds.tar?x=1"

    assert _run(cfg_filename, base, url) == tool.EXIT_USAGE

    assert "vis.tar" in capsys.readouterr().err
    assert not base.exists()


def test_a_missing_config_is_a_usage_error(tmp_path, capsys):
    assert _run(str(tmp_path / "nope.cfg"), tmp_path / "job") == tool.EXIT_USAGE

    assert "does not exist" in capsys.readouterr().err


def test_a_program_that_does_not_exist_is_a_usage_error_before_the_download(cfg_filename, tmp_path, capsys):
    os.remove(os.path.join(tests_common.get_test_bin_dir("mwax_calvin_processor"), "wsclean"))

    with patched_pipeline(tmp_path / "job") as mocks:
        assert _run(cfg_filename, tmp_path / "job") == tool.EXIT_USAGE

    assert "[wsclean] binary_path" in capsys.readouterr().err
    mocks.download.assert_not_called()


# ---------------------------------------------------------------------------
# a whole run
# ---------------------------------------------------------------------------


def test_a_vis_tar_run_does_every_step_in_order_and_cleans_up(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"
    calls: list[str] = []

    with patched_pipeline(base) as mocks:
        for name in ("download", "metafits", "birli", "hyperdrive", "solutions", "imaging"):
            original = getattr(mocks, name)
            original.side_effect = _record(calls, name, original.side_effect, original.return_value)
        assert _run(cfg_filename, base) == tool.EXIT_OK

    assert calls == ["download", "metafits", "birli", "hyperdrive", "solutions", "imaging"]
    assert _step_status(capsys) == {
        "download": "ok",
        "metafits": "ok",
        "disk space": "ok",
        "birli": "ok",
        "hyperdrive": "ok",
        "solutions": "ok",
        "imaging": "ok",
        "cleanup": "ok",
    }
    # The tar is deleted after extraction, the measurement sets at the end, the outputs stay
    assert not (base / "input" / f"{OBS}_1021186_vis.tar").exists()
    assert list((base / "work").glob("*.ms")) == []
    assert (base / "output" / f"{OBS}_wsclean-MFS-image.png").exists()
    assert (base / "input" / f"{OBS}_metafits.fits").exists()
    assert (base / tool.LOG_FILENAME).parent.exists()


def _record(calls: list[str], name: str, side_effect, return_value):
    def wrapper(*args, **kwargs):
        calls.append(name)
        return side_effect(*args, **kwargs) if side_effect is not None else return_value

    return wrapper


def test_birli_gets_the_settings_from_the_config(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_OK

    args = mocks.birli.call_args.args
    assert args[0] == str(base / "input")
    assert args[2] == str(base / "work" / f"{OBS}.ms")
    assert args[3] == str(base / "output")
    assert args[4] == OBS
    assert args[5] is False  # oversampled
    assert args[7] == 830  # [birli] max_mem_gib, not reduced because the work directory is on disk
    assert args[9] == 40 * 1000  # freq_res_khz in Hz


def test_the_download_gets_the_retry_and_proxy_settings(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base) as mocks:
        _run(cfg_filename, base)

    url, dest, attempts, wait, timeout, proxy, no_proxy = mocks.download.call_args.args
    assert url == VIS_URL
    assert dest == str(base / "input" / f"{OBS}_1021186_vis.tar")
    assert (attempts, wait, timeout) == (3, 240, 3600)
    assert proxy == "http://localhost:3128"
    assert no_proxy == "asvo.mwatelescope.org"


def test_an_ms_tar_run_skips_birli_and_keeps_the_downloaded_measurement_sets(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"
    # Birli is not needed, so it does not have to exist
    os.remove(os.path.join(tests_common.get_test_bin_dir("mwax_calvin_processor"), "birli"))

    with patched_pipeline(base, kind="ms") as mocks:
        assert _run(cfg_filename, base, MS_URL) == tool.EXIT_OK

    statuses = _step_status(capsys)
    assert statuses["birli"] == "skipped"
    mocks.birli.assert_not_called()
    # The measurement set moved from input/ to work/, hyperdrive got it, and it is not removed
    assert mocks.hyperdrive.call_args.args[0] == [str(base / "work" / f"{OBS}.ms")]
    assert (base / "work" / f"{OBS}.ms").is_dir()
    assert not (base / "input" / f"{OBS}.ms").exists()
    # ... and the imaging step is told not to delete its input
    assert mocks.imaging.call_args.args[4].remove_input_ms is False
    # The calibrated copy is removed at the end
    assert not (base / "work" / f"{OBS}_cal.ms").exists()


def test_a_vis_tar_run_tells_the_imaging_step_to_delete_the_birli_sets(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base) as mocks:
        _run(cfg_filename, base)

    assert mocks.imaging.call_args.args[4].remove_input_ms is True
    assert mocks.imaging.call_args.args[2] == str(base / "output")
    assert mocks.imaging.call_args.args[3] == str(base / "work")


def test_an_ms_tar_with_a_measurement_set_the_pipeline_cannot_use_fails(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    def fake_extract(tar, dest):
        (Path(dest) / "something_else.ms").mkdir()

    with patched_pipeline(base, kind="ms", extract=MagicMock(side_effect=fake_extract)) as mocks:
        assert _run(cfg_filename, base, MS_URL) == tool.EXIT_STEP_FAILED

    assert "something_else.ms" in capsys.readouterr().out
    mocks.hyperdrive.assert_not_called()


def test_picket_fence_measurement_set_names_are_accepted(cfg_filename, tmp_path):
    base = tmp_path / "job"

    def fake_extract(tar, dest):
        for name in (f"{OBS}_ch75-76.ms", f"{OBS}_ch159.ms"):
            (Path(dest) / name).mkdir()

    with patched_pipeline(base, kind="ms", extract=MagicMock(side_effect=fake_extract)) as mocks:
        assert _run(cfg_filename, base, MS_URL) == tool.EXIT_OK

    assert [os.path.basename(p) for p in mocks.hyperdrive.call_args.args[0]] == [f"{OBS}_ch159.ms", f"{OBS}_ch75-76.ms"]


def test_the_work_dir_argument_is_used(cfg_filename, tmp_path):
    base = tmp_path / "job"
    work = tmp_path / "somewhere_else"

    with patched_pipeline(base, work_dir=work) as mocks:
        assert _run(cfg_filename, base, VIS_URL, "--work-dir", str(work)) == tool.EXIT_OK

    assert mocks.birli.call_args.args[2] == str(work / f"{OBS}.ms")
    assert mocks.imaging.call_args.args[3] == str(work)
    assert not (base / "work").exists()


# ---------------------------------------------------------------------------
# memory and disk space
# ---------------------------------------------------------------------------


def test_a_ram_backed_work_dir_takes_its_size_off_the_birli_and_wsclean_memory(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(
        base, estimate=MagicMock(return_value=100 * 10**9), ram_backed=MagicMock(return_value=True)
    ) as m:
        assert _run(cfg_filename, base) == tool.EXIT_OK

    # 100 GB of measurement sets is 200 GB of working space with imaging, which is 186 GiB
    assert m.birli.call_args.args[7] == 830 - 186
    assert m.imaging.call_args.args[4].wsclean_abs_mem_gib == 830 - 186


def test_a_work_dir_on_disk_leaves_the_memory_alone(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base, estimate=MagicMock(return_value=100 * 10**9)) as m:
        _run(cfg_filename, base)

    assert m.birli.call_args.args[7] == 830
    assert m.imaging.call_args.args[4].wsclean_abs_mem_gib == 830


def test_not_enough_room_stops_before_birli(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"
    tiny = SimpleNamespace(total=10**9, used=0, free=10**9)

    with patched_pipeline(base) as mocks, patch(f"{MODULE}.shutil.disk_usage", return_value=tiny):
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    assert "Not enough room" in capsys.readouterr().out
    mocks.birli.assert_not_called()


# ---------------------------------------------------------------------------
# failures and options
# ---------------------------------------------------------------------------


def test_a_failed_download_stops_the_run(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base, download=MagicMock(side_effect=DownloadError("The URL has expired"))) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    out = capsys.readouterr().out
    assert "expired" in out
    assert "FAILED" in out
    mocks.metafits.assert_not_called()
    mocks.birli.assert_not_called()


def test_the_run_stops_at_the_first_failed_step_and_does_not_clean_up(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base, hyperdrive=MagicMock(return_value=(False, ""))) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    statuses = _step_status(capsys)
    assert statuses["hyperdrive"] == "failed"
    assert "solutions" not in statuses
    assert "cleanup" not in statuses
    mocks.solutions.assert_not_called()
    assert (base / "work" / f"{OBS}.ms").exists()  # left for inspection


def test_no_unflagged_tiles_is_a_failure(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base, solutions=MagicMock(return_value=None)) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    assert "No unflagged tiles" in capsys.readouterr().out
    mocks.imaging.assert_not_called()


def test_an_exception_while_processing_solutions_writes_a_readme_and_fails(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base, solutions=MagicMock(side_effect=RuntimeError("boom"))) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    assert "boom" in (base / "output" / "readme_error.txt").read_text()
    mocks.imaging.assert_not_called()


def test_a_failed_imaging_step_fails_the_run(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base, imaging=MagicMock(return_value=False)):
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    assert _step_status(capsys)["imaging"] == "failed"


def test_stop_after_stops_and_does_not_clean_up(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base) as mocks:
        assert _run(cfg_filename, base, VIS_URL, "--stop-after", "birli") == tool.EXIT_OK

    statuses = _step_status(capsys)
    assert statuses["birli"] == "ok"
    assert "hyperdrive" not in statuses
    assert "cleanup" not in statuses
    mocks.hyperdrive.assert_not_called()
    assert (base / "work" / f"{OBS}.ms").exists()


def test_stop_after_download_does_not_get_the_metafits(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base) as mocks:
        assert _run(cfg_filename, base, VIS_URL, "--stop-after", "download") == tool.EXIT_OK

    mocks.metafits.assert_not_called()


def test_skip_download_uses_the_data_that_is_there(cfg_filename, tmp_path):
    base = tmp_path / "job"
    (base / "input").mkdir(parents=True)
    (base / "input" / f"{OBS}_20260101000000_gpubox01_00.fits").write_text("x")
    (base / "input" / f"{OBS}_metafits.fits").write_text("metafits")

    with patched_pipeline(base) as mocks:
        assert _run(cfg_filename, base, VIS_URL, "--skip-download") == tool.EXIT_OK

    mocks.download.assert_not_called()
    mocks.metafits.assert_not_called()  # the metafits is already there
    mocks.birli.assert_called_once()


def test_skip_download_with_nothing_in_the_input_directory_fails(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base) as mocks:
        assert _run(cfg_filename, base, VIS_URL, "--skip-download") == tool.EXIT_STEP_FAILED

    assert "--skip-download needs data" in capsys.readouterr().out
    mocks.birli.assert_not_called()


def test_an_ms_tar_run_can_be_repeated_with_skip_download(cfg_filename, tmp_path):
    base = tmp_path / "job"

    with patched_pipeline(base, kind="ms"):
        assert _run(cfg_filename, base, MS_URL) == tool.EXIT_OK
    # The first run moved the measurement set to work/ and removed only the calibrated copy
    assert (base / "work" / f"{OBS}.ms").is_dir()

    with patched_pipeline(base, kind="ms") as mocks:
        assert _run(cfg_filename, base, MS_URL, "--skip-download") == tool.EXIT_OK

    mocks.download.assert_not_called()
    assert mocks.hyperdrive.call_args.args[0] == [str(base / "work" / f"{OBS}.ms")]


def test_ctrl_c_ends_the_run_with_a_failure_and_still_prints_the_summary(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base, birli=MagicMock(side_effect=KeyboardInterrupt)):
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    assert "interrupted" in capsys.readouterr().out


def test_keep_completed_visibility_files_keeps_everything(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"
    with open(cfg_filename, encoding="UTF-8") as f:
        text = f.read()
    assert "keep_completed_visibility_files=0" in text
    with open(cfg_filename, "w", encoding="UTF-8") as f:
        f.write(text.replace("keep_completed_visibility_files=0", "keep_completed_visibility_files=1"))

    with patched_pipeline(base) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_OK

    assert _step_status(capsys)["cleanup"] == "skipped"
    assert mocks.imaging.call_args.args[4].remove_input_ms is False
    assert (base / "work" / f"{OBS}_cal.ms").exists()
    assert (base / "input" / f"{OBS}_1021186_vis.tar").exists()


# ---------------------------------------------------------------------------
# what the tool must not use
# ---------------------------------------------------------------------------


def test_the_tool_module_does_not_import_the_database_the_upload_or_the_processor():
    source = Path(tool.__file__).read_text(encoding="utf-8")
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden = (
        "mwax_mover.db",
        "mwax_mover.net",
        "mwax_mover.archive",
        "mwax_mover.calvin.slurm",
        "mwax_mover.calvin.solution_files",
        "mwax_mover.cli.mwax_calvin_processor",
        "mwax_mover.cli.mwax_calvin_controller",
        "psycopg",
        "redis",
    )
    assert [m for m in imported if m.startswith(forbidden)] == []


def test_format_summary_lists_every_step():
    job = tool.Job(
        settings=MagicMock(),
        info=tool.parse_signed_url(VIS_URL),
        dirs=tool.Dirs("/d/input", "/d/work", "/d/output"),
        url=VIS_URL,
    )
    job.results = [
        tool.StepResult("download", "ok", 12.34, "downloaded 3.0 GB"),
        tool.StepResult("birli", "failed", 1.0, "Birli failed"),
    ]

    summary = tool.format_summary(job, success=False)

    assert f"observation {OBS} (MWA ASVO job 1021186, vis)" in summary
    assert "download" in summary
    assert "12.3" in summary
    assert "Birli failed" in summary
    assert "Result: FAILED" in summary
    assert "Output: /d/output" in summary


# ---------------------------------------------------------------------------
# the WSClean image size and scale are worked out for each observation
# ---------------------------------------------------------------------------


def test_imaging_gets_the_image_size_and_scale_worked_out_for_the_observation(cfg_filename, tmp_path):
    base = tmp_path / "job"
    compact = ArrayImageSettings("compact", (4000, 4000), "0.0090226", 133, 128, 2873.5)

    with patched_pipeline(base, image_settings=MagicMock(return_value=compact)) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_OK

    # The metafits and the cutoff from the config go in ...
    context, cutoff = mocks.image_settings.call_args.args
    assert context is mocks.context.return_value
    assert cutoff == 3500.0
    # ... and the size and scale come out in the WSClean options. The other options are from the config.
    wsclean_args = mocks.imaging.call_args.args[4].wsclean_args
    assert wsclean_args.size == (4000, 4000)
    assert wsclean_args.scale == "0.0090226"
    assert wsclean_args.niter == 1000000
    assert wsclean_args.gridder == "wgridder"


def test_the_image_settings_are_in_the_summary(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base):
        _run(cfg_filename, base)

    assert "extended array: -size 8000 8000 -scale 0.0045113" in capsys.readouterr().out


def test_a_failure_to_work_out_the_image_settings_fails_imaging_before_anything_runs(cfg_filename, tmp_path, capsys):
    base = tmp_path / "job"

    with patched_pipeline(base, image_settings=MagicMock(side_effect=ValueError("centre channel 0"))) as mocks:
        assert _run(cfg_filename, base) == tool.EXIT_STEP_FAILED

    assert "centre channel 0" in capsys.readouterr().out
    mocks.imaging.assert_not_called()
