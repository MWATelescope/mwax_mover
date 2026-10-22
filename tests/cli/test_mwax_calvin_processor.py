import base64
import json
import logging
import os
import signal
from unittest.mock import patch

import numpy as np
import pytest
import tests_common
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.calvin.wsclean import WSCleanArgs
from mwax_mover.cli.mwax_calvin_processor import CalvinJobType, MWAXCalvinProcessor
from mwax_mover.constants import EXIT_FAILURE

logger = logging.getLogger(__name__)


def test_mwax_calvin_processor():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = tests_common.setup_test_directories("mwax_calvin_processor")
    bin_dir = tests_common.get_test_bin_dir("mwax_calvin_processor")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "calvin99"

    # Determine config file location
    config_filename = tests_common.render_test_config("mwax_calvin_processor")

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # Call to read config <-- this is what we're testing!
    mcal.initialise(
        config_filename,
        1234567890,
        1234,
        None,
        CalvinJobType.realtime,
        "https//test.test",
        [
            123,
        ],
        fake_db_handler,
    )

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcal.cfg_log_path == os.path.join(base_dir, "logs"), (
        f"log path mismatch: {mcal.cfg_log_path} {os.path.join(base_dir, 'logs')}"
    )

    assert mcal.cfg_health_multicast_interface_name == "lo"
    assert mcal.cfg_health_multicast_ip == "127.0.0.1"
    assert mcal.cfg_health_multicast_port == 8011
    assert mcal.cfg_health_multicast_hops == 1

    assert mcal.cfg_proc_job_input_path == os.path.join(base_dir, "data/calvin/in_jobs")
    assert mcal.cfg_proc_job_output_path == os.path.join(base_dir, "data/calvin/out_jobs")
    assert mcal.cfg_hyperdrive_source_list_filename == os.path.join(bin_dir, "srclist.txt")
    assert mcal.cfg_hyperdrive_source_list_type == "rts"
    assert mcal.cfg_hyperdrive_binary_path == os.path.join(bin_dir, "hyperdrive")
    assert mcal.cfg_hyperdrive_timeout == 7200
    assert mcal.cfg_birli_binary_path == os.path.join(bin_dir, "birli")
    assert mcal.cfg_birli_timeout == 3600
    assert mcal.cfg_proc_keep_completed_visibility_files == 0
    assert mcal.cfg_proc_cal_export_max_age_hours == 24
    assert (
        mcal.hyperdrive_extra_args == "--uvw-min 50m --uvw-max 1667l --max-iterations 500 --stop-thresh 1e-20"
        " --veto-threshold 0.0 --source-dist-cutoff 50"
    )
    assert mcal.cfg_proc_phase_fit_niter == 3
    assert mcal.gains_cut_off_max == 100.0


def test_mwax_calvin_processor_no_gains_cutoff():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = tests_common.setup_test_directories("mwax_calvin_processor")
    bin_dir = tests_common.get_test_bin_dir("mwax_calvin_processor")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "calvin99"

    # Determine config file location
    config_filename = tests_common.render_test_config(
        "mwax_calvin_processor", "mwax_calvin_processor_no_gains_cutoff.cfg"
    )

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # Call to read config <-- this is what we're testing!
    mcal.initialise(
        config_filename,
        1234567890,
        1234,
        None,
        CalvinJobType.realtime,
        "https//test.test",
        [
            123,
        ],
        fake_db_handler,
    )

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcal.cfg_log_path == os.path.join(base_dir, "logs"), (
        f"log path mismatch: {mcal.cfg_log_path} {os.path.join(base_dir, 'logs')}"
    )

    assert mcal.cfg_health_multicast_interface_name == "lo"
    assert mcal.cfg_health_multicast_ip == "127.0.0.1"
    assert mcal.cfg_health_multicast_port == 8011
    assert mcal.cfg_health_multicast_hops == 1

    assert mcal.cfg_proc_job_input_path == os.path.join(base_dir, "data/calvin/in_jobs")
    assert mcal.cfg_proc_job_output_path == os.path.join(base_dir, "data/calvin/out_jobs")
    assert mcal.cfg_hyperdrive_source_list_filename == os.path.join(bin_dir, "srclist.txt")
    assert mcal.cfg_hyperdrive_source_list_type == "rts"
    assert mcal.cfg_hyperdrive_binary_path == os.path.join(bin_dir, "hyperdrive")
    assert mcal.cfg_hyperdrive_timeout == 7200
    assert mcal.cfg_birli_binary_path == os.path.join(bin_dir, "birli")
    assert mcal.cfg_birli_timeout == 3600
    assert mcal.cfg_proc_keep_completed_visibility_files == 0
    assert mcal.cfg_proc_cal_export_max_age_hours == 24
    assert (
        mcal.hyperdrive_extra_args == "--uvw-min 50m --uvw-max 1667l --max-iterations 500 --stop-thresh 1e-20"
        " --veto-threshold 0.0 --source-dist-cutoff 50"
    )
    assert mcal.cfg_proc_phase_fit_niter == 3
    assert mcal.gains_cut_off_max is None


def _read_processor_config(config_filename: str) -> MWAXCalvinProcessor:
    """Initialise a processor from a rendered test config file."""
    mcal = MWAXCalvinProcessor()
    mcal.hostname = "calvin99"
    mcal.initialise(
        config_filename,
        1234567890,
        1234,
        None,
        CalvinJobType.realtime,
        "https//test.test",
        [123],
        FakeMWAXDBHandler(),
    )
    return mcal


def test_mwax_calvin_processor_reads_apply_timeout_and_wsclean_disabled():
    tests_common.setup_test_directories("mwax_calvin_processor")
    mcal = _read_processor_config(tests_common.render_test_config("mwax_calvin_processor"))

    assert mcal.cfg_hyperdrive_apply_timeout == 7200
    assert mcal.cfg_wsclean_enabled is False


def test_mwax_calvin_processor_reads_wsclean_enabled():
    tests_common.setup_test_directories("mwax_calvin_processor")
    config_filename = tests_common.render_test_config("mwax_calvin_processor")
    with open(config_filename, encoding="UTF-8") as f:
        text = f.read()
    assert "[wsclean]\nenabled=0" in text
    with open(config_filename, "w", encoding="UTF-8") as f:
        f.write(text.replace("[wsclean]\nenabled=0", "[wsclean]\nenabled=1"))

    mcal = _read_processor_config(config_filename)

    assert mcal.cfg_wsclean_enabled is True


def test_mwax_calvin_processor_reads_wsclean_binary_and_timeout():
    tests_common.setup_test_directories("mwax_calvin_processor")
    mcal = _read_processor_config(tests_common.render_test_config("mwax_calvin_processor"))

    assert mcal.cfg_wsclean_binary_path == os.path.join(
        tests_common.get_test_bin_dir("mwax_calvin_processor"), "wsclean"
    )
    assert mcal.cfg_wsclean_timeout == 14400


def test_mwax_calvin_processor_wsclean_enabled_needs_the_binary_to_exist():
    tests_common.setup_test_directories("mwax_calvin_processor")
    config_filename = tests_common.render_test_config("mwax_calvin_processor")
    with open(config_filename, encoding="UTF-8") as f:
        text = f.read()
    with open(config_filename, "w", encoding="UTF-8") as f:
        f.write(text.replace("[wsclean]\nenabled=0", "[wsclean]\nenabled=1"))

    bin_dir = tests_common.get_test_bin_dir("mwax_calvin_processor")
    wsclean_path = os.path.join(bin_dir, "wsclean")
    if os.path.exists(wsclean_path):
        os.remove(wsclean_path)

    with pytest.raises(SystemExit):
        _read_processor_config(config_filename)


def _processor_with_ms_dirs(tmp_path, names):
    """A processor whose working path holds placeholder measurement set directories."""
    mcal = MWAXCalvinProcessor()
    mcal.obs_id = 1234567890
    mcal.working_path = str(tmp_path)
    mcal.cfg_proc_job_output_path = str(tmp_path / "out")
    mcal.cfg_proc_keep_completed_visibility_files = False
    for name in names:
        ms_dir = tmp_path / name
        ms_dir.mkdir()
        (ms_dir / "table.dat").write_text("placeholder")
    return mcal


def test_remove_measurement_sets_removes_birli_and_calibrated_sets(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms", "1234567890_ch75_cal.ms"])

    mcal.remove_measurement_sets()

    assert list(tmp_path.glob("*.ms")) == []


def test_remove_measurement_sets_keeps_them_if_asked(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    mcal.cfg_proc_keep_completed_visibility_files = True

    mcal.remove_measurement_sets()

    assert (tmp_path / "1234567890_ch75.ms").exists()


def test_run_imaging_applies_solutions_then_runs_wsclean_on_the_calibrated_sets(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms", "1234567890_ch159.ms", "1234567890_ch159_cal.ms"])
    mcal.cfg_wsclean_binary_path = "/bin/wsclean"
    mcal.cfg_wsclean_timeout = 99
    mcal.cfg_hyperdrive_apply_timeout = 77
    mcal.cfg_hyperdrive_binary_path = "/bin/hyperdrive"
    mcal.metafits_filename = "meta.fits"
    cal_ms = [str(tmp_path / "1234567890_ch159_cal.ms"), str(tmp_path / "1234567890_ch75_cal.ms")]

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)) as mock_apply,
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True) as mock_wsclean,
    ):
        mcal.run_imaging()

    apply_args = mock_apply.call_args.args
    # Only the Birli sets go in, sorted
    assert apply_args[0] == [str(tmp_path / "1234567890_ch159.ms"), str(tmp_path / "1234567890_ch75.ms")]
    assert apply_args[1:] == (
        "meta.fits",
        str(tmp_path / "out"),
        str(tmp_path),
        1234567890,
        "/bin/hyperdrive",
        77,
        True,
    )
    wsclean_args = mock_wsclean.call_args.args
    assert wsclean_args[0] == cal_ms
    assert wsclean_args[3:5] == ("/bin/wsclean", 99)
    assert wsclean_args[5].name == str(tmp_path / "out" / "1234567890_wsclean")
    assert wsclean_args[5].temp_dir == str(tmp_path)


def test_run_imaging_does_not_run_wsclean_if_solutions_apply_fails(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(False, [])),
        patch("mwax_mover.calvin.imaging.run_wsclean") as mock_wsclean,
    ):
        mcal.run_imaging()

    mock_wsclean.assert_not_called()


def test_run_imaging_swallows_unexpected_exceptions(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])

    with patch("mwax_mover.calvin.imaging.run_solutions_apply", side_effect=RuntimeError("boom")):
        mcal.run_imaging()  # must not raise


def test_signal_handler_before_success_fails_the_job(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, [])
    mcal.data_downloaded = True

    with (
        patch.object(mcal, "fail_job_processing") as mock_fail,
        patch.object(mcal, "stop", side_effect=SystemExit(1)) as mock_stop,
        pytest.raises(SystemExit),
    ):
        mcal.signal_handler(signal.SIGTERM, None)

    mock_fail.assert_called_once()
    mock_stop.assert_called_once_with(exit_code=EXIT_FAILURE)


def test_signal_handler_after_success_keeps_the_job_successful_and_removes_ms(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms", "1234567890_ch75_cal.ms"])
    mcal.data_downloaded = True
    mcal.job_succeeded = True

    with (
        patch.object(mcal, "fail_job_processing") as mock_fail,
        patch.object(mcal, "fail_job_downloading") as mock_fail_dl,
        patch.object(mcal, "stop", side_effect=SystemExit(0)) as mock_stop,
        pytest.raises(SystemExit),
    ):
        mcal.signal_handler(signal.SIGUSR1, None)

    mock_fail.assert_not_called()
    mock_fail_dl.assert_not_called()
    mock_stop.assert_called_once_with(exit_code=0)
    assert list(tmp_path.glob("*.ms")) == []


def _processor_ready_to_publish_images(tmp_path):
    mcal = MWAXCalvinProcessor()
    mcal.obs_id = 1234567890
    mcal.cfg_proc_job_output_path = str(tmp_path / "out")
    mcal.cfg_proc_plot_upload_path = str(tmp_path / "plots")
    mcal.cfg_proc_plot_front_end_url = "https://somes3url"
    os.makedirs(mcal.cfg_proc_job_output_path)
    os.makedirs(mcal.cfg_proc_plot_upload_path)
    for name in ["1234567890_wsclean-image.fits", "1234567890_wsclean_readme.txt"]:
        with open(os.path.join(mcal.cfg_proc_job_output_path, name), "w", encoding="UTF-8") as f:
            f.write(name)
    mcal.published_index = {"version": 2, "generated_at": "", "files": [{"filename": "phase_fits.png"}]}
    return mcal


def test_publish_images_publishes_the_extended_index_to_the_images_dir(tmp_path):
    mcal = _processor_ready_to_publish_images(tmp_path)

    mcal.publish_images(1768401673707300)

    images_dir = tmp_path / "plots" / "1768401673707300_images"
    assert sorted(p.name for p in images_dir.iterdir()) == [
        "1234567890_wsclean-image.fits",
        "1234567890_wsclean_readme.txt",
        "index.json",
    ]
    with open(images_dir / "index.json", encoding="UTF-8") as f:
        index = json.load(f)
    assert [e["filename"] for e in index["files"]] == [
        "phase_fits.png",
        "1234567890_wsclean-image.fits",
        "1234567890_wsclean_readme.txt",
    ]


def test_publish_images_does_nothing_if_the_first_publish_did_not_work(tmp_path):
    mcal = _processor_ready_to_publish_images(tmp_path)
    mcal.published_index = None

    mcal.publish_images(1768401673707300)

    assert os.listdir(tmp_path / "plots") == []


def test_publish_images_swallows_unexpected_exceptions(tmp_path):
    mcal = _processor_ready_to_publish_images(tmp_path)

    with patch("mwax_mover.cli.mwax_calvin_processor.upload_image_files", side_effect=RuntimeError("boom")):
        mcal.publish_images(1768401673707300)  # must not raise


def test_run_imaging_returns_whether_wsclean_worked(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    cal_ms = [str(tmp_path / "1234567890_ch75_cal.ms")]

    with patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)):
        with patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True):
            assert mcal.run_imaging() is True
        with patch("mwax_mover.calvin.imaging.run_wsclean", return_value=False):
            assert mcal.run_imaging() is False

    with patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(False, [])):
        assert mcal.run_imaging() is False


def _rendered_config_with(replacements: dict[str, str]) -> str:
    """Render the processor test config, then replace text in it."""
    tests_common.setup_test_directories("mwax_calvin_processor")
    config_filename = tests_common.render_test_config("mwax_calvin_processor")
    with open(config_filename, encoding="UTF-8") as f:
        text = f.read()
    for old, new in replacements.items():
        assert old in text
        text = text.replace(old, new, 1)
    with open(config_filename, "w", encoding="UTF-8") as f:
        f.write(text)
    return config_filename


def test_mwax_calvin_processor_reads_the_wsclean_options_if_enabled():
    mcal = _read_processor_config(_rendered_config_with({"[wsclean]\nenabled=0": "[wsclean]\nenabled=1"}))

    assert mcal.cfg_wsclean_max_mem_gib == 830
    assert mcal.cfg_wsclean_args.size == (3072, 3072)
    assert mcal.cfg_wsclean_args.scale == "0.75amin"
    assert mcal.cfg_wsclean_args.channels_out == 4
    assert mcal.cfg_wsclean_args.join_channels is True


def test_mwax_calvin_processor_does_not_need_the_wsclean_options_if_disabled():
    config_filename = _rendered_config_with({"max_mem_gib=830\nsize=3072 3072\nscale=0.75amin\n": ""})

    mcal = _read_processor_config(config_filename)

    assert mcal.cfg_wsclean_enabled is False
    assert mcal.cfg_wsclean_args.size is None


def test_mwax_calvin_processor_quits_on_wsclean_options_that_do_not_fit_together():
    config_filename = _rendered_config_with(
        {"[wsclean]\nenabled=0": "[wsclean]\nenabled=1", "channels_out=4": "channels_out=1"}
    )

    with pytest.raises(SystemExit):
        _read_processor_config(config_filename)


def test_run_imaging_gives_wsclean_the_configured_options_and_the_memory_limit(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    mcal.cfg_wsclean_args = WSCleanArgs(name="", temp_dir="", size=(3072, 3072), scale="0.75amin", multiscale=True)
    mcal.wsclean_abs_mem_gib = 737
    cal_ms = [str(tmp_path / "1234567890_ch75_cal.ms")]

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True) as mock_wsclean,
    ):
        assert mcal.run_imaging() is True

    args = mock_wsclean.call_args.args[5]
    assert args.name == str(tmp_path / "out" / "1234567890_wsclean")
    assert args.temp_dir == str(tmp_path)
    assert args.abs_mem_gib == 737
    assert args.size == (3072, 3072)
    assert args.multiscale is True
    # The options from the config are not changed by a job
    assert mcal.cfg_wsclean_args.name == ""
    assert mcal.cfg_wsclean_args.abs_mem_gib is None


def test_run_imaging_is_skipped_if_the_measurement_sets_leave_no_memory_for_wsclean(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    mcal.wsclean_abs_mem_gib = 0

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply") as mock_apply,
        patch("mwax_mover.calvin.imaging.run_wsclean") as mock_wsclean,
    ):
        assert mcal.run_imaging() is False

    mock_apply.assert_not_called()
    mock_wsclean.assert_not_called()


def _write_small_wsclean_fits(path: str) -> None:
    from astropy.io import fits

    hdu = fits.PrimaryHDU(np.random.default_rng(1).normal(0.0, 0.01, (1, 1, 32, 32)).astype(np.float32))
    hdu.header["BUNIT"] = "JY/BEAM"
    hdu.writeto(path)


def test_run_imaging_makes_the_preview_png_after_wsclean_works(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    os.makedirs(mcal.cfg_proc_job_output_path)
    _write_small_wsclean_fits(os.path.join(mcal.cfg_proc_job_output_path, "1234567890_wsclean-MFS-image.fits"))
    cal_ms = [str(tmp_path / "1234567890_ch75_cal.ms")]

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
    ):
        assert mcal.run_imaging() is True

    assert os.path.exists(os.path.join(mcal.cfg_proc_job_output_path, "1234567890_wsclean-MFS-image.png"))


def test_run_imaging_still_works_if_the_preview_png_cannot_be_made(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    cal_ms = [str(tmp_path / "1234567890_ch75_cal.ms")]

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True),
    ):
        # There is no image in the output dir, so there is nothing to draw
        assert mcal.run_imaging() is True


def test_run_imaging_does_not_make_a_preview_png_if_wsclean_fails(tmp_path):
    mcal = _processor_with_ms_dirs(tmp_path, ["1234567890_ch75.ms"])
    cal_ms = [str(tmp_path / "1234567890_ch75_cal.ms")]

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, cal_ms)),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=False),
        patch("mwax_mover.calvin.imaging.make_wsclean_preview") as mock_preview,
    ):
        assert mcal.run_imaging() is False

    mock_preview.assert_not_called()


def test_publish_images_includes_the_preview_png(tmp_path):
    mcal = _processor_ready_to_publish_images(tmp_path)
    png = os.path.join(mcal.cfg_proc_job_output_path, "1234567890_wsclean-MFS-image.png")
    with open(png, "wb") as f:
        f.write(
            base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )
        )

    mcal.publish_images(1768401673707300)

    images_dir = tmp_path / "plots" / "1768401673707300_images"
    assert (images_dir / "1234567890_wsclean-MFS-image.png").exists()
    with open(images_dir / "index.json", encoding="UTF-8") as f:
        index = json.load(f)
    assert "1234567890_wsclean-MFS-image.png" in [e["filename"] for e in index["files"]]
