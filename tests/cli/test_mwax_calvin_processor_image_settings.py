"""How the processor uses the WSClean image size and scale that are worked out for each observation."""

import os
from unittest.mock import patch

import pytest
import tests_common
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.calvin.pipeline import CalvinJobType
from mwax_mover.calvin.wsclean import ArrayImageSettings, WSCleanArgs
from mwax_mover.cli.mwax_calvin_processor import MWAXCalvinProcessor

OBS = 1234567890


def _processor(tmp_path, enabled: bool) -> MWAXCalvinProcessor:
    """A processor with a working path that holds one Birli measurement set."""
    mcal = MWAXCalvinProcessor()
    mcal.obs_id = OBS
    mcal.working_path = str(tmp_path)
    mcal.cfg_proc_job_output_path = str(tmp_path / "out")
    mcal.cfg_proc_keep_completed_visibility_files = False
    mcal.cfg_wsclean_enabled = enabled
    mcal.cfg_wsclean_args = WSCleanArgs(name="", temp_dir="", niter=1000000, gridder="wgridder", polarizations="i")
    (tmp_path / f"{OBS}.ms").mkdir()
    return mcal


def test_run_imaging_gives_wsclean_the_size_and_scale_worked_out_for_the_observation(tmp_path):
    mcal = _processor(tmp_path, enabled=True)
    mcal.wsclean_image_settings = ArrayImageSettings("extended", (8000, 8000), "0.0045113", 133, 128, 5302.9)

    with (
        patch("mwax_mover.calvin.imaging.run_solutions_apply", return_value=(True, ["x_cal.ms"])),
        patch("mwax_mover.calvin.imaging.run_wsclean", return_value=True) as mock_wsclean,
        patch("mwax_mover.calvin.imaging.make_wsclean_preview"),
    ):
        assert mcal.run_imaging() is True

    args = mock_wsclean.call_args.args[5]
    assert args.size == (8000, 8000)
    assert args.scale == "0.0045113"
    assert args.niter == 1000000
    # The options from the config are not changed by a job
    assert mcal.cfg_wsclean_args.size is None
    assert mcal.cfg_wsclean_args.scale is None


def test_run_imaging_is_skipped_if_the_size_and_scale_could_not_be_worked_out(tmp_path):
    mcal = _processor(tmp_path, enabled=True)
    assert mcal.wsclean_image_settings is None

    with patch("mwax_mover.calvin.imaging.run_solutions_apply") as mock_apply:
        assert mcal.run_imaging() is False

    mock_apply.assert_not_called()


def _config_with(replacements: dict[str, str]) -> str:
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


def _initialise(config_filename: str) -> MWAXCalvinProcessor:
    mcal = MWAXCalvinProcessor()
    mcal.hostname = "calvin99"
    mcal.initialise(
        config_filename, OBS, 1234, None, CalvinJobType.realtime, "https//test.test", [123], FakeMWAXDBHandler()
    )
    return mcal


@pytest.mark.parametrize("value", ["0", "-5", "far"])
def test_the_processor_quits_on_a_bad_compact_array_baseline_max_length_if_imaging_is_enabled(value):
    config_filename = _config_with(
        {
            "[wsclean]\nenabled=0": "[wsclean]\nenabled=1",
            "compact_array_baseline_max_length=3500": f"compact_array_baseline_max_length={value}",
        }
    )

    with pytest.raises(SystemExit):
        _initialise(config_filename)


def test_the_compact_array_baseline_max_length_is_read_if_imaging_is_enabled():
    config_filename = _config_with(
        {
            "[wsclean]\nenabled=0": "[wsclean]\nenabled=1",
            "compact_array_baseline_max_length=3500": "compact_array_baseline_max_length=3000",
        }
    )

    assert _initialise(config_filename).cfg_wsclean_compact_array_baseline_max_length == 3000.0


def test_the_compact_array_baseline_max_length_is_not_needed_if_imaging_is_disabled():
    config_filename = _config_with({"compact_array_baseline_max_length=3500": "compact_array_baseline_max_length=far"})

    mcal = _initialise(config_filename)

    assert mcal.cfg_wsclean_enabled is False
    assert os.path.exists(config_filename)
