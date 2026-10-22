"""Tests for the controller's Slurm walltime config and how it reaches the sbatch script."""

from configparser import ConfigParser
from unittest.mock import patch

import pytest
import tests_common
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.cli.mwax_calvin_controller import CalibrationRequest, MWAXCalvinController, read_wall_time_secs
from mwax_mover.calvin.pipeline import CalvinJobType
from mwax_mover.constants import (
    DEFAULT_MWA_ASVO_WALL_TIME_SECS,
    DEFAULT_REALTIME_WALL_TIME_SECS,
    SLURM_USR1_WARNING_SECS,
)


def _config(**calvin_keys: str) -> ConfigParser:
    config = ConfigParser()
    config.read_dict({"calvin": calvin_keys})
    return config


def test_read_wall_time_secs_uses_the_default_if_not_in_the_config():
    assert read_wall_time_secs(_config(), "realtime_wall_time_secs", 123) == 123


def test_read_wall_time_secs_reads_the_config():
    assert read_wall_time_secs(_config(realtime_wall_time_secs="7200"), "realtime_wall_time_secs", 123) == 7200


@pytest.mark.parametrize("value", [str(SLURM_USR1_WARNING_SECS), "0", "-5"])
def test_read_wall_time_secs_rejects_a_wall_time_the_signal_cannot_fit_in(value):
    with pytest.raises(ValueError, match="must be more than"):
        read_wall_time_secs(_config(realtime_wall_time_secs=value), "realtime_wall_time_secs", 123)


def test_read_wall_time_secs_rejects_a_non_integer():
    with pytest.raises(ValueError):
        read_wall_time_secs(_config(realtime_wall_time_secs="six hours"), "realtime_wall_time_secs", 123)


def _initialise_controller(config_filename: str) -> MWAXCalvinController:
    mcal = MWAXCalvinController()
    mcal.hostname = "calvin99"
    mcal.initialise(config_filename, FakeMWAXDBHandler())
    return mcal


def test_controller_uses_the_default_wall_times_if_not_in_the_config():
    tests_common.setup_test_directories("mwax_calvin_controller")
    mcal = _initialise_controller(tests_common.render_test_config("mwax_calvin_controller"))

    assert mcal.cfg_realtime_wall_time_secs == DEFAULT_REALTIME_WALL_TIME_SECS
    assert mcal.cfg_mwa_asvo_wall_time_secs == DEFAULT_MWA_ASVO_WALL_TIME_SECS


def test_controller_reads_the_wall_times_from_the_config():
    tests_common.setup_test_directories("mwax_calvin_controller")
    config_filename = tests_common.render_test_config("mwax_calvin_controller")
    with open(config_filename, encoding="UTF-8") as f:
        text = f.read()
    with open(config_filename, "w", encoding="UTF-8") as f:
        f.write(
            text.replace("[calvin]\n", "[calvin]\nrealtime_wall_time_secs=7200\nmwa_asvo_wall_time_secs=108000\n", 1)
        )

    mcal = _initialise_controller(config_filename)

    assert mcal.cfg_realtime_wall_time_secs == 7200
    assert mcal.cfg_mwa_asvo_wall_time_secs == 108000


def test_controller_quits_on_a_bad_wall_time():
    tests_common.setup_test_directories("mwax_calvin_controller")
    config_filename = tests_common.render_test_config("mwax_calvin_controller")
    with open(config_filename, encoding="UTF-8") as f:
        text = f.read()
    with open(config_filename, "w", encoding="UTF-8") as f:
        f.write(text.replace("[calvin]\n", "[calvin]\nmwa_asvo_wall_time_secs=60\n", 1))

    with pytest.raises(SystemExit):
        _initialise_controller(config_filename)


def test_realtime_submit_passes_the_configured_wall_time_to_the_sbatch_script():
    mcal = MWAXCalvinController()
    mcal.worker_config_filename = "/cfg.cfg"
    mcal.cfg_log_path = "/logs"
    mcal.cfg_calvin_script_path = "/scripts"
    mcal.cfg_realtime_wall_time_secs = 5400
    request = CalibrationRequest()
    request.obs_id = 1234567890
    request.request_id = 7
    request.type = CalvinJobType.realtime

    with (
        patch("mwax_mover.cli.mwax_calvin_controller.create_sbatch_script", return_value="script") as mock_create,
        # The submit fails, so no database is needed. Only the script arguments matter here.
        patch("mwax_mover.cli.mwax_calvin_controller.submit_sbatch", side_effect=OSError("no sbatch")),
    ):
        mcal.realtime_submit_to_slurm(request)

    assert mock_create.call_args.args[-1] == 5400
    assert mcal.slurm_errors == 1
