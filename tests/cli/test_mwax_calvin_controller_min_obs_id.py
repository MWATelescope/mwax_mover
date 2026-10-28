"""Tests for the controller failing requests for observations older than MIN_OBS_ID."""

from unittest.mock import patch

import tests_common
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.cli.mwax_calvin_controller import MWAXCalvinController
from mwax_mover.calvin.pipeline import CalvinJobType
from mwax_mover.constants import MIN_OBS_ID, MIN_OBS_ID_MESSAGE

GET_REQUESTS = "mwax_mover.cli.mwax_calvin_controller.get_unattempted_calibration_requests"
FAIL_REQUEST = "mwax_mover.cli.mwax_calvin_controller.update_calibration_request_download_complete_status"


def _controller() -> MWAXCalvinController:
    tests_common.setup_test_directories("mwax_calvin_controller")
    mcal = MWAXCalvinController()
    mcal.hostname = "calvin99"
    mcal.initialise(tests_common.render_test_config("mwax_calvin_controller"), FakeMWAXDBHandler())
    mcal.running = True
    return mcal


def test_requests_before_min_obs_id_are_failed_and_not_returned():
    mcal = _controller()
    # (request_id, obs_id, realtime, bulk_request)
    rows = [(1, MIN_OBS_ID - 1, True, False), (2, MIN_OBS_ID - 1, False, False), (3, MIN_OBS_ID - 1, False, True)]

    with patch(GET_REQUESTS, return_value=rows), patch(FAIL_REQUEST) as mock_fail:
        realtime, asvo = mcal.get_new_calibration_requests()

    assert realtime == []
    assert asvo == []
    assert mock_fail.call_count == 3
    for call, request_id in zip(mock_fail.call_args_list, [1, 2, 3], strict=True):
        args = call.args
        assert args[0] is mcal.db_handler
        assert args[1] is None  # no slurm job
        assert args[2] == [request_id]
        assert args[3] is None  # not completed
        assert args[4] is not None  # error datetime
        assert args[5] == MIN_OBS_ID_MESSAGE


def test_requests_at_or_after_min_obs_id_are_not_failed():
    mcal = _controller()
    rows = [(1, MIN_OBS_ID, True, False), (2, MIN_OBS_ID + 1, False, False), (3, MIN_OBS_ID + 5, False, True)]

    with patch(GET_REQUESTS, return_value=rows), patch(FAIL_REQUEST) as mock_fail:
        realtime, asvo = mcal.get_new_calibration_requests()

    mock_fail.assert_not_called()
    assert [r.request_id for r in realtime] == [1]
    assert realtime[0].type == CalvinJobType.realtime
    assert [r.request_id for r in asvo] == [2, 3]
    assert [r.bulk_request for r in asvo] == [False, True]


def test_only_the_old_requests_in_a_mixed_batch_are_failed():
    mcal = _controller()
    rows = [(1, MIN_OBS_ID - 1000, False, False), (2, MIN_OBS_ID + 1000, False, False)]

    with patch(GET_REQUESTS, return_value=rows), patch(FAIL_REQUEST) as mock_fail:
        _, asvo = mcal.get_new_calibration_requests()

    assert [r.request_id for r in asvo] == [2]
    mock_fail.assert_called_once()
    assert mock_fail.call_args.args[2] == [1]


def test_a_database_error_failing_a_request_is_counted_and_does_not_stop_the_others():
    mcal = _controller()
    rows = [(1, MIN_OBS_ID - 1, False, False), (2, MIN_OBS_ID + 1000, False, False)]

    with patch(GET_REQUESTS, return_value=rows), patch(FAIL_REQUEST, side_effect=RuntimeError("db down")):
        _, asvo = mcal.get_new_calibration_requests()

    assert [r.request_id for r in asvo] == [2]
    assert mcal.database_errors == 1
