"""Tests for mwax_mover.net.webservice.call_webservice.

Was previously untested (see the module's git history: this file used to be
a placeholder). Added alongside converting the function's outer retry loop
from a hand-rolled for-loop to tenacity (docs/CLEANUP.md 7.7), since a
non-trivial retry-mechanism change landing with no test coverage at all
would be its own risk.

time.sleep is mocked throughout: tenacity's own default sleep strategy
(tenacity.nap.sleep) calls the stdlib time.sleep, and this function's fixed
30-second wait would otherwise make any test exercising more than one
attempt take tens of seconds for real.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from mwax_mover.net.webservice import call_webservice


def _make_response(status_code: int, text: str = "") -> MagicMock:
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.text = text
    return response


class TestCallWebservice:
    def test_succeeds_on_first_url_first_attempt(self):
        """A 200 on the first URL returns immediately, with no retry at all."""
        response = _make_response(200)
        with patch("mwax_mover.net.webservice.requests.request", return_value=response) as mock_request:
            result = call_webservice(123, ["http://a"], None)

        assert result is response
        mock_request.assert_called_once_with("GET", "http://a", data=None, timeout=30)

    def test_falls_back_to_next_url_within_one_attempt(self):
        """A failing first URL moves on to the next URL in the same attempt."""
        bad = _make_response(500, "server error")
        good = _make_response(200)
        with patch("mwax_mover.net.webservice.requests.request", side_effect=[bad, good]) as mock_request:
            result = call_webservice(123, ["http://a", "http://b"], None)

        assert result is good
        assert mock_request.call_count == 2

    def test_retries_the_whole_list_and_succeeds_on_second_attempt(self):
        """Every URL failing on attempt 1 triggers a full retry, which then succeeds."""
        bad = _make_response(500, "server error")
        good = _make_response(200)
        with (
            patch("mwax_mover.net.webservice.requests.request", side_effect=[bad, bad, good]) as mock_request,
            patch("time.sleep") as mock_sleep,
        ):
            result = call_webservice(123, ["http://a", "http://b"], None, max_retries=2)

        assert result is good
        assert mock_request.call_count == 3
        mock_sleep.assert_called_once_with(30)

    def test_raises_the_original_exception_type_after_exhausting_all_retries(self):
        """Final failure re-raises requests.RequestException, not tenacity's RetryError."""
        bad = _make_response(500, "server error")
        with (
            patch("mwax_mover.net.webservice.requests.request", return_value=bad),
            patch("time.sleep"),
        ):
            with pytest.raises(requests.RequestException):
                call_webservice(123, ["http://a"], None, max_retries=2)

    def test_waits_the_fixed_30_seconds_between_every_retry(self):
        """Each retry is preceded by exactly a 30-second wait, regardless of attempt number."""
        bad = _make_response(500, "server error")
        with (
            patch("mwax_mover.net.webservice.requests.request", return_value=bad),
            patch("time.sleep") as mock_sleep,
        ):
            with pytest.raises(requests.RequestException):
                call_webservice(123, ["http://a"], None, max_retries=3)

        assert mock_sleep.call_args_list == [((30,),), ((30,),)]

    def test_max_retries_must_be_at_least_one(self):
        """max_retries=0 is rejected before any HTTP call is attempted."""
        with patch("mwax_mover.net.webservice.requests.request") as mock_request:
            with pytest.raises(ValueError):
                call_webservice(123, ["http://a"], None, max_retries=0)

        mock_request.assert_not_called()

    def test_passes_method_and_timeout_through(self):
        """A caller's method/timeout override reaches the underlying requests.request call."""
        response = _make_response(200)
        with patch("mwax_mover.net.webservice.requests.request", return_value=response) as mock_request:
            call_webservice(123, ["http://a"], {"key": "value"}, timeout=60, method="POST")

        mock_request.assert_called_once_with("POST", "http://a", data={"key": "value"}, timeout=60)

    def test_a_single_max_retries_does_not_retry_at_all(self):
        """max_retries=1 (e.g. an outer loop already retrying) fails fast with one attempt."""
        bad = _make_response(500, "server error")
        with (
            patch("mwax_mover.net.webservice.requests.request", return_value=bad) as mock_request,
            patch("time.sleep") as mock_sleep,
        ):
            with pytest.raises(requests.RequestException):
                call_webservice(123, ["http://a"], None, max_retries=1)

        mock_request.assert_called_once()
        mock_sleep.assert_not_called()
