"""Calling MWA web service HTTP endpoints with URL-list fallback and retry.

call_webservice() is the generic building block: it tries each URL in
url_list in order, retrying the whole list up to max_retries times (via
tenacity, with a fixed 30-second wait between attempts). Callers that need
a specific endpoint (e.g. metafits download, or a data-file listing) build
on this -- see fits.metafits and filesystem.naming respectively. The
data-file-listing queries live in filesystem.naming rather than here
because they depend on MWADataFileType; keeping them here would create an
import cycle (see filesystem.naming's docstring).
"""

import http
import logging

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)


def call_webservice(
    obs_id: int,
    url_list: list[str],
    data,
    max_retries: int = 3,
    timeout: int = 30,
    method: str = "GET",
) -> requests.Response:
    """
    Call a list of MWA web service URLs in order, retrying on transient failures.

    Iterates through ``url_list`` on each attempt, returning immediately on an
    HTTP 200 response. Any other outcome (non-200 status code, network
    exception) causes the next URL to be tried; if all URLs are exhausted the
    attempt fails and the whole sequence is retried up to ``max_retries``
    times, with a fixed 30-second wait between attempts (via tenacity).

    Args:
        obs_id: The MWA observation ID, used only for log messages.
        url_list: Ordered list of URLs to try.
        data: Parameters to pass to the request, or None.
        max_retries: Maximum number of attempts at the whole url_list.
            Defaults to 3. Kept as a parameter (rather than fixed, as the
            30-second wait is) since some callers -- e.g.
            cli/mwax_calvin_processor.py's release_cal_obs, which already
            retries across hosts in its own outer loop -- deliberately pass
            1 to fail fast instead of compounding two retry loops.
        timeout: Timeout to get a response from the server. Defaults to 30.
        method: HTTP method to use, "GET" or "POST". Defaults to "GET".
            Endpoints which change state (rather than just reading it) are
            POST-only, so callers of those must pass "POST".

    Returns:
        The first successful ``requests.Response`` object (HTTP status 200).

    Raises:
        ValueError: If max_retries is not at least 1.
        requests.RequestException: If all URLs fail on every attempt across
            all retries.
    """

    def call_webservice_inner(obs_id: int, url_list: list[str], data) -> requests.Response:
        """Call each url in the list until one returns HTTP 200.

        Returns:
            The first successful ``requests.Response`` object (HTTP status 200).

        Raises:
            requests.RequestException: If every URL in url_list fails.
        """
        i = 0

        while i < len(url_list):
            url: str = url_list[i]

            # we increment i here so the next pass of the loop gets the other url
            # try next url
            i += 1

            logger.debug(f"{obs_id}: trying with {url} with data ({'' if data is None else data})")

            try:
                response = requests.request(method, url, data=data, timeout=timeout)

                if response.status_code == 200:
                    logger.debug(f"{obs_id}: returned 200 (success)")
                    return response

                elif response.status_code >= http.HTTPStatus.BAD_REQUEST and response.status_code <= 599:
                    # 400 - 599 status code- try next url. 599 is left as a literal:
                    # there's no single named constant for "highest possible HTTP status".
                    error_message = f"{obs_id}: returned {response.status_code} {response.text} (failure)"
                    logger.error(error_message)

                else:
                    # Non-200 status code- try next url
                    logger.warning(f"{obs_id}: returned {response.status_code} {response.text} (failure)")

            except Exception:
                # Exception raised- try next url
                logger.exception(f"{obs_id}: exception")

        # We tried all urls without success- up to tenacity to retry now.
        logger.error(f"{obs_id}: failed after trying {len(url_list)} urls.")
        raise requests.RequestException(f"{obs_id}: call_webservice()- failed after trying {len(url_list)} urls.")

    if max_retries <= 0:
        raise ValueError("max_retries must be >=1")

    def _log_before_sleep(retry_state) -> None:
        logger.warning(
            f"{obs_id}: attempt {retry_state.attempt_number}/{max_retries} failed "
            f"({retry_state.outcome.exception()}). Retrying."
        )

    # max_retries is a per-call parameter (see the Args entry above), so this
    # uses tenacity's imperative Retrying rather than the @retry decorator,
    # which is configured once at decoration time. reraise=True re-raises
    # call_webservice_inner's own requests.RequestException on final failure,
    # rather than wrapping it in tenacity's RetryError -- callers catch the
    # former, per this function's own Raises: section.
    retrying = Retrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_fixed(30),
        retry=retry_if_exception_type(Exception),
        before_sleep=_log_before_sleep,
        reraise=True,
    )

    try:
        return retrying(call_webservice_inner, obs_id, url_list, data)
    except Exception:
        logger.error(f"{obs_id}: all {max_retries} attempt(s) failed.")
        raise
