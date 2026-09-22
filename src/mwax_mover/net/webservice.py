"""Calling MWA web service HTTP endpoints with URL-list fallback and retry.

call_webservice() is the generic building block: it tries each URL in
url_list in order, retrying the whole list up to max_retries times (via
tenacity, with a fixed 30-second wait between attempts) -- but only when a
failure is transient enough that retrying might help. Callers that need a
specific endpoint (e.g. metafits download, or a data-file listing) build on
this -- see fits.metafits and filesystem.naming respectively. The
data-file-listing queries live in filesystem.naming rather than here because
they depend on MWADataFileType; keeping them here would create an import
cycle (see filesystem.naming's docstring).
"""

import http
import logging

import requests
from tenacity import Retrying, retry_if_exception, stop_after_attempt

from mwax_mover.core.timing import retry_wait_fixed

logger = logging.getLogger(__name__)

# requests transport exceptions that reflect a transient network/server
# condition and are therefore worth retrying the whole url_list for.
# ConnectTimeout/ReadTimeout subclass Timeout, and ConnectTimeout also
# subclasses ConnectionError, so both are covered here. Deterministic requests
# errors (malformed URL/schema, redirect loops, ...) are intentionally absent:
# retrying the same inputs would fail the same way.
TRANSIENT_REQUEST_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)

# Non-200 HTTP status codes worth retrying (server-side / rate-limit). Any
# other non-200 is treated as a deterministic client/response error and is not
# retried.
TRANSIENT_STATUS_CODES = frozenset(
    {
        http.HTTPStatus.REQUEST_TIMEOUT,  # 408
        http.HTTPStatus.TOO_MANY_REQUESTS,  # 429
        http.HTTPStatus.INTERNAL_SERVER_ERROR,  # 500
        http.HTTPStatus.BAD_GATEWAY,  # 502
        http.HTTPStatus.SERVICE_UNAVAILABLE,  # 503
        http.HTTPStatus.GATEWAY_TIMEOUT,  # 504
    }
)


def _is_transient(exc: BaseException) -> bool:
    """Whether a failed attempt is worth retrying the whole url_list for.

    Used as call_webservice's tenacity retry predicate (via
    ``retry_if_exception``). Returns True for transient transport exceptions
    (``TRANSIENT_REQUEST_EXCEPTIONS``) and for a ``requests.HTTPError`` carrying
    a transient status code (``TRANSIENT_STATUS_CODES``). Returns False for
    deterministic ``requests`` errors (bad URL/schema, 4xx client errors, ...)
    and -- since call_webservice_inner only ever raises ``requests`` exceptions
    for URL failures -- for any non-``requests`` exception that reaches it (e.g.
    a programming error), so those propagate immediately instead of being
    retried and masked.
    """
    if isinstance(exc, TRANSIENT_REQUEST_EXCEPTIONS):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in TRANSIENT_STATUS_CODES
    return False


def call_webservice(
    obs_id: int,
    url_list: list[str],
    data,
    max_retries: int = 3,
    timeout: int = 30,
    method: str = "GET",
    retry_wait_seconds: int = 30,
) -> requests.Response:
    """
    Call a list of MWA web service URLs in order, retrying on transient failures.

    Iterates through ``url_list`` on each attempt, returning immediately on an
    HTTP 200 response. Every URL is tried regardless of how earlier ones failed
    (the list is a fallback chain); a ``requests`` exception or a non-200 status
    code simply moves on to the next URL. If all URLs fail, the attempt as a
    whole fails and the whole sequence is retried up to ``max_retries`` times,
    with a fixed wait between attempts (``retry_wait_seconds``) (via tenacity) -- but *only*
    when at least one failure was transient (see ``_is_transient``). If every
    failure was deterministic (a malformed URL, a 4xx client error, ...),
    retrying cannot help, so the call fails immediately without further retries.

    Any exception that is not a ``requests`` exception (e.g. a programming
    error) is neither caught for URL-fallback nor retried: it propagates
    immediately with its original type and traceback, rather than being
    swallowed and eventually re-surfaced as a generic request failure.

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
        retry_wait_seconds: number of seconds to wait between retries.
            Default is 30 seconds.

    Returns:
        The first successful ``requests.Response`` object (HTTP status 200).

    Raises:
        ValueError: If max_retries is not at least 1.
        requests.RequestException: If every URL fails. The concrete subclass
            reflects the failure and drives the retry decision: a transient
            transport exception (connection error / timeout), or a
            ``requests.HTTPError`` carrying a transient status code, when
            retrying may help; otherwise a deterministic ``requests`` exception
            or ``HTTPError``, raised immediately without retrying. This is the
            failure type callers should catch.
        Exception: Any non-``requests`` exception raised during an attempt
            (e.g. a ``TypeError`` from a programming error) is not caught for
            URL-fallback and does not satisfy the retry predicate, so it
            propagates immediately -- on the first attempt -- with its original
            type and traceback preserved.
    """

    def call_webservice_inner(obs_id: int, url_list: list[str], data) -> requests.Response:
        """Try each URL in url_list until one returns HTTP 200.

        Every URL is attempted regardless of how earlier ones failed (the list
        is a fallback chain). If none returns 200, the most retry-worthy failure
        is raised: a transient one (a transient transport exception, or an
        ``HTTPError`` with a transient status code) if any URL hit one,
        otherwise a deterministic one. ``_is_transient`` then uses that
        exception to decide whether the enclosing tenacity policy retries the
        whole list. Non-``requests`` exceptions are not caught here and
        propagate to the caller unchanged.

        Returns:
            The first successful ``requests.Response`` object (HTTP status 200).

        Raises:
            requests.RequestException: If every URL in url_list fails (see the
                outer docstring for which concrete type, and why).
        """
        transient_exc: requests.RequestException | None = None
        permanent_exc: requests.RequestException | None = None

        for url in url_list:
            logger.debug(f"{obs_id}: trying {url} with data ({'' if data is None else data})")

            try:
                response = requests.request(method, url, data=data, timeout=timeout)
            except requests.RequestException as exc:
                # A requests/transport-layer failure on this URL: classify it,
                # remember it, and fall through to the next URL. Anything that
                # is NOT a requests exception (e.g. a programming error) is
                # deliberately not caught here -- it propagates out unretried,
                # with its original type and traceback.
                if isinstance(exc, TRANSIENT_REQUEST_EXCEPTIONS):
                    logger.warning(f"{obs_id}: transient error from {url}: {exc}")
                    transient_exc = exc
                else:
                    logger.error(f"{obs_id}: permanent request error from {url}: {exc}")
                    permanent_exc = exc
                continue

            if response.status_code == http.HTTPStatus.OK:
                logger.debug(f"{obs_id}: returned 200 (success)")
                return response

            # Non-200: represent it as an HTTPError (carrying the response) so
            # both failure channels -- transport exceptions and bad statuses --
            # flow through _is_transient uniformly. Classify by status code,
            # then try the next URL.
            http_exc = requests.HTTPError(
                f"{obs_id}: returned {response.status_code} {response.text}", response=response
            )
            if response.status_code in TRANSIENT_STATUS_CODES:
                logger.warning(f"{obs_id}: transient status {response.status_code} from {url}")
                transient_exc = http_exc
            else:
                logger.error(f"{obs_id}: permanent status {response.status_code} {response.text} from {url}")
                permanent_exc = http_exc

        # Every URL failed. Prefer surfacing a transient failure so the retry
        # policy gets a chance to help; only when *every* failure was
        # deterministic do we raise a permanent one, which _is_transient rejects
        # so tenacity does not retry.
        chosen = transient_exc or permanent_exc
        logger.error(f"{obs_id}: failed after trying {len(url_list)} url(s).")
        if chosen is None:
            # url_list was empty -- no attempt was made. Deterministic caller
            # error; not retried.
            raise requests.RequestException(f"{obs_id}: call_webservice()- no URLs to try.")
        raise chosen

    if max_retries <= 0:
        raise ValueError("max_retries must be >=1")

    def _log_before_sleep(retry_state) -> None:
        logger.warning(
            f"{obs_id}: attempt {retry_state.attempt_number}/{max_retries} failed "
            f"({retry_state.outcome.exception()}). Retrying."
        )

    # max_retries is a per-call parameter (see the Args entry above), so this
    # uses tenacity's imperative Retrying rather than the @retry decorator,
    # which is configured once at decoration time. retry=retry_if_exception is
    # scoped by _is_transient, so only transient failures are retried; a
    # deterministic requests error (or a non-requests programming error) fails
    # the predicate and is re-raised immediately by tenacity with its original
    # type and traceback, instead of being retried and finally masked as a
    # generic requests error. reraise=True re-raises call_webservice_inner's own
    # exception on final give-up rather than wrapping it in tenacity's
    # RetryError -- callers catch requests.RequestException, per the Raises: section.
    retrying = Retrying(
        stop=stop_after_attempt(max_retries),
        wait=retry_wait_fixed(retry_wait_seconds),
        retry=retry_if_exception(_is_transient),
        before_sleep=_log_before_sleep,
        reraise=True,
    )

    try:
        return retrying(call_webservice_inner, obs_id, url_list, data)
    except requests.RequestException:
        # Reached only when every URL failed (transient failures exhausted their
        # retries, or a deterministic failure gave up on the first pass). Non-
        # requests exceptions bypass this handler and propagate untouched.
        logger.error(f"{obs_id}: giving up (no successful response).")
        raise
