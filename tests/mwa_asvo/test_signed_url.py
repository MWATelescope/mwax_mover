"""Tests for mwa_asvo.signed_url: reading and downloading an MWA ASVO signed URL."""

import http.server
import threading
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import pytest

from mwax_mover.mwa_asvo.signed_url import (
    DownloadError,
    DownloadNotFoundError,
    SignedUrlExpiredError,
    SignedUrlKind,
    _proxies_for,
    download_signed_url,
    parse_signed_url,
)

BASE = "https://projects.pawsey.org.au/mwa-asvo"
QUERY = "?AWSAccessKeyId=abc&Signature=xyz&Expires=1700000000"


# ---------------------------------------------------------------------------
# parse_signed_url
# ---------------------------------------------------------------------------


def test_parse_signed_url_for_raw_visibilities():
    info = parse_signed_url(f"{BASE}/1444927824_1021186_vis.tar{QUERY}")

    assert info.obs_id == 1444927824
    assert info.job_id == 1021186
    assert info.kind is SignedUrlKind.vis
    assert info.tar_filename == "1444927824_1021186_vis.tar"


def test_parse_signed_url_for_measurement_sets():
    info = parse_signed_url(f"{BASE}/1444927824_1021186_ms.tar{QUERY}")

    assert info.kind is SignedUrlKind.ms
    assert info.obs_id == 1444927824
    assert info.job_id == 1021186


@pytest.mark.parametrize(
    "name",
    [
        "1444927824_1021186_vis_ppds.tar",
        "1444927824_1021186_ms.zip",
        "444927824_1021186_vis.tar",  # 9 digit obs ID
        "14449278244_1021186_vis.tar",  # 11 digit obs ID
        "1444927824_vis.tar",  # no job ID
        "1444927824_1021186_cal.tar",  # unknown kind
        "somefile.tar",
    ],
)
def test_parse_signed_url_rejects_other_names(name):
    with pytest.raises(ValueError, match="obs_id"):
        parse_signed_url(f"{BASE}/{name}{QUERY}")


def test_parse_signed_url_rejects_a_url_with_no_file_name():
    with pytest.raises(ValueError):
        parse_signed_url(f"{BASE}/{QUERY}")


# ---------------------------------------------------------------------------
# _proxies_for
# ---------------------------------------------------------------------------


def test_no_proxy_setting_leaves_it_to_requests():
    assert _proxies_for(f"{BASE}/x.tar", None, "projects.pawsey.org.au") is None


def test_proxy_is_used_for_https_and_http():
    assert _proxies_for(f"{BASE}/x.tar", "http://p:3128", None) == {"https": "http://p:3128", "http": "http://p:3128"}


@pytest.mark.parametrize(
    "no_proxy", ["projects.pawsey.org.au", "pawsey.org.au", "a.b, pawsey.org.au", ".pawsey.org.au"]
)
def test_hosts_in_no_proxy_do_not_use_the_proxy(no_proxy):
    assert _proxies_for(f"{BASE}/x.tar", "http://p:3128", no_proxy) is None


def test_a_host_that_only_ends_with_the_same_letters_still_uses_the_proxy():
    assert _proxies_for(f"{BASE}/x.tar", "http://p:3128", "wsey.org.au") is not None


# ---------------------------------------------------------------------------
# download_signed_url, against a small local HTTP server
# ---------------------------------------------------------------------------

CONTENT = b"0123456789" * 1000

# The waits that the last _download() asked for
SLEEPS: list[int] = []


class _Server:
    """A local HTTP server. `behaviour` is called with the request number (from 1) and gives the response."""

    def __init__(self, behaviour: Callable[[int], tuple[int, bytes, int | None]]):
        self.requests = 0
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - the name http.server needs
                outer.requests += 1
                status, body, declared_length = behaviour(outer.requests)
                self.send_response(status)
                self.send_header("Content-Length", str(len(body) if declared_length is None else declared_length))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002 - the name http.server uses
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/1444927824_1021186_vis.tar"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture(autouse=True)
def _no_proxy_for_localhost(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")


def _download(url: str, dest: Path, attempts: int = 3, timeout_secs: int = 60) -> int:
    """Download with the sleep patched out. The waits that were asked for are kept in SLEEPS."""
    with patch("mwax_mover.mwa_asvo.signed_url.time.sleep") as mock_sleep:
        try:
            return download_signed_url(url, str(dest), attempts, retry_wait_secs=7, timeout_secs=timeout_secs)
        finally:
            SLEEPS[:] = [c.args[0] for c in mock_sleep.call_args_list]


def test_download_writes_the_file(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (200, CONTENT, None)) as server:
        assert _download(server.url, dest) == len(CONTENT)

    assert dest.read_bytes() == CONTENT
    assert server.requests == 1


def test_download_of_an_expired_url_is_not_retried(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (403, b"expired", None)) as server, pytest.raises(SignedUrlExpiredError, match="expired"):
        _download(server.url, dest)

    assert server.requests == 1
    assert not dest.exists()


def test_download_of_a_missing_file_is_not_retried(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (404, b"", None)) as server, pytest.raises(DownloadNotFoundError):
        _download(server.url, dest)

    assert server.requests == 1


def test_download_retries_after_a_server_error_and_waits_between_attempts(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (503, b"", None) if n < 3 else (200, CONTENT, None)) as server:
        assert _download(server.url, dest) == len(CONTENT)

    assert server.requests == 3
    assert SLEEPS == [7, 7]
    assert dest.read_bytes() == CONTENT


def test_download_gives_up_after_the_last_attempt(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (500, b"", None)) as server, pytest.raises(DownloadError, match="after 2 attempt"):
        _download(server.url, dest, attempts=2)

    assert server.requests == 2
    assert SLEEPS == [7]  # no wait after the last attempt
    assert not dest.exists()


def test_download_treats_a_short_file_as_a_failure_and_removes_it(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (200, CONTENT[:100], len(CONTENT))) as server, pytest.raises(DownloadError):
        _download(server.url, dest, attempts=1)

    assert not dest.exists()
    assert server.requests == 1


def test_download_stops_if_it_takes_longer_than_the_time_limit(tmp_path):
    dest = tmp_path / "out.tar"
    with (
        _Server(lambda n: (200, CONTENT, None)) as server,
        patch("mwax_mover.mwa_asvo.signed_url.time.monotonic", side_effect=[0.0, 100.0, 200.0, 300.0]),
        pytest.raises(DownloadError, match="longer than 10 seconds"),
    ):
        _download(server.url, dest, attempts=1, timeout_secs=10)

    assert not dest.exists()


def test_download_attempts_is_at_least_one(tmp_path):
    dest = tmp_path / "out.tar"
    with _Server(lambda n: (200, CONTENT, None)) as server:
        assert _download(server.url, dest, attempts=0) == len(CONTENT)
