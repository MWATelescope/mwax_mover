"""Tests for mwa_asvo.giant_squid: the giant-squid CLI wrapper.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).
"""

import pytest

from mwax_mover.mwa_asvo.giant_squid import extract_filename_from_mwa_asvo_signed_url, run_giant_squid


@pytest.mark.integration
def test_run_giant_squid():
    timeout_secs = 4
    path_to_binary = "../giant-squid/target/release/giant-squid"
    subcmd = "list"
    args = ""

    stdout = run_giant_squid(path_to_binary, subcmd, args, timeout_secs, max_retries=1, retry_delay_seconds=1)
    assert stdout != ""


def test_get_filename_from_url():
    filename_in_url = (
        "https://projects.pawsey org au/mwa-asvo/1444927824_1021186_vis.tar"
        "?AWSAccessKeyId=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        "&Signature=YYYYYYYYYYYYYYYYY%3D&Expires=1777533409"
    )

    assert extract_filename_from_mwa_asvo_signed_url(filename_in_url) == "1444927824_1021186_vis.tar"

    no_filename_url = "https://something.com"
    with pytest.RaisesExc(Exception):
        extract_filename_from_mwa_asvo_signed_url(no_filename_url)
