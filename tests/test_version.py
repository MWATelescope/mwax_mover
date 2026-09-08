"""Test for version.get_mwax_mover_version_string(), a top-level module
alongside constants.py, so its test stays at the tests/ root too.
"""

from mwax_mover import version


def test_version():
    assert len(version.get_mwax_mover_version_string()) > 0
