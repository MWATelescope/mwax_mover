"""Test for version.get_mwax_mover_version_string(), a top-level module
alongside constants.py, so its test stays at the tests/ root too.
"""

from mwax_mover import version


def test_version():
    assert len(version.get_mwax_mover_version_string()) > 0


def test_beamformer_version_from_binary(tmp_path):
    """The beamformer version is the stripped stdout of `<binary> -V`."""
    binary = tmp_path / "mwax_beamformer"
    binary.write_text("#!/bin/sh\necho 'v1.2.3'\n", encoding="utf-8")
    binary.chmod(0o755)

    assert version.get_mwax_beamformer_version(str(binary)) == "v1.2.3"


def test_beamformer_version_unknown_if_binary_missing(tmp_path):
    assert version.get_mwax_beamformer_version(str(tmp_path / "nope")) == version.UNKNOWN_BEAMFORMER_VERSION
