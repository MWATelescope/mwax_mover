"""Tests for the vdif_cat CLI's --beamformer-binary-path option."""

from pathlib import Path

from tests_common import data_path

from mwax_mover.cli import vdif_cat

TEST_METAFITS = data_path("1454343736", "1454343736_metafits.fits")


def test_parse_args_beamformer_binary_path_default():
    args = vdif_cat.parse_args(["-m", "m.fits", "-i", "in", "-o", "out"])

    assert args.beamformer_binary_path is None


def test_parse_args_beamformer_binary_path_given():
    args = vdif_cat.parse_args(["-m", "m.fits", "-i", "in", "-o", "out", "-b", "/some/mwax_beamformer"])

    assert args.beamformer_binary_path == Path("/some/mwax_beamformer")


def test_main_missing_beamformer_binary_fails(tmp_path):
    """A beamformer binary path that does not exist is an error, not a version of Unknown."""
    input_dir = tmp_path / "in"
    input_dir.mkdir()

    result = vdif_cat.main(
        [
            "-m",
            TEST_METAFITS,
            "-i",
            str(input_dir),
            "-o",
            str(tmp_path),
            "-b",
            str(tmp_path / "no_such_mwax_beamformer"),
        ]
    )

    assert result == 1
