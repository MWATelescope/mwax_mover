"""Tests for the mwax_wsclean_preview command."""

import numpy as np
import pytest
from astropy.io import fits

from mwax_mover.calvin.plots.wsclean_preview import PREVIEW_LIMIT_SIGMA, PreviewStretch
from mwax_mover.cli import mwax_wsclean_preview as tool
from mwax_mover.constants import EXIT_FAILURE
from mwax_mover.filesystem.files import get_png_dimensions

SIZE = 32


def _write_fits(path) -> str:
    data = np.random.default_rng(1).normal(0.0, 0.01, (SIZE, SIZE)).astype(np.float32)
    fits.PrimaryHDU(data).writeto(path)
    return str(path)


def test_the_label_is_the_polarisation_in_the_file_name_or_else_the_file_name():
    assert tool.label_for("/a/1234567890_wsclean-MFS-XX-image.fits") == "XX"
    assert tool.label_for("/a/1234567890_wsclean-YY-residual.fits") == "YY"
    assert tool.label_for("/a/something.fits") == "something.fits"


def test_the_defaults_are_the_values_that_the_processor_uses():
    args = tool.parse_args(["-o", "out.png", "a.fits"])

    assert PreviewStretch(args.limit_sigma, args.negative_limit_sigma, args.linear_sigma) == PreviewStretch()
    assert args.limit_sigma == PREVIEW_LIMIT_SIGMA


def test_it_makes_the_png_from_two_images(tmp_path):
    xx = _write_fits(tmp_path / "1234567890_wsclean-MFS-XX-image.fits")
    yy = _write_fits(tmp_path / "1234567890_wsclean-MFS-YY-image.fits")
    png = tmp_path / "preview.png"

    tool.main(["-o", str(png), "--limit-sigma", "20", "--linear-sigma", "2", xx, yy])

    assert get_png_dimensions(str(png)) == (1400, 700)


def test_it_fails_if_two_images_have_the_same_label(tmp_path):
    a = _write_fits(tmp_path / "a_wsclean-MFS-XX-image.fits")
    b = _write_fits(tmp_path / "b_wsclean-MFS-XX-image.fits")

    with pytest.raises(SystemExit) as exit_info:
        tool.main(["-o", str(tmp_path / "preview.png"), a, b])

    assert exit_info.value.code == EXIT_FAILURE


def test_it_fails_with_a_stretch_value_that_is_not_above_zero(tmp_path):
    xx = _write_fits(tmp_path / "a_wsclean-MFS-XX-image.fits")

    with pytest.raises(SystemExit) as exit_info:
        tool.main(["-o", str(tmp_path / "preview.png"), "--linear-sigma", "0", xx])

    assert exit_info.value.code == EXIT_FAILURE


def test_it_fails_and_makes_no_png_if_the_image_cannot_be_read(tmp_path):
    bad = tmp_path / "a_wsclean-MFS-XX-image.fits"
    bad.write_text("not fits")
    png = tmp_path / "preview.png"

    with pytest.raises(SystemExit) as exit_info:
        tool.main(["-o", str(png), str(bad)])

    assert exit_info.value.code == EXIT_FAILURE
    assert not png.exists()
