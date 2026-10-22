"""Tests for calvin.plots.wsclean_preview: the PNG preview of the main WSClean image."""

import numpy as np
from astropy.io import fits

from mwax_mover.calvin.plots.wsclean_preview import find_main_image, make_preview_png, make_wsclean_preview
from mwax_mover.filesystem.files import get_png_dimensions

OBS = 1234567890
SIZE = 64


def _write_wsclean_fits(path, data=None, sky=True, four_axes=True) -> None:
    """Write a small FITS image like WSClean's: axes x, y, frequency, Stokes."""
    if data is None:
        rng = np.random.default_rng(1)
        data = rng.normal(0.0, 0.01, (SIZE, SIZE)).astype(np.float32)
        data[SIZE // 2, SIZE // 2] = 50.0  # one bright source
    if four_axes:
        data = data.reshape((1, 1, *data.shape))

    hdu = fits.PrimaryHDU(data)
    header = hdu.header
    if sky:
        header["CTYPE1"] = "RA---SIN"
        header["CTYPE2"] = "DEC--SIN"
        header["CRVAL1"] = 60.0
        header["CRVAL2"] = -27.0
        header["CDELT1"] = -0.0125
        header["CDELT2"] = 0.0125
        header["CRPIX1"] = SIZE / 2
        header["CRPIX2"] = SIZE / 2
        header["CUNIT1"] = "deg"
        header["CUNIT2"] = "deg"
    if four_axes:
        header["CTYPE3"] = "FREQ"
        header["CRVAL3"] = 1.8e8
        header["CRPIX3"] = 1.0
        header["CDELT3"] = 3.0e7
        header["CTYPE4"] = "STOKES"
        header["CRVAL4"] = 1.0
        header["CRPIX4"] = 1.0
        header["CDELT4"] = 1.0
    header["BUNIT"] = "JY/BEAM"
    hdu.writeto(path, overwrite=True)


def test_find_main_image_prefers_the_mfs_image(tmp_path):
    for name in [f"{OBS}_wsclean-image.fits", f"{OBS}_wsclean-MFS-image.fits", f"{OBS}_wsclean-0000-image.fits"]:
        (tmp_path / name).write_text("x")

    assert find_main_image(str(tmp_path), OBS) == str(tmp_path / f"{OBS}_wsclean-MFS-image.fits")


def test_find_main_image_uses_the_single_image_if_there_is_no_mfs_image(tmp_path):
    (tmp_path / f"{OBS}_wsclean-image.fits").write_text("x")
    (tmp_path / f"{OBS}_wsclean-dirty.fits").write_text("x")

    assert find_main_image(str(tmp_path), OBS) == str(tmp_path / f"{OBS}_wsclean-image.fits")


def test_find_main_image_returns_none_if_wsclean_made_no_image(tmp_path):
    (tmp_path / f"{OBS}_wsclean-dirty.fits").write_text("x")
    (tmp_path / f"{OBS + 1}_wsclean-MFS-image.fits").write_text("x")  # another observation

    assert find_main_image(str(tmp_path), OBS) is None


def test_make_preview_png_from_a_wsclean_image_with_sky_coordinates(tmp_path):
    fits_file = tmp_path / "image.fits"
    png_file = tmp_path / "image.png"
    _write_wsclean_fits(fits_file)

    assert make_preview_png(str(fits_file), str(png_file), "title") is True

    width, height = get_png_dimensions(str(png_file))
    assert (width, height) == (800, 800)


def test_make_preview_png_without_sky_coordinates_or_extra_axes(tmp_path):
    fits_file = tmp_path / "image.fits"
    png_file = tmp_path / "image.png"
    _write_wsclean_fits(fits_file, sky=False, four_axes=False)

    assert make_preview_png(str(fits_file), str(png_file), "title") is True
    assert png_file.exists()


def test_make_preview_png_copes_with_nan_pixels(tmp_path):
    data = np.full((SIZE, SIZE), 0.01, dtype=np.float32)
    data[:10, :] = np.nan
    fits_file = tmp_path / "image.fits"
    png_file = tmp_path / "image.png"
    _write_wsclean_fits(fits_file, data=data)

    assert make_preview_png(str(fits_file), str(png_file), "title") is True


def test_make_preview_png_fails_cleanly_if_the_image_has_no_valid_pixels(tmp_path):
    fits_file = tmp_path / "image.fits"
    png_file = tmp_path / "image.png"
    _write_wsclean_fits(fits_file, data=np.full((SIZE, SIZE), np.nan, dtype=np.float32))

    assert make_preview_png(str(fits_file), str(png_file), "title") is False
    assert not png_file.exists()


def test_make_preview_png_fails_cleanly_if_the_image_is_not_2d(tmp_path):
    fits_file = tmp_path / "image.fits"
    png_file = tmp_path / "image.png"
    fits.PrimaryHDU(np.zeros((3, SIZE, SIZE), dtype=np.float32)).writeto(fits_file)

    assert make_preview_png(str(fits_file), str(png_file), "title") is False
    assert not png_file.exists()


def test_make_preview_png_fails_cleanly_for_a_file_that_is_not_fits(tmp_path):
    fits_file = tmp_path / "image.fits"
    png_file = tmp_path / "image.png"
    fits_file.write_text("not a fits file")

    assert make_preview_png(str(fits_file), str(png_file), "title") is False
    assert not png_file.exists()


def test_make_wsclean_preview_writes_the_png_next_to_the_fits(tmp_path):
    _write_wsclean_fits(tmp_path / f"{OBS}_wsclean-MFS-image.fits")

    png = make_wsclean_preview(str(tmp_path), OBS)

    assert png == str(tmp_path / f"{OBS}_wsclean-MFS-image.png")
    assert (tmp_path / f"{OBS}_wsclean-MFS-image.png").exists()


def test_make_wsclean_preview_returns_none_without_an_image_and_does_not_raise(tmp_path):
    assert make_wsclean_preview(str(tmp_path), OBS) is None
    assert make_wsclean_preview(str(tmp_path / "does_not_exist"), OBS) is None
