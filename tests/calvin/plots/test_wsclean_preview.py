"""Tests for calvin.plots.wsclean_preview: the PNG preview of the main WSClean images (XX and YY)."""

import numpy as np
import pytest
from astropy.io import fits

from mwax_mover.calvin.plots.wsclean_preview import (
    PreviewStretch,
    find_main_images,
    make_norm,
    make_preview_png,
    make_wsclean_preview,
    panel_title,
    preview_png_filename,
    robust_sigma,
)
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


def _images(tmp_path, **kwargs) -> dict[str, str]:
    """Write an XX and a YY image and return them as {label: filename}."""
    images = {}
    for pol in ("XX", "YY"):
        path = tmp_path / f"image_{pol}.fits"
        _write_wsclean_fits(path, **kwargs)
        images[pol] = str(path)
    return images


def test_find_main_images_prefers_the_mfs_images(tmp_path):
    for pol in ("XX", "YY"):
        for name in [f"{OBS}_wsclean-{pol}-image.fits", f"{OBS}_wsclean-MFS-{pol}-image.fits"]:
            (tmp_path / name).write_text("x")
        (tmp_path / f"{OBS}_wsclean-0000-{pol}-image.fits").write_text("x")

    assert find_main_images(str(tmp_path), OBS) == {
        "XX": str(tmp_path / f"{OBS}_wsclean-MFS-XX-image.fits"),
        "YY": str(tmp_path / f"{OBS}_wsclean-MFS-YY-image.fits"),
    }


def test_find_main_images_uses_the_single_images_if_there_are_no_mfs_images(tmp_path):
    (tmp_path / f"{OBS}_wsclean-XX-image.fits").write_text("x")
    (tmp_path / f"{OBS}_wsclean-YY-image.fits").write_text("x")
    (tmp_path / f"{OBS}_wsclean-XX-dirty.fits").write_text("x")

    assert find_main_images(str(tmp_path), OBS) == {
        "XX": str(tmp_path / f"{OBS}_wsclean-XX-image.fits"),
        "YY": str(tmp_path / f"{OBS}_wsclean-YY-image.fits"),
    }


def test_find_main_images_leaves_out_a_polarisation_that_has_no_image(tmp_path):
    (tmp_path / f"{OBS}_wsclean-MFS-YY-image.fits").write_text("x")

    assert list(find_main_images(str(tmp_path), OBS)) == ["YY"]


def test_find_main_images_returns_nothing_if_wsclean_made_no_image(tmp_path):
    (tmp_path / f"{OBS}_wsclean-MFS-XX-dirty.fits").write_text("x")
    (tmp_path / f"{OBS + 1}_wsclean-MFS-XX-image.fits").write_text("x")  # another observation
    (tmp_path / f"{OBS}_wsclean-MFS-image.fits").write_text("x")  # Stokes I: not an XX or YY image

    assert find_main_images(str(tmp_path), OBS) == {}


def test_robust_sigma_of_gaussian_noise_is_close_to_the_noise_and_ignores_a_bright_source():
    rng = np.random.default_rng(2)
    data = rng.normal(0.0, 0.5, (200, 200))
    data[100, 100] = 1e6

    assert robust_sigma(data) == pytest.approx(0.5, rel=0.05)


def test_robust_sigma_ignores_nan_and_is_always_above_zero():
    assert robust_sigma(np.array([np.nan, 2.0, 2.0, 2.0])) == 2.0  # MAD is zero: the largest value
    assert robust_sigma(np.zeros((4, 4))) == 1.0


def test_robust_sigma_with_no_valid_pixels_is_an_error():
    with pytest.raises(ValueError, match="no valid pixels"):
        robust_sigma(np.full((3, 3), np.nan))


def test_the_colour_scale_has_zero_in_the_middle_and_the_limits_in_sigma():
    norm = make_norm(2.0, PreviewStretch(limit_sigma=50.0, negative_limit_sigma=5.0, linear_sigma=1.0))

    assert (norm.vmin, norm.vmax) == (-10.0, 100.0)
    assert float(norm(0.0)) == pytest.approx(0.5)
    assert float(norm(100.0)) == pytest.approx(1.0)
    assert float(norm(-10.0)) == pytest.approx(0.0)


def test_the_colour_scale_gives_more_of_the_colours_to_small_values_than_a_linear_scale():
    norm = make_norm(1.0, PreviewStretch(limit_sigma=50.0, negative_limit_sigma=5.0, linear_sigma=1.0))

    # 2 sigma is 4% of the way to the top, linearly. With asinh it takes more than 4% of the colours.
    assert float(norm(2.0)) - 0.5 > 0.5 * (2.0 / 50.0)


def test_the_colour_scale_is_the_same_going_back_and_forth():
    norm = make_norm(0.01, PreviewStretch())
    values = np.array([-0.05, -0.001, 0.0, 0.002, 0.3, 0.5])

    assert norm.inverse(norm(values)) == pytest.approx(values)


@pytest.mark.parametrize("name", ["limit_sigma", "negative_limit_sigma", "linear_sigma"])
@pytest.mark.parametrize("value", [0.0, -1.0, float("nan")])
def test_a_stretch_value_that_is_not_above_zero_is_an_error(name, value):
    with pytest.raises(ValueError, match=name):
        PreviewStretch(**{name: value}).validate()


def test_make_preview_png_draws_the_images_side_by_side_with_sky_coordinates(tmp_path):
    png_file = tmp_path / "image.png"

    assert make_preview_png(_images(tmp_path), str(png_file), "title") is True

    width, height = get_png_dimensions(str(png_file))
    assert (width, height) == (1400, 700)


def test_make_preview_png_draws_one_image(tmp_path):
    png_file = tmp_path / "image.png"
    images = _images(tmp_path)

    assert make_preview_png({"XX": images["XX"]}, str(png_file), "title") is True
    assert png_file.exists()


def test_make_preview_png_without_sky_coordinates_or_extra_axes(tmp_path):
    png_file = tmp_path / "image.png"

    assert make_preview_png(_images(tmp_path, sky=False, four_axes=False), str(png_file), "title") is True
    assert png_file.exists()


def test_make_preview_png_copes_with_nan_pixels(tmp_path):
    data = np.full((SIZE, SIZE), 0.01, dtype=np.float32)
    data[0, 0] = 0.02  # not flat, so the noise is not zero
    data[:10, 1:] = np.nan
    png_file = tmp_path / "image.png"

    assert make_preview_png(_images(tmp_path, data=data), str(png_file), "title") is True


def test_make_preview_png_copes_with_a_flat_image(tmp_path):
    png_file = tmp_path / "image.png"

    assert (
        make_preview_png(_images(tmp_path, data=np.zeros((SIZE, SIZE), dtype=np.float32)), str(png_file), "t") is True
    )


def test_make_preview_png_uses_the_stretch_it_is_given(tmp_path, monkeypatch):
    seen = []
    real_make_norm = make_norm
    monkeypatch.setattr(
        "mwax_mover.calvin.plots.wsclean_preview.make_norm",
        lambda sigma, stretch: seen.append((sigma, stretch)) or real_make_norm(sigma, stretch),
    )
    stretch = PreviewStretch(limit_sigma=7.0, negative_limit_sigma=3.0, linear_sigma=2.0)

    assert make_preview_png(_images(tmp_path), str(tmp_path / "image.png"), "title", stretch) is True

    assert len(seen) == 1  # one colour scale for both images
    assert seen[0][1] == stretch


def test_make_preview_png_fails_cleanly_with_a_bad_stretch(tmp_path):
    png_file = tmp_path / "image.png"

    assert make_preview_png(_images(tmp_path), str(png_file), "title", PreviewStretch(limit_sigma=0.0)) is False
    assert not png_file.exists()


def test_make_preview_png_fails_cleanly_if_an_image_has_no_valid_pixels(tmp_path):
    png_file = tmp_path / "image.png"
    images = _images(tmp_path)
    _write_wsclean_fits(images["YY"], data=np.full((SIZE, SIZE), np.nan, dtype=np.float32))

    assert make_preview_png(images, str(png_file), "title") is False
    assert not png_file.exists()


def test_make_preview_png_fails_cleanly_if_an_image_is_not_2d(tmp_path):
    png_file = tmp_path / "image.png"
    images = _images(tmp_path)
    fits.PrimaryHDU(np.zeros((3, SIZE, SIZE), dtype=np.float32)).writeto(images["XX"], overwrite=True)

    assert make_preview_png(images, str(png_file), "title") is False
    assert not png_file.exists()


def test_make_preview_png_fails_cleanly_for_a_file_that_is_not_fits(tmp_path):
    png_file = tmp_path / "image.png"
    images = _images(tmp_path)
    (tmp_path / "image_YY.fits").write_text("not a fits file")

    assert make_preview_png(images, str(png_file), "title") is False
    assert not png_file.exists()


def test_make_preview_png_with_no_images_makes_no_png(tmp_path):
    assert make_preview_png({}, str(tmp_path / "image.png"), "title") is False


def test_preview_png_filename_with_and_without_mfs():
    assert preview_png_filename("/out", OBS, True) == f"/out/{OBS}_wsclean-MFS-XX-YY-image.png"
    assert preview_png_filename("/out", OBS, False) == f"/out/{OBS}_wsclean-XX-YY-image.png"


def test_make_wsclean_preview_writes_one_png_for_the_mfs_images(tmp_path):
    for pol in ("XX", "YY"):
        _write_wsclean_fits(tmp_path / f"{OBS}_wsclean-MFS-{pol}-image.fits")

    png = make_wsclean_preview(str(tmp_path), OBS)

    assert png == str(tmp_path / f"{OBS}_wsclean-MFS-XX-YY-image.png")
    assert (tmp_path / f"{OBS}_wsclean-MFS-XX-YY-image.png").exists()


def test_make_wsclean_preview_names_the_png_without_mfs_for_single_channel_images(tmp_path):
    for pol in ("XX", "YY"):
        _write_wsclean_fits(tmp_path / f"{OBS}_wsclean-{pol}-image.fits")

    assert make_wsclean_preview(str(tmp_path), OBS) == str(tmp_path / f"{OBS}_wsclean-XX-YY-image.png")


def test_make_wsclean_preview_makes_a_png_from_one_polarisation_if_the_other_is_missing(tmp_path):
    _write_wsclean_fits(tmp_path / f"{OBS}_wsclean-MFS-XX-image.fits")

    assert make_wsclean_preview(str(tmp_path), OBS) == str(tmp_path / f"{OBS}_wsclean-MFS-XX-YY-image.png")


def test_make_wsclean_preview_returns_none_without_an_image_and_does_not_raise(tmp_path):
    assert make_wsclean_preview(str(tmp_path), OBS) is None
    assert make_wsclean_preview(str(tmp_path / "does_not_exist"), OBS) is None


def test_a_panel_title_has_the_label_on_one_line_and_the_numbers_on_the_next():
    assert panel_title("XX", 60.62, 0.157) == "XX\npeak 60.6, sigma 0.157, peak/sigma 386"


def test_a_long_label_is_cut_so_that_neighbouring_titles_do_not_overlap():
    title = panel_title("1477739776_wsclean-MFS-XX-image-and-more-text-after.fits", 1.0, 0.5)
    label = title.splitlines()[0]

    assert len(label) == 40
    assert label.endswith("…")
