"""A PNG preview of the main WSClean images, to publish next to the FITS files.

The preview is made to show how good the calibration is. Calibration errors show up as ghost
sources, stripes and rings around bright sources, and as a raised noise floor. These are faint
compared with the peak, so the colour scale is based on the noise, not on the lowest and highest
pixel:

- The noise (sigma) is the robust rms of the pixels (the median absolute deviation, MAD, so that
  sources and artefacts have little effect on it).
- The colour scale is symmetric around zero in shape (zero is the centre of a diverging colour map),
  and runs from -PREVIEW_NEGATIVE_LIMIT_SIGMA to +PREVIEW_LIMIT_SIGMA times sigma. Pixels outside
  that range have the end colour (the colour bar has an arrow at each end).
- The colour scale is linear for values within +/- PREVIEW_LINEAR_SIGMA times sigma and
  compresses (asinh) beyond that, so the noise can be seen next to a bright source.
- XX and YY are drawn side by side with the same colour scale (found from both images together),
  so they can be compared.
- The title of each panel has the peak, sigma and the dynamic range (peak / sigma), so there is
  also a number for the quality.

find_main_images() finds the images (the MFS image of each polarisation, or the single image if WSClean
made one output channel). make_preview_png() draws them with matplotlib, with sky coordinates if the
FITS header has them. make_wsclean_preview() does both for a job and never raises: a preview is a
convenience, so a problem is logged and the job carries on.
"""

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.colors import FuncNorm
from matplotlib.figure import Figure

from mwax_mover.constants import (
    WSCLEAN_IMAGE_FILE_SUFFIX,
    WSCLEAN_MFS_NAME_PART,
    WSCLEAN_NAME_SUFFIX,
    WSCLEAN_POLARIZATION_NAMES,
    WSCLEAN_PREVIEW_PNG_SUFFIX,
)

logger = logging.getLogger(__name__)

# How the images are drawn. These are first guesses, to be tuned on real images (the
# mwax_wsclean_preview command takes them as options, so they can be tried without changing code).
# The colour scale goes from -PREVIEW_NEGATIVE_LIMIT_SIGMA to +PREVIEW_LIMIT_SIGMA times the noise.
PREVIEW_LIMIT_SIGMA = 50.0
PREVIEW_NEGATIVE_LIMIT_SIGMA = 5.0
# The colour scale is linear within this many sigma of zero, and asinh beyond.
PREVIEW_LINEAR_SIGMA = 1.0
# Standard deviation of Gaussian noise = this * the median absolute deviation.
PREVIEW_MAD_TO_SIGMA = 1.4826
PREVIEW_COLORMAP = "RdBu_r"
PREVIEW_FIGURE_SIZE_INCHES = (14.0, 7.0)
PREVIEW_DPI = 100


@dataclass(frozen=True)
class PreviewStretch:
    """The colour scale of the preview, in multiples of the noise (sigma).

    Attributes:
        limit_sigma: The top of the colour scale is this many sigma above zero.
        negative_limit_sigma: The bottom of the colour scale is this many sigma below zero.
        linear_sigma: The colour scale is linear within this many sigma of zero, and asinh beyond.
    """

    limit_sigma: float = PREVIEW_LIMIT_SIGMA
    negative_limit_sigma: float = PREVIEW_NEGATIVE_LIMIT_SIGMA
    linear_sigma: float = PREVIEW_LINEAR_SIGMA

    def validate(self) -> None:
        """Check that the values can make a colour scale.

        Raises:
            ValueError: If a value is not above zero.
        """
        for name, value in (
            ("limit_sigma", self.limit_sigma),
            ("negative_limit_sigma", self.negative_limit_sigma),
            ("linear_sigma", self.linear_sigma),
        ):
            if not value > 0:  # also false for NaN
                raise ValueError(f"{name} must be above zero, got {value}.")


@dataclass
class _PreviewImage:
    """One image to draw: its label, pixels and FITS header."""

    label: str
    data: np.ndarray
    header: fits.Header


def find_main_images(job_output_path: str, obs_id: int) -> dict[str, str]:
    """Find the WSClean images to make the preview from.

    Args:
        job_output_path: The job's output directory, where WSClean wrote its files.
        obs_id: The observation ID.

    Returns:
        For each polarisation that has an image, in the order of WSCLEAN_POLARIZATION_NAMES: the
        full path of ``<obs_id>_wsclean-MFS-<POL>-image.fits`` if it exists, otherwise of
        ``<obs_id>_wsclean-<POL>-image.fits``. A polarisation with neither is left out.
    """
    prefix = os.path.join(job_output_path, f"{obs_id}{WSCLEAN_NAME_SUFFIX}")
    images: dict[str, str] = {}

    for pol in WSCLEAN_POLARIZATION_NAMES:
        for name_part in (WSCLEAN_MFS_NAME_PART, ""):
            candidate = f"{prefix}{name_part}-{pol}{WSCLEAN_IMAGE_FILE_SUFFIX}"
            if os.path.isfile(candidate):
                images[pol] = candidate
                break

    return images


def robust_sigma(data: np.ndarray) -> float:
    """Estimate the noise of an image from the median absolute deviation (MAD).

    Sources and artefacts change the MAD much less than they change the standard deviation.

    Args:
        data: The pixels. Pixels that are not finite are ignored.

    Returns:
        The noise in the units of the data. If the MAD is zero (for example a flat image) this is
        the largest absolute value, or 1.0 for an image of zeros, so that it is always above zero.

    Raises:
        ValueError: If there are no finite pixels.
    """
    values = np.asarray(data, dtype=np.float64)
    values = values[np.isfinite(values)]
    if values.size == 0:
        raise ValueError("The image has no valid pixels.")

    sigma = PREVIEW_MAD_TO_SIGMA * float(np.median(np.abs(values - np.median(values))))
    if sigma > 0:
        return sigma

    return float(np.max(np.abs(values))) or 1.0


def make_norm(sigma: float, stretch: PreviewStretch) -> FuncNorm:
    """Make the colour scale.

    Zero is the middle of the colour map. Above zero the scale is asinh up to
    ``stretch.limit_sigma * sigma``, and below zero it is asinh down to
    ``-stretch.negative_limit_sigma * sigma``. Each side has the same number of colours.

    Args:
        sigma: The noise, in the units of the data (see robust_sigma()).
        stretch: The limits and the width of the linear part.

    Returns:
        The matplotlib normalisation.
    """
    linear_width = stretch.linear_sigma * sigma
    vmax = stretch.limit_sigma * sigma
    vmin = -stretch.negative_limit_sigma * sigma
    top = float(np.arcsinh(vmax / linear_width))
    bottom = float(np.arcsinh(-vmin / linear_width))

    def forward(x: np.ndarray) -> np.ndarray:
        values = np.asarray(x, dtype=np.float64)
        compressed = np.arcsinh(values / linear_width)
        return np.where(values >= 0, compressed / top, compressed / bottom)

    def inverse(y: np.ndarray) -> np.ndarray:
        values = np.asarray(y, dtype=np.float64)
        return np.where(values >= 0, linear_width * np.sinh(values * top), linear_width * np.sinh(values * bottom))

    return FuncNorm((forward, inverse), vmin=vmin, vmax=vmax)


def _read_image(label: str, fits_filename: str) -> _PreviewImage | None:
    """Read a WSClean image.

    Args:
        label: The name to show for the image (e.g. "XX").
        fits_filename: The WSClean image. It can have 2 to 4 axes, but only 2 can be longer than 1
            (WSClean writes frequency and polarization axes of length 1).

    Returns:
        The image, or None (with the reason logged) if it is not a 2D image or has no valid pixels.
    """
    with fits.open(fits_filename, memmap=False) as hdul:
        header = hdul[0].header.copy()
        data = np.squeeze(np.asarray(hdul[0].data, dtype=np.float32))

    if data.ndim != 2:  # noqa: PLR2004 - an image has two axes
        logger.warning(f"{fits_filename}: expected a 2D image, got shape {data.shape}. No preview made.")
        return None

    if not np.isfinite(data).any():
        logger.warning(f"{fits_filename}: the image has no valid pixels. No preview made.")
        return None

    return _PreviewImage(label, data, header)


def make_preview_png(
    fits_filenames: Mapping[str, str],
    png_filename: str,
    title: str,
    stretch: PreviewStretch | None = None,
) -> bool:
    """Draw WSClean FITS images side by side as one PNG.

    All the images have the same colour scale. It is worked out from the noise of all of them together.

    Args:
        fits_filenames: The images to draw, as {label: FITS filename}, left to right. Each can have
            2 to 4 axes, but only 2 can be longer than 1.
        png_filename: The PNG to write.
        title: The title of the whole figure.
        stretch: The colour scale. If None, the PREVIEW_* defaults.

    Returns:
        True if the PNG was written, False otherwise (the reason is logged). A partly written PNG is removed.
    """
    try:
        stretch = stretch or PreviewStretch()
        stretch.validate()

        images = [_read_image(label, filename) for label, filename in fits_filenames.items()]
        if not images or any(image is None for image in images):
            logger.warning(f"Could not read all of {list(fits_filenames.values())}. No preview made.")
            return False
        valid_images: list[_PreviewImage] = [image for image in images if image is not None]

        # One colour scale for all of the images, from the pixels of all of them
        sigma = robust_sigma(np.concatenate([image.data.ravel() for image in valid_images]))
        norm = make_norm(sigma, stretch)

        # Use the Figure class directly, not pyplot, so there is no global state to share between threads.
        fig = Figure(figsize=PREVIEW_FIGURE_SIZE_INCHES)
        FigureCanvasAgg(fig)
        fig.suptitle(title)

        axes = []
        mappable = None
        for index, image in enumerate(valid_images, start=1):
            sky_wcs = WCS(image.header, naxis=2)
            has_sky = bool(sky_wcs.has_celestial)
            ax = fig.add_subplot(1, len(valid_images), index, projection=sky_wcs if has_sky else None)
            mappable = ax.imshow(image.data, origin="lower", cmap=PREVIEW_COLORMAP, norm=norm)

            peak = float(np.nanmax(image.data))
            image_sigma = robust_sigma(image.data)
            ax.set_title(
                f"{image.label}: peak {peak:.3g}, sigma {image_sigma:.3g}, peak/sigma {peak / image_sigma:.0f}"
            )
            if has_sky:
                ax.grid(color="gray", alpha=0.3)
                ax.set_xlabel("Right ascension")
                ax.set_ylabel("Declination" if index == 1 else " ")
            else:
                ax.set_xlabel("x (pixels)")
                ax.set_ylabel("y (pixels)")
            axes.append(ax)

        assert mappable is not None  # there is at least one image
        unit = str(valid_images[0].header.get("BUNIT", "")).strip()
        fig.colorbar(
            mappable,
            ax=axes,
            fraction=0.046,
            pad=0.04,
            extend="both",
            label=f"{unit} (zero in the middle, asinh; sigma = {sigma:.3g})",
        )

        fig.savefig(png_filename, dpi=PREVIEW_DPI)
        return True

    except Exception:  # noqa: BLE001 - a preview is optional; one bad image must not stop the job
        logger.exception(f"Could not make a preview PNG from {list(fits_filenames.values())}")
        try:
            if os.path.exists(png_filename):
                os.remove(png_filename)
        except OSError:
            logger.warning(f"Could not remove the partial PNG {png_filename}. Ignoring.")
        return False


def preview_png_filename(job_output_path: str, obs_id: int, mfs: bool) -> str:
    """Name the preview PNG of a job.

    Args:
        job_output_path: The job's output directory.
        obs_id: The observation ID.
        mfs: True if the images are the MFS images.

    Returns:
        The full path: ``<obs_id>_wsclean-MFS-XX-YY-image.png``, or without ``-MFS`` if not ``mfs``.
    """
    name_part = WSCLEAN_MFS_NAME_PART if mfs else ""
    return os.path.join(job_output_path, f"{obs_id}{WSCLEAN_NAME_SUFFIX}{name_part}{WSCLEAN_PREVIEW_PNG_SUFFIX}")


def make_wsclean_preview(job_output_path: str, obs_id: int) -> str | None:
    """Make the preview PNG of a job's main WSClean images, in the job's output directory.

    Args:
        job_output_path: The job's output directory, where WSClean wrote its files.
        obs_id: The observation ID.

    Returns:
        The full path of the PNG (see preview_png_filename()), or None if there is no image or the PNG
        could not be made. This never raises.
    """
    images = find_main_images(job_output_path, obs_id)
    if not images:
        logger.warning(f"{obs_id}: no WSClean image found in {job_output_path}. No preview made.")
        return None

    if len(images) < len(WSCLEAN_POLARIZATION_NAMES):
        logger.warning(f"{obs_id}: only found the WSClean image of {', '.join(images)}. Making the preview from it.")

    mfs_prefix = f"{obs_id}{WSCLEAN_NAME_SUFFIX}{WSCLEAN_MFS_NAME_PART}-"
    mfs = all(os.path.basename(filename).startswith(mfs_prefix) for filename in images.values())
    png_filename = preview_png_filename(job_output_path, obs_id, mfs)
    title = f"{obs_id} WSClean {', '.join(images)}"

    if make_preview_png(images, png_filename, title):
        logger.info(f"{obs_id}: made preview {png_filename}")
        return png_filename

    return None
