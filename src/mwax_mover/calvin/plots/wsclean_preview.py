"""A PNG preview of the main WSClean image, to publish next to the FITS files.

find_main_image() picks the image (the MFS image, or the single image if WSClean made one output
channel). make_preview_png() draws it with matplotlib, with sky coordinates if the FITS header has
them. make_wsclean_preview() does both for a job and never raises: a preview is a convenience, so a
problem is logged and the job carries on.
"""

import logging
import os

import numpy as np
from astropy.io import fits
from astropy.visualization import AsinhStretch, ImageNormalize, PercentileInterval
from astropy.wcs import WCS
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure

from mwax_mover.constants import WSCLEAN_IMAGE_SUFFIX, WSCLEAN_MFS_IMAGE_SUFFIX, WSCLEAN_NAME_SUFFIX

logger = logging.getLogger(__name__)

# How the image is drawn. The colour scale runs from the lowest value to this percentile of the
# image, so a few very bright sources do not turn the rest of the image black. The asinh stretch
# (softening parameter below) keeps the faint structure visible.
PREVIEW_PERCENTILE = 99.5
PREVIEW_ASINH_A = 0.1
PREVIEW_COLORMAP = "viridis"
PREVIEW_FIGURE_SIZE_INCHES = (8.0, 8.0)
PREVIEW_DPI = 100


def find_main_image(job_output_path: str, obs_id: int) -> str | None:
    """Find the WSClean image to make the preview from.

    Args:
        job_output_path: The job's output directory, where WSClean wrote its files.
        obs_id: The observation ID.

    Returns:
        The full path of ``<obs_id>_wsclean-MFS-image.fits`` if it exists, otherwise of
        ``<obs_id>_wsclean-image.fits``, otherwise None.
    """
    prefix = os.path.join(job_output_path, f"{obs_id}{WSCLEAN_NAME_SUFFIX}")

    for suffix in (WSCLEAN_MFS_IMAGE_SUFFIX, WSCLEAN_IMAGE_SUFFIX):
        candidate = f"{prefix}{suffix}"
        if os.path.isfile(candidate):
            return candidate

    return None


def make_preview_png(fits_filename: str, png_filename: str, title: str) -> bool:
    """Draw a WSClean FITS image as a PNG.

    Args:
        fits_filename: The WSClean image. It can have 2 to 4 axes, but only 2 can be longer than 1
            (WSClean writes frequency and polarization axes of length 1).
        png_filename: The PNG to write.
        title: The title of the plot.

    Returns:
        True if the PNG was written, False otherwise (the reason is logged). A partly written PNG is removed.
    """
    try:
        with fits.open(fits_filename, memmap=False) as hdul:
            header = hdul[0].header
            data = np.squeeze(np.asarray(hdul[0].data, dtype=np.float32))

        if data.ndim != 2:
            logger.warning(f"{fits_filename}: expected a 2D image, got shape {data.shape}. No preview made.")
            return False

        if not np.isfinite(data).any():
            logger.warning(f"{fits_filename}: the image has no valid pixels. No preview made.")
            return False

        sky_wcs = WCS(header, naxis=2)
        has_sky = bool(sky_wcs.has_celestial)

        norm = ImageNormalize(
            data,
            interval=PercentileInterval(PREVIEW_PERCENTILE),
            stretch=AsinhStretch(PREVIEW_ASINH_A),
        )

        # Use the Figure class directly, not pyplot, so there is no global state to share between threads.
        fig = Figure(figsize=PREVIEW_FIGURE_SIZE_INCHES)
        FigureCanvasAgg(fig)
        ax = fig.add_subplot(projection=sky_wcs) if has_sky else fig.add_subplot()
        image = ax.imshow(data, origin="lower", cmap=PREVIEW_COLORMAP, norm=norm)
        ax.set_title(title)
        if has_sky:
            ax.grid(color="white", alpha=0.3)
            ax.set_xlabel("Right ascension")
            ax.set_ylabel("Declination")
        else:
            ax.set_xlabel("x (pixels)")
            ax.set_ylabel("y (pixels)")

        unit = str(header.get("BUNIT", "")).strip()
        fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04, label=unit)

        fig.savefig(png_filename, dpi=PREVIEW_DPI)
        return True

    except Exception:  # noqa: BLE001 - a preview is optional; one bad image must not stop the job
        logger.exception(f"Could not make a preview PNG from {fits_filename}")
        try:
            if os.path.exists(png_filename):
                os.remove(png_filename)
        except OSError:
            logger.warning(f"Could not remove the partial PNG {png_filename}. Ignoring.")
        return False


def make_wsclean_preview(job_output_path: str, obs_id: int) -> str | None:
    """Make the preview PNG of a job's main WSClean image, next to the FITS file.

    The PNG has the FITS file's name with ``.png`` instead of ``.fits``.

    Args:
        job_output_path: The job's output directory, where WSClean wrote its files.
        obs_id: The observation ID.

    Returns:
        The full path of the PNG, or None if there is no image or the PNG could not be made.
        This never raises.
    """
    fits_filename = find_main_image(job_output_path, obs_id)
    if fits_filename is None:
        logger.warning(f"{obs_id}: no WSClean image found in {job_output_path}. No preview made.")
        return None

    png_filename = os.path.splitext(fits_filename)[0] + ".png"
    title = os.path.basename(fits_filename)

    if make_preview_png(fits_filename, png_filename, title):
        logger.info(f"{obs_id}: made preview {png_filename}")
        return png_filename

    return None
