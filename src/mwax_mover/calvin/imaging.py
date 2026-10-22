"""The imaging steps of a calibration job, shared by mwax_calvin_processor and mwax_dl_conv_cal_image.

run_imaging_steps() applies the calibration solutions to each band's measurement set (hyperdrive
solutions-apply), images the calibrated bands with WSClean, and makes a PNG preview of the main image.
It uses explicit settings (ImagingSettings), not the state of a class, so that both callers run the
same code.
"""

import dataclasses
import glob
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass

from mwax_mover.calvin.hyperdrive_apply import run_solutions_apply
from mwax_mover.calvin.plots.wsclean_preview import make_wsclean_preview
from mwax_mover.calvin.wsclean import WSCleanArgs, run_wsclean
from mwax_mover.constants import CALIBRATED_MS_SUFFIX, EXT_MS, WSCLEAN_MIN_ABS_MEM_GIB, WSCLEAN_NAME_SUFFIX

logger = logging.getLogger(__name__)


@dataclass
class ImagingSettings:
    """What the imaging steps need.

    Attributes:
        hyperdrive_binary_path: Path to the hyperdrive executable (for solutions-apply).
        apply_timeout: Timeout in seconds for each solutions-apply run.
        wsclean_binary_path: Path to the wsclean executable.
        wsclean_timeout: Timeout in seconds for the WSClean run.
        wsclean_args: The WSClean options. The name, temp_dir and abs_mem_gib are set for each run.
        wsclean_abs_mem_gib: The WSClean memory limit in GiB, or None for no limit. Imaging is
            skipped if it is below WSCLEAN_MIN_ABS_MEM_GIB.
        remove_input_ms: If True, each band's uncalibrated measurement set is removed after
            solutions-apply worked for it, to free working space.
    """

    hyperdrive_binary_path: str
    apply_timeout: int
    wsclean_binary_path: str
    wsclean_timeout: int
    wsclean_args: WSCleanArgs
    wsclean_abs_mem_gib: int | None
    remove_input_ms: bool


def run_imaging_steps(
    obs_id: int,
    metafits_filename: str,
    job_output_path: str,
    working_path: str,
    settings: ImagingSettings,
    on_task: Callable[[str], None] | None = None,
) -> bool:
    """Apply the solutions to each band, then image the calibrated bands with WSClean.

    If solutions-apply fails for any band, WSClean does not run (a partial set of bands is
    not imaged). If the PNG preview fails, that is logged and the result is still True.
    Nothing is raised: any failure is logged and False is returned.

    Args:
        obs_id: Observation ID.
        metafits_filename: Path to the metafits file.
        job_output_path: Directory with the solutions. Readmes, WSClean outputs and the PNG are written here.
        working_path: Directory with the Birli measurement sets. Calibrated ones and WSClean's
            temporary files are written here.
        settings: The imaging settings.
        on_task: Called with the name of each step as it starts (for a status display). Optional.

    Returns:
        True if WSClean ran successfully, False otherwise.
    """

    def task(name: str) -> None:
        if on_task is not None:
            on_task(name)

    try:
        # The Birli measurement sets, not any calibrated ones
        ms_files = sorted(
            ms for ms in glob.glob(os.path.join(working_path, f"*{EXT_MS}")) if not ms.endswith(CALIBRATED_MS_SUFFIX)
        )
        if not ms_files:
            logger.warning(f"{obs_id}: no measurement sets found to image. Skipping imaging.")
            return False

        abs_mem_gib = settings.wsclean_abs_mem_gib
        if abs_mem_gib is not None and abs_mem_gib < WSCLEAN_MIN_ABS_MEM_GIB:
            logger.warning(
                f"{obs_id}: only {abs_mem_gib} GiB is left for WSClean after the measurement sets in"
                " the RAM-backed working path. Skipping imaging."
            )
            return False

        task("Applying solutions")
        apply_success, cal_ms_files = run_solutions_apply(
            ms_files,
            metafits_filename,
            job_output_path,
            working_path,
            obs_id,
            settings.hyperdrive_binary_path,
            settings.apply_timeout,
            settings.remove_input_ms,
        )
        if not apply_success:
            logger.warning(f"{obs_id}: solutions-apply failed. Skipping imaging.")
            return False

        task("WSClean")
        wsclean_args = dataclasses.replace(
            settings.wsclean_args,
            name=os.path.join(job_output_path, f"{obs_id}{WSCLEAN_NAME_SUFFIX}"),
            temp_dir=working_path,
            abs_mem_gib=abs_mem_gib,
        )
        if not run_wsclean(
            cal_ms_files,
            job_output_path,
            obs_id,
            settings.wsclean_binary_path,
            settings.wsclean_timeout,
            wsclean_args,
        ):
            logger.warning(f"{obs_id}: WSClean failed.")
            return False

        # A PNG of the main image, published with the FITS files. If it fails the images are still published.
        task("Preview PNG")
        make_wsclean_preview(job_output_path, obs_id)
    except Exception:  # noqa: BLE001 - imaging is optional; one failure here must not stop the caller
        logger.exception(f"{obs_id}: unhandled exception while imaging.")
        return False

    return True
