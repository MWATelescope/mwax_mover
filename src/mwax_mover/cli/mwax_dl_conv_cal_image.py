"""mwax_dl_conv_cal_image: download, convert, calibrate and image one MWA ASVO observation.

Give it an MWA ASVO signed URL and a directory. It downloads the data, runs Birli (unless the download
already has CASA measurement sets), hyperdrive di-calibrate, the calibration solution processing,
hyperdrive solutions-apply and WSClean, and makes a PNG of the image. All of it happens in the
directory.

This is a test tool. It does not use the database, the upload (S3, plot upload path, index.json),
the MWAX servers or Slurm. See docs/MWAX_DL_CONV_CAL_IMAGE.md for the design.
"""

import argparse
import dataclasses
import glob
import logging
import os
import re
import shutil
import sys
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field

from mwalib import MetafitsContext

from mwax_mover.calvin.birli import estimate_birli_output_bytes, estimate_working_space_gb, run_birli
from mwax_mover.calvin.hyperdrive import run_hyperdrive
from mwax_mover.calvin.imaging import ImagingSettings, run_imaging_steps
from mwax_mover.calvin.local_config import LocalConfigError, LocalSettings, check_files_exist, read_local_config
from mwax_mover.calvin.pipeline import process_solutions_files
from mwax_mover.calvin.wsclean import array_image_settings_from_metafits, calculate_abs_mem_gib
from mwax_mover.constants import (
    CALIBRATED_MS_SUFFIX,
    EXT_MS,
    LOG_FORMAT,
    WSCLEAN_MFS_IMAGE_SUFFIX,
    WSCLEAN_NAME_SUFFIX,
)
from mwax_mover.core.command import write_readme_file
from mwax_mover.core.env import is_ram_backed
from mwax_mover.core.units import BYTES_PER_GIGABYTE, gigabyte_to_gibibyte
from mwax_mover.filesystem.files import extract_tar
from mwax_mover.fits.metafits import download_metafits_file
from mwax_mover.mwa_asvo.signed_url import (
    DownloadError,
    SignedUrlInfo,
    SignedUrlKind,
    download_signed_url,
    parse_signed_url,
)
from mwax_mover.version import get_mwax_mover_version_string

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_STEP_FAILED = 1
EXIT_USAGE = 2

# The steps that --stop-after accepts, in the order they run.
STEP_DOWNLOAD = "download"
STEP_METAFITS = "metafits"
STEP_BIRLI = "birli"
STEP_HYPERDRIVE = "hyperdrive"
STEP_SOLUTIONS = "solutions"
STEP_IMAGING = "imaging"
STOP_AFTER_CHOICES = (STEP_DOWNLOAD, STEP_METAFITS, STEP_BIRLI, STEP_HYPERDRIVE, STEP_SOLUTIONS, STEP_IMAGING)

# Steps that are not in STOP_AFTER_CHOICES because it makes no sense to stop after them.
STEP_DISK_SPACE = "disk space"
STEP_CLEANUP = "cleanup"

INPUT_DIR_NAME = "input"
WORK_DIR_NAME = "work"
OUTPUT_DIR_NAME = "output"
LOG_FILENAME = "mwax_dl_conv_cal_image.log"

STATUS_OK = "ok"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"

# A measurement set from MWA ASVO must be named like the ones Birli makes: <obs_id>.ms, or
# <obs_id>_chN.ms / <obs_id>_chN-M.ms for a picket fence observation. The rest of the pipeline finds
# the solutions by that name.
MS_NAME_PATTERN = r"^{obs_id}(_ch\d{{1,3}}(-\d{{1,3}})?)?\.ms$"


@dataclass
class StepResult:
    """How one step went.

    Attributes:
        name: The step name.
        status: STATUS_OK, STATUS_FAILED or STATUS_SKIPPED.
        seconds: How long it took.
        detail: A short message: why it failed or was skipped, or what it made.
    """

    name: str
    status: str
    seconds: float = 0.0
    detail: str = ""


@dataclass
class Dirs:
    """The directories of a run.

    Attributes:
        input: The extracted download and the metafits.
        work: The measurement sets. It can be on a RAM-backed file system.
        output: The solutions, plots, stats, readmes, WSClean outputs and the PNG.
    """

    input: str
    work: str
    output: str


@dataclass
class Job:
    """The state of one run, filled in as the steps go.

    Attributes:
        settings: The settings from the config file.
        info: What the URL says.
        dirs: The directories.
        url: The signed URL.
        skip_download: True to use the data already in the input directory.
        metafits_filename: The metafits file, once it exists.
        metafits_context: The metafits, read by mwalib.
        work_is_ram_backed: True if the work directory uses RAM.
        birli_max_mem_gib: The Birli memory limit (reduced if the work directory uses RAM).
        wsclean_abs_mem_gib: The WSClean memory limit (reduced if the work directory uses RAM).
    """

    settings: LocalSettings
    info: SignedUrlInfo
    dirs: Dirs
    url: str
    skip_download: bool = False
    metafits_filename: str = ""
    metafits_context: MetafitsContext | None = None
    work_is_ram_backed: bool = False
    birli_max_mem_gib: int = 0
    wsclean_abs_mem_gib: int | None = None
    results: list[StepResult] = field(default_factory=list)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the command line.

    Args:
        argv: The arguments (not including the program name). None means sys.argv.

    Returns:
        The arguments. An argument error exits with code 2 (argparse does this).
    """
    parser = argparse.ArgumentParser(
        prog="mwax_dl_conv_cal_image",
        description=(
            "Download, convert, calibrate and image one MWA ASVO observation, in one directory."
            " This is a test tool: it does not use the database, the upload, the MWAX servers or Slurm."
        ),
    )
    parser.add_argument("--cfg", required=True, help="A calvin_processor.cfg. It gives the program paths and options.")
    parser.add_argument(
        "--url", required=True, help="The MWA ASVO signed URL: <obs_id>_<job_id>_vis.tar or <obs_id>_<job_id>_ms.tar."
    )
    parser.add_argument(
        "--dir",
        required=True,
        help="The directory to download and process in. It has input/, output/ and (by default) work/ in it.",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help=(
            "Where the measurement sets go. The default is <dir>/work. Use a RAM-backed directory such as"
            " /tmp/x for speed (the tool then takes the RAM they use off the Birli and WSClean memory)"
            " or a directory on a disk such as /data/x."
        ),
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Use the data already in <dir>/input (for running again with other options).",
    )
    parser.add_argument(
        "--stop-after",
        choices=STOP_AFTER_CHOICES,
        default=None,
        help="Stop after this step.",
    )
    return parser.parse_args(argv)


def _directory_size_bytes(path: str) -> int:
    """Add up the size of the files in a directory tree.

    Args:
        path: The directory.

    Returns:
        The size in bytes.
    """
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                logger.warning(f"Could not get the size of {os.path.join(root, name)}. Ignoring.")
    return total


def _find_measurement_sets(directory: str, include_calibrated: bool = False) -> list[str]:
    """Find the measurement sets (directories ending .ms) under a directory, not looking inside them.

    Args:
        directory: Where to look.
        include_calibrated: If False, measurement sets that end in _cal.ms are left out.

    Returns:
        The full paths, sorted.
    """
    found: list[str] = []
    for root, dir_names, _files in os.walk(directory):
        for name in list(dir_names):
            if name.endswith(EXT_MS):
                dir_names.remove(name)  # do not look inside it
                if include_calibrated or not name.endswith(CALIBRATED_MS_SUFFIX):
                    found.append(os.path.join(root, name))
    return sorted(found)


def collect_measurement_sets(job: Job) -> list[str]:
    """Put the measurement sets of an ``_ms.tar`` download in the work directory.

    The measurement sets are moved from the input directory. If there are none there but the work
    directory already has some (from an earlier run), those are used.

    Args:
        job: The job.

    Returns:
        The measurement sets in the work directory.

    Raises:
        ValueError: If there is no measurement set, or one has a name the pipeline cannot use.
    """
    name_pattern = re.compile(MS_NAME_PATTERN.format(obs_id=job.info.obs_id))
    in_input = _find_measurement_sets(job.dirs.input)

    if not in_input:
        already_in_work = _find_measurement_sets(job.dirs.work)
        if already_in_work:
            logger.info(f"Using the measurement sets that are already in {job.dirs.work}")
            return already_in_work
        raise ValueError(f"The download has no measurement set (a directory ending {EXT_MS}) in {job.dirs.input}.")

    bad_names = [os.path.basename(ms) for ms in in_input if not name_pattern.match(os.path.basename(ms))]
    if bad_names:
        raise ValueError(
            f"These measurement sets are not named like <obs_id>{EXT_MS} or <obs_id>_chN-M{EXT_MS}, so the"
            f" solutions could not be matched to them: {', '.join(bad_names)}"
        )

    moved: list[str] = []
    for ms in in_input:
        destination = os.path.join(job.dirs.work, os.path.basename(ms))
        if os.path.exists(destination):
            raise ValueError(f"{destination} already exists. Use an empty work directory.")
        shutil.move(ms, destination)
        moved.append(destination)

    logger.info(f"Moved {len(moved)} measurement set(s) to {job.dirs.work}")
    return sorted(moved)


def _step_download(job: Job) -> tuple[str, str]:
    """Download the signed URL and extract it. For an ``_ms.tar``, put the measurement sets in the work directory.

    Args:
        job: The job.

    Returns:
        (status, detail). A failure raises or returns STATUS_FAILED.
    """
    settings = job.settings

    if job.skip_download:
        has_input = os.path.isdir(job.dirs.input) and bool(os.listdir(job.dirs.input))
        has_work_ms = job.info.kind is SignedUrlKind.ms and bool(_find_measurement_sets(job.dirs.work))
        if not has_input and not has_work_ms:
            return STATUS_FAILED, f"--skip-download needs data in {job.dirs.input}, but there is none."
        detail = f"used the data in {job.dirs.input}"
    else:
        tar_filename = os.path.join(job.dirs.input, job.info.tar_filename)
        try:
            bytes_downloaded = download_signed_url(
                job.url,
                tar_filename,
                settings.download_retries,
                settings.download_retry_wait,
                settings.download_timeout,
                settings.https_proxy,
                settings.no_proxy,
            )
        except DownloadError as download_error:
            return STATUS_FAILED, str(download_error)

        logger.info(f"Extracting {tar_filename} to {job.dirs.input}")
        try:
            extract_tar(tar_filename, job.dirs.input)
        finally:
            if not settings.keep_completed_visibility_files and os.path.exists(tar_filename):
                os.remove(tar_filename)
        detail = f"downloaded {bytes_downloaded / BYTES_PER_GIGABYTE:.1f} GB"

    if job.info.kind is SignedUrlKind.ms:
        measurement_sets = collect_measurement_sets(job)
        detail += f", {len(measurement_sets)} measurement set(s)"

    return STATUS_OK, detail


def _step_metafits(job: Job) -> tuple[str, str]:
    """Get the metafits file from the MWA web service (as the processor does) and read it.

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    job.metafits_filename = os.path.join(job.dirs.input, f"{job.info.obs_id}_metafits.fits")

    if os.path.exists(job.metafits_filename):
        detail = "used the metafits that is already there"
    else:
        job.metafits_filename = download_metafits_file(job.info.obs_id, job.dirs.input)
        detail = "from the MWA web service"

    job.metafits_context = MetafitsContext(job.metafits_filename, None)
    return STATUS_OK, detail


def _step_disk_space(job: Job) -> tuple[str, str]:
    """Check there is room in the work directory, and set the memory limits.

    The room needed is what the processor allows for: the measurement sets, plus the calibrated copy and
    WSClean's reordered copy (WORKING_SPACE_FACTOR_IMAGING times the Birli measurement set size). If the
    work directory is RAM-backed, that is also RAM that Birli and WSClean cannot use, so it is
    taken off their memory limits, as the processor does for its RAM-backed temp working path.

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    settings = job.settings
    assert job.metafits_context is not None

    if job.info.kind is SignedUrlKind.vis:
        estimated_bytes = estimate_birli_output_bytes(
            job.metafits_context, settings.birli_freq_res_khz, settings.birli_int_time_res_sec
        )
    else:
        estimated_bytes = sum(_directory_size_bytes(ms) for ms in _find_measurement_sets(job.dirs.work))

    estimated_gb = max(int(estimated_bytes / BYTES_PER_GIGABYTE), 1)
    needed_gb = estimate_working_space_gb(estimated_gb, True)
    free_gb = shutil.disk_usage(job.dirs.work).free / BYTES_PER_GIGABYTE

    job.work_is_ram_backed = is_ram_backed(job.dirs.work)
    job.birli_max_mem_gib = settings.birli_max_mem_gib
    job.wsclean_abs_mem_gib = settings.wsclean_max_mem_gib

    if job.work_is_ram_backed:
        job.birli_max_mem_gib = settings.birli_max_mem_gib - int(gigabyte_to_gibibyte(needed_gb))
        job.wsclean_abs_mem_gib = calculate_abs_mem_gib(settings.wsclean_max_mem_gib, needed_gb)
        logger.info(
            f"{job.dirs.work} is RAM-backed. Birli memory is now {job.birli_max_mem_gib} GiB"
            f" and WSClean memory is now {job.wsclean_abs_mem_gib} GiB."
        )

    detail = f"need about {needed_gb} GB in {job.dirs.work}, {free_gb:.0f} GB free"
    if needed_gb > free_gb:
        return STATUS_FAILED, f"Not enough room: {detail}"

    return STATUS_OK, detail


def _step_birli(job: Job) -> tuple[str, str]:
    """Run Birli to make the measurement sets, unless the download has them.

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    if job.info.kind is SignedUrlKind.ms:
        return STATUS_SKIPPED, "the download already has measurement sets"

    assert job.metafits_context is not None
    settings = job.settings

    if job.birli_max_mem_gib < 1:
        return STATUS_FAILED, "No memory is left for Birli after the measurement sets in the RAM-backed work directory."

    ok = run_birli(
        job.dirs.input,
        job.metafits_filename,
        os.path.join(job.dirs.work, f"{job.info.obs_id}{EXT_MS}"),
        job.dirs.output,
        job.info.obs_id,
        job.metafits_context.oversampled,
        settings.birli_binary_path,
        job.birli_max_mem_gib,
        settings.birli_timeout,
        settings.birli_freq_res_khz * 1000,
        settings.birli_int_time_res_sec,
        settings.birli_edge_width_khz * 1000,
    )
    if not ok:
        return STATUS_FAILED, "Birli failed. See the log and the readme in the output directory."

    measurement_sets = _find_measurement_sets(job.dirs.work)
    if not measurement_sets:
        return STATUS_FAILED, f"Birli worked, but there is no measurement set in {job.dirs.work}."

    return STATUS_OK, f"{len(measurement_sets)} measurement set(s)"


def _step_hyperdrive(job: Job) -> tuple[str, str]:
    """Run hyperdrive di-calibrate on each measurement set.

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    assert job.metafits_context is not None
    settings = job.settings

    ok, _calibration_command = run_hyperdrive(
        _find_measurement_sets(job.dirs.work),
        job.metafits_filename,
        job.metafits_context,
        job.dirs.output,
        job.info.obs_id,
        settings.hyperdrive_binary_path,
        settings.hyperdrive_source_list_filename,
        settings.hyperdrive_source_list_type,
        settings.hyperdrive_num_sources,
        settings.hyperdrive_timeout,
        settings.hyperdrive_extra_args,
        settings.birli_edge_width_khz * 1000,
    )
    if not ok:
        return STATUS_FAILED, "hyperdrive di-calibrate failed. See the log and the readme in the output directory."

    return STATUS_OK, "solutions are in the output directory"


def _step_solutions(job: Job) -> tuple[str, str]:
    """Process the solutions: flag, plot, write the stats and the modified solutions. No database.

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    settings = job.settings

    try:
        processed = process_solutions_files(
            job.info.obs_id,
            job.dirs.input,
            job.dirs.output,
            settings.phase_fit_niter,
            settings.gains_cut_off_max,
            settings.gain_outlier_poly_degree,
            settings.gain_outlier_mad_residual_threshold,
            settings.gain_outlier_plot_n_tiles_per_page,
            settings.tile_bad_channel_fraction,
            settings.hyperdrive_binary_path,
            settings.phase_outlier_nstd,
        )
    except Exception:  # noqa: BLE001 - the step runner must get a result, with the error in a readme
        error_text = f"Error in process_solutions_files():\n{traceback.format_exc()}"
        logger.exception(error_text)
        write_readme_file(
            os.path.join(job.dirs.output, "readme_error.txt"), "process_solutions_files()", 1, "", error_text
        )
        return STATUS_FAILED, "Processing the solutions failed. See readme_error.txt in the output directory."

    if processed is None:
        return STATUS_FAILED, "No unflagged tiles found, so there is nothing to apply."

    return STATUS_OK, f"{len(processed.soln_tile_ids)} tiles"


def _step_imaging(job: Job) -> tuple[str, str]:
    """Apply the solutions, run WSClean and make the preview PNG (calvin.imaging.run_imaging_steps).

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    settings = job.settings
    assert job.metafits_context is not None

    # The WSClean image size and pixel scale depend on the array configuration and the frequency
    try:
        image_settings = array_image_settings_from_metafits(
            job.metafits_context, settings.wsclean_compact_array_baseline_max_length
        )
    except Exception as image_error:  # noqa: BLE001 - reported as a failed step
        logger.exception("Could not work out the WSClean image size and scale")
        return STATUS_FAILED, f"Could not work out the WSClean image size and scale: {image_error}"
    wsclean_args = dataclasses.replace(settings.wsclean_args, size=image_settings.size, scale=image_settings.scale)

    # An _ms.tar download is the expensive input, so it is never deleted: it is needed to run again.
    remove_input_ms = job.info.kind is SignedUrlKind.vis and not settings.keep_completed_visibility_files
    imaging_settings = ImagingSettings(
        hyperdrive_binary_path=settings.hyperdrive_binary_path,
        apply_timeout=settings.hyperdrive_apply_timeout,
        wsclean_binary_path=settings.wsclean_binary_path,
        wsclean_timeout=settings.wsclean_timeout,
        wsclean_args=wsclean_args,
        wsclean_abs_mem_gib=job.wsclean_abs_mem_gib,
        remove_input_ms=remove_input_ms,
    )

    ok = run_imaging_steps(
        job.info.obs_id,
        job.metafits_filename,
        job.dirs.output,
        job.dirs.work,
        imaging_settings,
        lambda task_name: logger.info(f"Imaging: {task_name}"),
    )
    if not ok:
        return STATUS_FAILED, "Imaging failed. See the log and the readmes in the output directory."

    image_prefix = os.path.join(job.dirs.output, f"{job.info.obs_id}{WSCLEAN_NAME_SUFFIX}")
    png = f"{image_prefix}{WSCLEAN_MFS_IMAGE_SUFFIX.removesuffix('.fits')}.png"
    if not os.path.exists(png):
        pngs = glob.glob(f"{image_prefix}-*.png")
        png = pngs[0] if pngs else ""

    image_detail = (
        f"{image_settings.configuration} array: -size {image_settings.size[0]} {image_settings.size[1]}"
        f" -scale {image_settings.scale}"
    )
    return STATUS_OK, f"{image_detail}; preview: {png}" if png else f"{image_detail}; no preview PNG was made"


def _step_cleanup(job: Job) -> tuple[str, str]:
    """Remove the measurement sets that are not needed any more.

    The calibrated sets always go. The Birli sets go too, but an ``_ms.tar`` download is kept. Nothing is
    removed if ``keep_completed_visibility_files`` is set.

    Args:
        job: The job.

    Returns:
        (status, detail).
    """
    if job.settings.keep_completed_visibility_files:
        return STATUS_SKIPPED, "keep_completed_visibility_files is set"

    removed = 0
    for ms in _find_measurement_sets(job.dirs.work, include_calibrated=True):
        if ms.endswith(CALIBRATED_MS_SUFFIX) or job.info.kind is SignedUrlKind.vis:
            try:
                shutil.rmtree(ms)
                removed += 1
            except OSError:
                logger.warning(f"Could not remove {ms}. Ignoring.")

    return STATUS_OK, f"removed {removed} measurement set(s)"


def _run_step(job: Job, name: str, step: Callable[[Job], tuple[str, str]]) -> bool:
    """Run one step, time it and record the result.

    Args:
        job: The job.
        name: The step name.
        step: The step function. It returns (status, detail).

    Returns:
        True if the run can go on (the step worked or was skipped).
    """
    logger.info(f"=== Step: {name} ===")
    start_time = time.monotonic()
    try:
        status, detail = step(job)
    except Exception as step_error:  # noqa: BLE001 - one step must not stop the summary from being written
        logger.exception(f"Step {name} raised an exception")
        status, detail = STATUS_FAILED, f"Unhandled exception: {step_error}"

    result = StepResult(name, status, time.monotonic() - start_time, detail)
    job.results.append(result)
    logger.log(logging.ERROR if status == STATUS_FAILED else logging.INFO, f"Step {name}: {status}. {detail}")
    return status != STATUS_FAILED


def run_job(job: Job, stop_after: str | None) -> bool:
    """Run the steps in order. Stop at the first failure, or after the step in stop_after.

    Args:
        job: The job.
        stop_after: A step from STOP_AFTER_CHOICES, or None to run them all.

    Returns:
        True if every step that ran worked.
    """
    steps: list[tuple[str, Callable[[Job], tuple[str, str]], bool]] = [
        # (name, function, can --stop-after stop after it)
        (STEP_DOWNLOAD, _step_download, True),
        (STEP_METAFITS, _step_metafits, True),
        (STEP_DISK_SPACE, _step_disk_space, False),
        (STEP_BIRLI, _step_birli, True),
        (STEP_HYPERDRIVE, _step_hyperdrive, True),
        (STEP_SOLUTIONS, _step_solutions, True),
        (STEP_IMAGING, _step_imaging, True),
    ]

    for name, step, can_stop_after in steps:
        if not _run_step(job, name, step):
            return False
        if can_stop_after and stop_after == name:
            logger.info(f"Stopping after {name}, as asked.")
            return True

    # Only a run that did everything cleans up
    return _run_step(job, STEP_CLEANUP, _step_cleanup)


def format_summary(job: Job, success: bool) -> str:
    """Write the summary of a run as text.

    Args:
        job: The job, with its results.
        success: True if every step worked.

    Returns:
        The summary, with one line per step.
    """
    lines = [
        "",
        f"mwax_dl_conv_cal_image {get_mwax_mover_version_string()}: observation {job.info.obs_id}"
        f" (MWA ASVO job {job.info.job_id}, {job.info.kind.value})",
        f"{'Step':<14}{'Status':<9}{'Seconds':>9}  Detail",
    ]
    lines += [f"{r.name:<14}{r.status:<9}{r.seconds:>9.1f}  {r.detail}" for r in job.results]
    lines += [
        f"Result: {'SUCCESS' if success else 'FAILED'}",
        f"Output: {job.dirs.output}",
        f"Log: {os.path.join(os.path.dirname(job.dirs.input), LOG_FILENAME)}",
    ]
    return "\n".join(lines)


def run(argv: list[str] | None = None) -> int:
    """Run the tool: read the arguments and the config, make the directories, run the steps.

    Args:
        argv: The command line arguments. None means sys.argv.

    Returns:
        The exit code: 0 if every step worked, 1 if a step failed, 2 for a bad command line or config.
    """
    args = parse_args(argv)

    try:
        info = parse_signed_url(args.url)
        settings = read_local_config(args.cfg)
    except (ValueError, LocalConfigError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return EXIT_USAGE

    problems = check_files_exist(settings, need_birli=info.kind is SignedUrlKind.vis)
    if problems:
        print("Error:\n" + "\n".join(problems), file=sys.stderr)
        return EXIT_USAGE

    base_dir = os.path.abspath(args.dir)
    dirs = Dirs(
        input=os.path.join(base_dir, INPUT_DIR_NAME),
        work=os.path.abspath(args.work_dir) if args.work_dir else os.path.join(base_dir, WORK_DIR_NAME),
        output=os.path.join(base_dir, OUTPUT_DIR_NAME),
    )
    for directory in (dirs.input, dirs.work, dirs.output):
        os.makedirs(directory, exist_ok=True)

    job = Job(settings=settings, info=info, dirs=dirs, url=args.url, skip_download=args.skip_download)

    try:
        success = run_job(job, args.stop_after)
    except KeyboardInterrupt:
        logger.error("Stopped by Ctrl-C. A program that was running (Birli, hyperdrive, WSClean) may still be running.")
        job.results.append(StepResult("interrupted", STATUS_FAILED, 0.0, "Ctrl-C"))
        success = False

    summary = format_summary(job, success)
    logger.info(summary)
    print(summary)

    return EXIT_OK if success else EXIT_STEP_FAILED


def main() -> None:
    """Entry point of mwax_dl_conv_cal_image: set up logging, run, and exit with the exit code."""
    # The log file goes in the directory, so the arguments are read once here for it. run() reads them again.
    args = parse_args()
    base_dir = os.path.abspath(args.dir)
    os.makedirs(base_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler(), logging.FileHandler(os.path.join(base_dir, LOG_FILENAME))],
    )
    sys.exit(run())


if __name__ == "__main__":
    main()
