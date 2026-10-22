"""Running WSClean to image calibrated measurement sets.

run_wsclean() shells out to the wsclean binary via a Popen handle (so it can be
signalled/waited on alongside other work) and always writes a readme
(core.command.write_readme_file) recording the command and outcome. WSCleanArgs
holds the WSClean options and turns them into command line arguments.

A failed WSClean run does not fail the calibration job: the solutions are
already published, so run_wsclean() only returns False and the caller logs it.
"""

import logging
import os
import time
from configparser import ConfigParser
from dataclasses import dataclass

from mwax_mover.constants import (
    SECTION_WSCLEAN,
    WSCLEAN_GRIDDER,
    WSCLEAN_IDG_MODE,
    WSCLEAN_POLARIZATIONS,
    WSCLEAN_README_SUFFIX,
)
from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file
from mwax_mover.core.config import read_config, read_config_bool
from mwax_mover.core.units import gigabyte_to_gibibyte

logger = logging.getLogger(__name__)


@dataclass
class WSCleanArgs:
    """The WSClean options used by calvin.

    An option that is None (or False, for a flag) is not put on the command line.
    wsclean_args_from_config() sets all the options calvin uses, from the [wsclean]
    config section. The processor then fills in name, temp_dir and abs_mem_gib for the job.

    Attributes:
        name: Prefix for all WSClean output files. Must be an absolute path, because WSClean
            writes relative to the process working directory.
        temp_dir: Directory for WSClean's temporary (reordered) files.
        no_update_model_required: If True, WSClean does not write the model data column.
        abs_mem_gib: Memory limit in GiB (-abs-mem).
        size: Image width and height in pixels (-size).
        scale: Pixel scale with its unit, e.g. "0.75amin" (-scale).
        briggs_robust: Robustness of Briggs weighting (-weight briggs <robust>).
        niter: Maximum number of clean iterations (-niter).
        mgain: Major iteration gain (-mgain).
        auto_threshold: Clean to this many sigma (-auto-threshold).
        auto_mask: Mask at this many sigma (-auto-mask).
        channels_out: Number of output channels (-channels-out).
        gridder: Gridder (-gridder).
        idg_mode: IDG mode (-idg-mode).
        polarizations: Comma separated polarisations to image (-pol).
        multiscale: Use multiscale cleaning (-multiscale).
        join_channels: Clean the output channels together (-join-channels).
        join_polarizations: Clean the polarisations together (-join-polarizations).
        log_time: Put the date and time on each line of output (-log-time).
        verbose: Show more output (-verbose).
    """

    name: str
    temp_dir: str
    no_update_model_required: bool = True
    abs_mem_gib: int | None = None
    size: tuple[int, int] | None = None
    scale: str | None = None
    briggs_robust: float | None = None
    niter: int | None = None
    mgain: float | None = None
    auto_threshold: float | None = None
    auto_mask: float | None = None
    channels_out: int | None = None
    gridder: str | None = None
    idg_mode: str | None = None
    polarizations: str | None = None
    multiscale: bool = False
    join_channels: bool = False
    join_polarizations: bool = False
    log_time: bool = False
    verbose: bool = False

    def validate(self) -> None:
        """Check that the options can be used together.

        Raises:
            ValueError: If an option is out of range, or if a join option has nothing to join.
        """
        if self.size is not None and min(self.size) < 1:
            raise ValueError(f"size must be positive, got {self.size[0]} {self.size[1]}.")

        if self.channels_out is not None and self.channels_out < 1:
            raise ValueError(f"channels_out must be at least 1, got {self.channels_out}.")

        if self.join_channels and (self.channels_out is None or self.channels_out < 2):
            raise ValueError("join_channels needs channels_out to be 2 or more.")

        if self.join_polarizations and (self.polarizations is None or len(self.polarizations.split(",")) < 2):
            raise ValueError("join_polarizations needs two or more polarizations.")

    def to_cmdline_args(self) -> list[str]:
        """Build the WSClean command line options (without the binary or the measurement sets).

        Returns:
            The options as separate arguments, e.g. ["-name", "/out/123_wsclean", ...].
        """
        args = ["-name", self.name, "-temp-dir", self.temp_dir]

        if self.abs_mem_gib is not None:
            args += ["-abs-mem", str(self.abs_mem_gib)]
        if self.size is not None:
            args += ["-size", str(self.size[0]), str(self.size[1])]
        if self.scale is not None:
            args += ["-scale", self.scale]
        if self.briggs_robust is not None:
            args += ["-weight", "briggs", f"{self.briggs_robust:g}"]
        if self.niter is not None:
            args += ["-niter", str(self.niter)]
        if self.mgain is not None:
            args += ["-mgain", f"{self.mgain:g}"]
        if self.auto_threshold is not None:
            args += ["-auto-threshold", f"{self.auto_threshold:g}"]
        if self.auto_mask is not None:
            args += ["-auto-mask", f"{self.auto_mask:g}"]
        if self.channels_out is not None:
            args += ["-channels-out", str(self.channels_out)]
        if self.gridder is not None:
            args += ["-gridder", self.gridder]
        if self.idg_mode is not None:
            args += ["-idg-mode", self.idg_mode]
        if self.polarizations is not None:
            args += ["-pol", self.polarizations]

        flags = [
            (self.multiscale, "-multiscale"),
            (self.join_channels, "-join-channels"),
            (self.join_polarizations, "-join-polarizations"),
            (self.log_time, "-log-time"),
            (self.verbose, "-verbose"),
            (self.no_update_model_required, "-no-update-model-required"),
        ]
        args += [flag for enabled, flag in flags if enabled]

        return args


def wsclean_args_from_config(config: ConfigParser) -> WSCleanArgs:
    """Read the WSClean options from the [wsclean] config section.

    Every option calvin sets is a required key. The gridder (IDG, hybrid mode) and the
    polarisations (xx,yy) are always used, so they are not in the config. WSClean uses all
    CPU cores unless told otherwise, and calvin does not tell it.

    The returned args have no name, temp_dir or memory limit: those are set per job.

    Args:
        config: The loaded calvin_processor config.

    Returns:
        The WSClean options, already checked with validate().

    Raises:
        configparser.NoOptionError: If a key is missing.
        ValueError: If a value is not valid, or the options cannot be used together.
    """
    size_values = read_config(config, SECTION_WSCLEAN, "size").replace(",", " ").split()
    try:
        size = (int(size_values[0]), int(size_values[1])) if len(size_values) == 2 else None
    except ValueError:
        size = None
    if size is None:
        raise ValueError(f"[wsclean] size must be a width and a height (two integers), got '{' '.join(size_values)}'.")

    args = WSCleanArgs(
        name="",
        temp_dir="",
        size=size,
        scale=read_config(config, SECTION_WSCLEAN, "scale"),
        briggs_robust=float(read_config(config, SECTION_WSCLEAN, "briggs_robust")),
        niter=int(read_config(config, SECTION_WSCLEAN, "niter")),
        mgain=float(read_config(config, SECTION_WSCLEAN, "mgain")),
        auto_threshold=float(read_config(config, SECTION_WSCLEAN, "auto_threshold")),
        auto_mask=float(read_config(config, SECTION_WSCLEAN, "auto_mask")),
        channels_out=int(read_config(config, SECTION_WSCLEAN, "channels_out")),
        gridder=WSCLEAN_GRIDDER,
        idg_mode=WSCLEAN_IDG_MODE,
        polarizations=WSCLEAN_POLARIZATIONS,
        multiscale=read_config_bool(config, SECTION_WSCLEAN, "multiscale"),
        join_channels=read_config_bool(config, SECTION_WSCLEAN, "join_channels"),
        join_polarizations=read_config_bool(config, SECTION_WSCLEAN, "join_polarizations"),
        log_time=read_config_bool(config, SECTION_WSCLEAN, "log_time"),
        verbose=read_config_bool(config, SECTION_WSCLEAN, "verbose"),
    )
    args.validate()

    return args


def calculate_abs_mem_gib(max_mem_gib: int, ram_backed_gb: int) -> int:
    """Work out the WSClean memory limit (-abs-mem).

    The calibrated measurement set and WSClean's reordered copy of it are in the temp
    working path, which is RAM-backed, so they use RAM that WSClean cannot. This is the same
    rule that is used to reduce Birli's memory.

    Args:
        max_mem_gib: The configured maximum memory in GiB.
        ram_backed_gb: The size in GB of the files in the RAM-backed working path (0 if they are
            on disk).

    Returns:
        The memory limit in GiB. It can be zero or negative if the files leave no memory.
    """
    return max_mem_gib - int(gigabyte_to_gibibyte(ram_backed_gb))


def run_wsclean(
    ms_filenames: list[str],
    job_output_path: str,
    obs_id: int,
    wsclean_binary_path: str,
    wsclean_timeout: int,
    args: WSCleanArgs,
) -> bool:
    """Execute wsclean to image calibrated measurement sets.

    The measurement sets are given to WSClean as separate arguments. A readme is
    written to job_output_path for success and for failure. Nothing is moved on
    failure, and no readme_error.txt is written.

    Args:
        ms_filenames: List of calibrated CASA measurement set dirs.
        job_output_path: Output directory for the readme.
        obs_id: Observation ID.
        wsclean_binary_path: Path to the wsclean executable.
        wsclean_timeout: Timeout in seconds for wsclean execution.
        args: The WSClean options.

    Returns:
        True if execution succeeded, False otherwise.
    """
    start_time = time.monotonic()
    stdout = ""
    stderr = ""
    exit_code = -1

    cmdline = " ".join([wsclean_binary_path, *args.to_cmdline_args(), *ms_filenames])

    try:
        wsclean_popen_process = start_command(cmdline, -1, False, False)

        exit_code, stdout, stderr = check_popen_finished(
            wsclean_popen_process,
            wsclean_timeout,
        )
    except Exception as wsclean_run_exception:  # noqa: BLE001 - one bad wsclean run must not crash the calibration job
        stderr = f"Unhandled exception {wsclean_run_exception}"

    elapsed = time.monotonic() - start_time
    write_readme_file(
        os.path.join(job_output_path, f"{obs_id}{WSCLEAN_README_SUFFIX}"), cmdline, exit_code, stdout, stderr
    )

    if exit_code == 0:
        logger.info(f"{obs_id}: wsclean run successful in {elapsed:.3f} seconds")
        return True

    logger.error(f"{obs_id}: wsclean run FAILED: Exit code of {exit_code} in {elapsed:.3f} seconds: {stderr}")
    return False
