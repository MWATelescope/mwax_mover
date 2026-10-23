"""Running WSClean to image calibrated measurement sets.

run_wsclean() shells out to the wsclean binary via a Popen handle (so it can be
signalled/waited on alongside other work) and always writes a readme
(core.command.write_readme_file) recording the command and outcome. WSCleanArgs
holds the WSClean options and turns them into command line arguments.

A failed WSClean run does not fail the calibration job: the solutions are
already published, so run_wsclean() only returns False and the caller logs it.
"""

import logging
import math
import os
import time
from configparser import ConfigParser
from dataclasses import dataclass

import numpy as np
from mwalib import MetafitsContext

from mwax_mover.constants import (
    MWA_COARSE_CHANNEL_WIDTH_HZ,
    SECTION_WSCLEAN,
    WSCLEAN_COMPACT_BASE_SCALE,
    WSCLEAN_COMPACT_IMAGE_SIZE,
    WSCLEAN_EXTENDED_BASE_SCALE,
    WSCLEAN_EXTENDED_IMAGE_SIZE,
    WSCLEAN_EXTENDED_MIN_TILES,
    WSCLEAN_GRIDDER,
    WSCLEAN_POLARIZATIONS,
    WSCLEAN_README_SUFFIX,
    WSCLEAN_SCALE_DECIMALS,
)
from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file
from mwax_mover.core.config import read_config, read_config_bool
from mwax_mover.core.units import gigabyte_to_gibibyte

logger = logging.getLogger(__name__)


@dataclass
class WSCleanArgs:
    """The WSClean options used by calvin.

    An option that is None (or False, for a flag) is not put on the command line.
    wsclean_args_from_config() sets the options calvin takes from the [wsclean] config
    section. The processor then fills in the name, temp_dir and abs_mem_gib for the job, and the
    size and scale, which are worked out for each observation (see array_image_settings()).

    Attributes:
        name: Prefix for all WSClean output files. Must be an absolute path, because WSClean
            writes relative to the process working directory.
        temp_dir: Directory for WSClean's temporary (reordered) files.
        no_update_model_required: If True, WSClean does not write the model data column.
        abs_mem_gib: Memory limit in GiB (-abs-mem).
        size: Image width and height in pixels (-size).
        scale: Pixel scale, in degrees unless it has a unit, e.g. "0.0045113" (-scale).
        briggs_robust: Robustness of Briggs weighting (-weight briggs <robust>).
        niter: Maximum number of clean iterations (-niter).
        nmiter: Maximum number of major iterations (-nmiter).
        mgain: Major iteration gain (-mgain).
        multiscale_gain: Multiscale clean gain (-multiscale-gain).
        auto_threshold: Clean to this many sigma (-auto-threshold).
        auto_mask: Mask at this many sigma (-auto-mask).
        minuv_l: Ignore baselines shorter than this many wavelengths (-minuv-l).
        channels_out: Number of output channels (-channels-out).
        gridder: Gridder (-gridder).
        polarizations: Polarizations to image (-pol).
        multiscale: Use multiscale cleaning (-multiscale).
        join_channels: Clean the output channels together (-join-channels).
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
    nmiter: int | None = None
    mgain: float | None = None
    multiscale_gain: float | None = None
    auto_threshold: float | None = None
    auto_mask: float | None = None
    minuv_l: float | None = None
    channels_out: int | None = None
    gridder: str | None = None
    polarizations: str | None = None
    multiscale: bool = False
    join_channels: bool = False
    log_time: bool = False
    verbose: bool = False

    def validate(self) -> None:
        """Check that the options can be used together.

        Raises:
            ValueError: If an option is out of range, or if join_channels has nothing to join.
        """
        if self.size is not None and min(self.size) < 1:
            raise ValueError(f"size must be positive, got {self.size[0]} {self.size[1]}.")

        if self.channels_out is not None and self.channels_out < 1:
            raise ValueError(f"channels_out must be at least 1, got {self.channels_out}.")

        if self.join_channels and (self.channels_out is None or self.channels_out < 2):
            raise ValueError("join_channels needs channels_out to be 2 or more.")

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
        if self.nmiter is not None:
            args += ["-nmiter", str(self.nmiter)]
        if self.mgain is not None:
            args += ["-mgain", f"{self.mgain:g}"]
        if self.multiscale_gain is not None:
            args += ["-multiscale-gain", f"{self.multiscale_gain:g}"]
        if self.auto_threshold is not None:
            args += ["-auto-threshold", f"{self.auto_threshold:g}"]
        if self.auto_mask is not None:
            args += ["-auto-mask", f"{self.auto_mask:g}"]
        if self.minuv_l is not None:
            args += ["-minuv-l", f"{self.minuv_l:g}"]
        if self.channels_out is not None:
            args += ["-channels-out", str(self.channels_out)]
        if self.gridder is not None:
            args += ["-gridder", self.gridder]
        if self.polarizations is not None:
            args += ["-pol", self.polarizations]

        flags = [
            (self.multiscale, "-multiscale"),
            (self.join_channels, "-join-channels"),
            (self.log_time, "-log-time"),
            (self.verbose, "-verbose"),
            (self.no_update_model_required, "-no-update-model-required"),
        ]
        args += [flag for enabled, flag in flags if enabled]

        args += ["-parallel-gridding", "4"]
        args += ["-parallel-reordering", "32"]
        args += ["-parallel-deconvolution", "1024"]

        return args


def wsclean_args_from_config(config: ConfigParser) -> WSCleanArgs:
    """Read the WSClean options from the [wsclean] config section.

    Every option read here is a required key. The gridder (w-gridder) and the polarization
    (-pol i, Stokes I) are always used, so they are not in the config. The image size and pixel
    scale are not in the config either: they are worked out for each observation (see
    array_image_settings()). WSClean uses all CPU cores unless told otherwise, and calvin does
    not tell it.

    The returned args have no name, temp_dir, memory limit, size or scale: those are set per job.

    Args:
        config: The loaded calvin_processor config.

    Returns:
        The WSClean options, already checked with validate().

    Raises:
        configparser.NoOptionError: If a key is missing.
        ValueError: If a value is not valid, or the options cannot be used together.
    """
    args = WSCleanArgs(
        name="",
        temp_dir="",
        briggs_robust=float(read_config(config, SECTION_WSCLEAN, "briggs_robust")),
        niter=int(read_config(config, SECTION_WSCLEAN, "niter")),
        nmiter=int(read_config(config, SECTION_WSCLEAN, "nmiter")),
        mgain=float(read_config(config, SECTION_WSCLEAN, "mgain")),
        multiscale_gain=float(read_config(config, SECTION_WSCLEAN, "multiscale_gain")),
        auto_threshold=float(read_config(config, SECTION_WSCLEAN, "auto_threshold")),
        auto_mask=float(read_config(config, SECTION_WSCLEAN, "auto_mask")),
        minuv_l=float(read_config(config, SECTION_WSCLEAN, "minuv_l")),
        channels_out=int(read_config(config, SECTION_WSCLEAN, "channels_out")),
        gridder=WSCLEAN_GRIDDER,
        polarizations=WSCLEAN_POLARIZATIONS,
        multiscale=read_config_bool(config, SECTION_WSCLEAN, "multiscale"),
        join_channels=read_config_bool(config, SECTION_WSCLEAN, "join_channels"),
        log_time=read_config_bool(config, SECTION_WSCLEAN, "log_time"),
        verbose=read_config_bool(config, SECTION_WSCLEAN, "verbose"),
    )
    args.validate()

    return args


def read_compact_array_baseline_max_length(config: ConfigParser) -> float:
    """Read [wsclean] compact_array_baseline_max_length: the longest baseline of a compact array, in metres.

    Args:
        config: The loaded calvin_processor config.

    Returns:
        The length in metres.

    Raises:
        configparser.NoOptionError: If the key is missing.
        ValueError: If the value is not a number above zero.
    """
    key = "compact_array_baseline_max_length"
    value = read_config(config, SECTION_WSCLEAN, key)
    try:
        length_m = float(value)
    except ValueError:
        length_m = math.nan
    if not length_m > 0:  # also false for NaN
        raise ValueError(f"[{SECTION_WSCLEAN}] {key} must be a number of metres above zero, got '{value}'.")

    return length_m


@dataclass
class ArrayImageSettings:
    """The WSClean image size and pixel scale for one observation, and how they were worked out.

    Attributes:
        configuration: "compact" or "extended".
        size: Image width and height in pixels.
        scale: The pixel scale in degrees, as the string given to WSClean.
        centre_channel: The centre coarse channel number (0-255).
        num_tiles: The number of tiles.
        max_baseline_m: The longest baseline in metres, from all the tile positions.
    """

    configuration: str
    size: tuple[int, int]
    scale: str
    centre_channel: int
    num_tiles: int
    max_baseline_m: float


def centre_coarse_channel(centre_freq_hz: float) -> int:
    """Work out the centre coarse channel number (0-255) from the centre frequency.

    For an observation with an even number of coarse channels the centre is between two channels
    (a .5 value), and this rounds half up, as the metafits CENTCHAN does. Python's round() rounds half
    to even, which would give 120 for 120.5, where CENTCHAN has 121. A calculation in floating point is
    not safe either: 163.2 / 1.28 is 127.49999999999999, not 127.5. So this uses whole numbers: the centre
    frequency in Hz is a whole number, and so is the channel width.

    Args:
        centre_freq_hz: The centre frequency in Hz (mwalib MetafitsContext.centre_freq_hz).

    Returns:
        The channel number.
    """
    width_hz = MWA_COARSE_CHANNEL_WIDTH_HZ
    return (2 * round(centre_freq_hz) + width_hz) // (2 * width_hz)


def max_baseline_length_m(positions_m: np.ndarray) -> float:
    """Find the longest baseline between a set of positions.

    Args:
        positions_m: An array of shape (n, 3): the east, north and height of each tile in metres.

    Returns:
        The longest distance between any two positions, in metres. Zero if there are fewer than two.
    """
    positions = np.asarray(positions_m, dtype=float)
    if len(positions) < 2:  # noqa: PLR2004 - a baseline needs two tiles
        return 0.0

    differences = positions[:, np.newaxis, :] - positions[np.newaxis, :, :]
    return float(np.sqrt((differences**2).sum(axis=-1)).max())


def array_image_settings(
    num_tiles: int,
    max_baseline_m: float,
    centre_freq_hz: float,
    compact_array_baseline_max_length: float,
) -> ArrayImageSettings:
    """Choose the WSClean image size and pixel scale for an observation.

    - With WSCLEAN_EXTENDED_MIN_TILES or more tiles the array is "extended".
    - Otherwise it is "compact" if the longest baseline is no more than compact_array_baseline_max_length,
      and "extended" if it is longer.
    - Compact: size WSCLEAN_COMPACT_IMAGE_SIZE and scale WSCLEAN_COMPACT_BASE_SCALE / centre channel.
      Extended: WSCLEAN_EXTENDED_IMAGE_SIZE and WSCLEAN_EXTENDED_BASE_SCALE / centre channel.
    - The scale is in degrees, with WSCLEAN_SCALE_DECIMALS decimal places.

    Args:
        num_tiles: The number of tiles.
        max_baseline_m: The longest baseline in metres.
        centre_freq_hz: The centre frequency in Hz.
        compact_array_baseline_max_length: The longest baseline of a compact array in metres.

    Returns:
        The settings.

    Raises:
        ValueError: If the centre frequency gives a channel number below 1.
    """
    centre_channel = centre_coarse_channel(centre_freq_hz)
    if centre_channel < 1:
        raise ValueError(f"The centre frequency {centre_freq_hz} Hz gives centre channel {centre_channel}.")

    if num_tiles >= WSCLEAN_EXTENDED_MIN_TILES or max_baseline_m > compact_array_baseline_max_length:
        configuration = "extended"
        base_scale = WSCLEAN_EXTENDED_BASE_SCALE
        image_size = WSCLEAN_EXTENDED_IMAGE_SIZE
    else:
        configuration = "compact"
        base_scale = WSCLEAN_COMPACT_BASE_SCALE
        image_size = WSCLEAN_COMPACT_IMAGE_SIZE

    return ArrayImageSettings(
        configuration=configuration,
        size=(image_size, image_size),
        scale=f"{base_scale / centre_channel:.{WSCLEAN_SCALE_DECIMALS}f}",
        centre_channel=centre_channel,
        num_tiles=num_tiles,
        max_baseline_m=max_baseline_m,
    )


def array_image_settings_from_metafits(
    metafits_context: MetafitsContext, compact_array_baseline_max_length: float
) -> ArrayImageSettings:
    """Choose the WSClean image size and pixel scale for an observation from its metafits.

    Args:
        metafits_context: The observation's metafits (mwalib). All the tiles are used, flagged or not.
        compact_array_baseline_max_length: The longest baseline of a compact array in metres.

    Returns:
        The settings (see array_image_settings()).
    """
    positions = np.array([(a.east_m, a.north_m, a.height_m) for a in metafits_context.antennas], dtype=float)
    settings = array_image_settings(
        metafits_context.num_ants,
        max_baseline_length_m(positions),
        metafits_context.centre_freq_hz,
        compact_array_baseline_max_length,
    )
    logger.info(
        f"{settings.num_tiles} tiles, longest baseline {settings.max_baseline_m:.0f} m, centre channel"
        f" {settings.centre_channel}: {settings.configuration} array, WSClean -size {settings.size[0]}"
        f" {settings.size[1]} -scale {settings.scale}"
    )
    return settings


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
