"""The settings that mwax_dl_conv_cal_image reads from a calvin_processor.cfg.

read_local_config() reads only the keys the tool uses (see docs/MWAX_DL_CONV_CAL_IMAGE.md). The database,
upload, giant-squid and MWAX sections of the file are not read, so a file that still has them is fine. It
does not read the job paths either: the tool works in the directories it is given. check_files_exist()
checks the programs and the source list that the settings point to.
"""

import configparser
import logging
import os
from configparser import ConfigParser
from dataclasses import dataclass

from mwax_mover.calvin.wsclean import WSCleanArgs, wsclean_args_from_config
from mwax_mover.constants import (
    SECTION_BIRLI,
    SECTION_DOWNLOADING,
    SECTION_HYPERDRIVE,
    SECTION_PROCESSING,
    SECTION_WSCLEAN,
)
from mwax_mover.core.config import read_config, read_config_bool, read_optional_config

logger = logging.getLogger(__name__)


class LocalConfigError(ValueError):
    """The config file is missing a section or a key, or a value is not valid."""


@dataclass
class LocalSettings:
    """Everything mwax_dl_conv_cal_image takes from the config file.

    The names are the config keys, with the section as a prefix.
    """

    birli_binary_path: str
    birli_timeout: int
    birli_max_mem_gib: int
    birli_freq_res_khz: int
    birli_int_time_res_sec: float
    birli_edge_width_khz: int

    hyperdrive_binary_path: str
    hyperdrive_timeout: int
    hyperdrive_apply_timeout: int
    hyperdrive_num_sources: int
    hyperdrive_source_list_filename: str
    hyperdrive_source_list_type: str
    hyperdrive_extra_args: str

    phase_fit_niter: int
    phase_outlier_nstd: float
    gains_cut_off_max: float | None
    gain_outlier_poly_degree: int
    gain_outlier_mad_residual_threshold: float
    gain_outlier_modify_gains: bool
    gain_outlier_plot_n_tiles_per_page: int
    tile_bad_channel_fraction: float
    keep_completed_visibility_files: bool

    wsclean_binary_path: str
    wsclean_timeout: int
    wsclean_max_mem_gib: int
    wsclean_args: WSCleanArgs

    download_retries: int
    download_retry_wait: int
    download_timeout: int
    https_proxy: str | None
    no_proxy: str | None


def _read(config: ConfigParser, section: str, key: str) -> str:
    """Read a required key. A missing section or key is a LocalConfigError that names it.

    Args:
        config: The loaded config.
        section: The section name.
        key: The key name.

    Returns:
        The value, as a string.

    Raises:
        LocalConfigError: If the section or key is missing.
    """
    try:
        return read_config(config, section, key)
    except configparser.NoSectionError as error:
        raise LocalConfigError(f"The config has no [{section}] section.") from error
    except configparser.NoOptionError as error:
        raise LocalConfigError(f"The config has no [{section}] {key}.") from error


def _read_as(config: ConfigParser, section: str, key: str, convert: type):
    """Read a required key and convert it. A value that cannot be converted is a LocalConfigError.

    Args:
        config: The loaded config.
        section: The section name.
        key: The key name.
        convert: int or float.

    Returns:
        The converted value.

    Raises:
        LocalConfigError: If the section or key is missing, or the value is not valid.
    """
    value = _read(config, section, key)
    try:
        return convert(value)
    except ValueError as error:
        raise LocalConfigError(f"[{section}] {key} must be a {convert.__name__}, got '{value}'.") from error


def _read_bool(config: ConfigParser, section: str, key: str) -> bool:
    """Read a required boolean key (1, 0, yes, no, true, false, on, off).

    Args:
        config: The loaded config.
        section: The section name.
        key: The key name.

    Returns:
        The value.

    Raises:
        LocalConfigError: If the section or key is missing, or the value is not valid.
    """
    _read(config, section, key)  # gives the clear error if it is missing
    try:
        return read_config_bool(config, section, key)
    except ValueError as error:
        raise LocalConfigError(
            f"[{section}] {key} must be 1 or 0 (or yes/no, true/false), got a different value."
        ) from error


def read_local_config(config_filename: str) -> LocalSettings:
    """Read the settings from a calvin_processor.cfg.

    Args:
        config_filename: Path to the config file.

    Returns:
        The settings. The WSClean options are read whether or not ``[wsclean] enabled`` is set,
        because this tool always runs imaging.

    Raises:
        LocalConfigError: If the file cannot be read, a key is missing, or a value is not valid.
            The message names the section and key.
    """
    if not os.path.isfile(config_filename):
        raise LocalConfigError(f"The config file {config_filename} does not exist.")

    config = ConfigParser()
    try:
        config.read(config_filename)
    except configparser.Error as error:
        raise LocalConfigError(f"Cannot read the config file {config_filename}: {error}") from error

    gains_cut_off_max: float | None = None
    if config.has_option(SECTION_PROCESSING, "gains_cut_off_max"):
        try:
            gains_cut_off_max = float(read_config(config, SECTION_PROCESSING, "gains_cut_off_max"))
        except ValueError:
            gains_cut_off_max = None  # not a number means "off", as in the processor

    try:
        wsclean_args = wsclean_args_from_config(config)
    except configparser.NoSectionError as error:
        raise LocalConfigError(f"The config has no [{SECTION_WSCLEAN}] section.") from error
    except configparser.NoOptionError as error:
        raise LocalConfigError(f"The config has no [{SECTION_WSCLEAN}] {error.option}.") from error
    except ValueError as error:
        raise LocalConfigError(f"[{SECTION_WSCLEAN}]: {error}") from error

    return LocalSettings(
        birli_binary_path=_read(config, SECTION_BIRLI, "binary_path"),
        birli_timeout=_read_as(config, SECTION_BIRLI, "timeout", int),
        birli_max_mem_gib=_read_as(config, SECTION_BIRLI, "max_mem_gib", int),
        birli_freq_res_khz=_read_as(config, SECTION_BIRLI, "freq_res_khz", int),
        birli_int_time_res_sec=_read_as(config, SECTION_BIRLI, "int_time_res_sec", float),
        birli_edge_width_khz=_read_as(config, SECTION_BIRLI, "edge_width_khz", int),
        hyperdrive_binary_path=_read(config, SECTION_HYPERDRIVE, "binary_path"),
        hyperdrive_timeout=_read_as(config, SECTION_HYPERDRIVE, "timeout", int),
        hyperdrive_apply_timeout=_read_as(config, SECTION_HYPERDRIVE, "apply_timeout", int),
        hyperdrive_num_sources=_read_as(config, SECTION_HYPERDRIVE, "num_sources", int),
        hyperdrive_source_list_filename=_read(config, SECTION_HYPERDRIVE, "source_list_filename"),
        hyperdrive_source_list_type=_read(config, SECTION_HYPERDRIVE, "source_list_type"),
        hyperdrive_extra_args=_read(config, SECTION_HYPERDRIVE, "extra_args"),
        phase_fit_niter=_read_as(config, SECTION_PROCESSING, "phase_fit_niter", int),
        phase_outlier_nstd=_read_as(config, SECTION_PROCESSING, "phase_outlier_nstd", float),
        gains_cut_off_max=gains_cut_off_max,
        gain_outlier_poly_degree=_read_as(config, SECTION_PROCESSING, "gain_outlier_poly_degree", int),
        gain_outlier_mad_residual_threshold=_read_as(
            config, SECTION_PROCESSING, "gain_outlier_mad_residual_threshold", float
        ),
        gain_outlier_modify_gains=_read_bool(config, SECTION_PROCESSING, "gain_outlier_modify_gains"),
        gain_outlier_plot_n_tiles_per_page=_read_as(
            config, SECTION_PROCESSING, "gain_outlier_plot_n_tiles_per_page", int
        ),
        tile_bad_channel_fraction=_read_as(config, SECTION_PROCESSING, "tile_bad_channel_fraction", float),
        keep_completed_visibility_files=_read_bool(config, SECTION_PROCESSING, "keep_completed_visibility_files"),
        wsclean_binary_path=_read(config, SECTION_WSCLEAN, "binary_path"),
        wsclean_timeout=_read_as(config, SECTION_WSCLEAN, "timeout", int),
        wsclean_max_mem_gib=_read_as(config, SECTION_WSCLEAN, "max_mem_gib", int),
        wsclean_args=wsclean_args,
        download_retries=_read_as(config, SECTION_DOWNLOADING, "download_retries", int),
        download_retry_wait=_read_as(config, SECTION_DOWNLOADING, "download_retry_wait", int),
        download_timeout=_read_as(config, SECTION_DOWNLOADING, "mwaasvo_download_obs_timeout", int),
        https_proxy=read_optional_config(config, SECTION_DOWNLOADING, "https_proxy"),
        no_proxy=read_optional_config(config, SECTION_DOWNLOADING, "no_proxy"),
    )


def check_files_exist(settings: LocalSettings, need_birli: bool) -> list[str]:
    """Check that the programs and the source list in the settings exist.

    Args:
        settings: The settings.
        need_birli: False if Birli will not run (the download already has measurement sets).

    Returns:
        A list of problems, one line each. Empty if everything exists.
    """
    to_check = [
        ("[hyperdrive] binary_path", settings.hyperdrive_binary_path),
        ("[hyperdrive] source_list_filename", settings.hyperdrive_source_list_filename),
        ("[wsclean] binary_path", settings.wsclean_binary_path),
    ]
    if need_birli:
        to_check.insert(0, ("[birli] binary_path", settings.birli_binary_path))

    return [f"{key} {path} does not exist." for key, path in to_check if not os.path.exists(path)]
