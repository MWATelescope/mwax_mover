"""Choosing the hyperdrive sky model (source list) for an observation.

The calvin_processor config has one ``[skymodel:<name>]`` section for each sky model. A section gives the
file, its type (the hyperdrive ``--source-list-type``, e.g. ``fits`` or ``rts``), and the range of
observation centre coarse channels that it is used for. For example::

    [skymodel:low]
    max_centre_channel=168
    source_list_filename=/software/srclists/GGSM_updated.fits
    source_list_type=fits

    [skymodel:high]
    min_centre_channel=169
    source_list_filename=/software/srclists/other.fits
    source_list_type=fits
    num_sources=2000

- Both ends of a range are included. Either end can be left out: no ``min_centre_channel`` means "from the
  lowest channel", and no ``max_centre_channel`` means "up to the highest channel".
- The ranges must cover every channel exactly once: no overlap and no gap. The lowest range has no minimum
  and the highest has no maximum. A single section with neither covers all channels.
- ``num_sources`` is optional. It defaults to ``[hyperdrive] num_sources``.
- The centre channel is the same one that is used for the WSClean pixel scale (see
  calvin.wsclean.centre_coarse_channel()).

sky_models_from_config() reads and checks the sections. select_sky_model() picks the one for a channel.
"""

import logging
import os
from configparser import ConfigParser
from dataclasses import dataclass

from mwalib import MetafitsContext

from mwax_mover.calvin.wsclean import centre_coarse_channel
from mwax_mover.constants import (
    SECTION_HYPERDRIVE,
    SECTION_SKY_MODEL_PREFIX,
    SKY_MODEL_KEY_FILENAME,
    SKY_MODEL_KEY_MAX_CHANNEL,
    SKY_MODEL_KEY_MIN_CHANNEL,
    SKY_MODEL_KEY_NUM_SOURCES,
    SKY_MODEL_KEY_TYPE,
    SKY_MODEL_TYPES,
)
from mwax_mover.core.config import read_config

logger = logging.getLogger(__name__)


class SkyModelError(ValueError):
    """The sky model config is not valid, or no sky model fits the observation. The message says why."""


@dataclass(frozen=True)
class SkyModel:
    """One sky model and the centre channels it is used for.

    Attributes:
        name: The section name after ``skymodel:``, for messages.
        filename: Path of the source list file.
        source_list_type: The hyperdrive ``--source-list-type``.
        num_sources: How many sources hyperdrive uses (``--num-sources``).
        min_channel: The lowest centre coarse channel this is used for, or None for no lower limit.
        max_channel: The highest centre coarse channel this is used for, or None for no upper limit.
    """

    name: str
    filename: str
    source_list_type: str
    num_sources: int
    min_channel: int | None = None
    max_channel: int | None = None

    def describe_range(self) -> str:
        """Describe the channels this model is used for, e.g. "channel 169 and above".

        Returns:
            The description.
        """
        if self.min_channel is None and self.max_channel is None:
            return "all channels"
        if self.min_channel is None:
            return f"channel {self.max_channel} and below"
        if self.max_channel is None:
            return f"channel {self.min_channel} and above"
        return f"channels {self.min_channel} to {self.max_channel}"


def _section_name(section: str) -> str:
    """Get the name of a sky model section: the part after ``skymodel:``."""
    return section[len(SECTION_SKY_MODEL_PREFIX) :].strip()


def _read_optional_channel(config: ConfigParser, section: str, key: str) -> int | None:
    """Read an optional centre channel (a whole number of at least 1) from a sky model section.

    Args:
        config: The loaded calvin_processor config.
        section: The section name.
        key: The key (SKY_MODEL_KEY_MIN_CHANNEL or SKY_MODEL_KEY_MAX_CHANNEL).

    Returns:
        The channel, or None if the key is not in the section.

    Raises:
        SkyModelError: If the value is not a whole number of at least 1.
    """
    if not config.has_option(section, key):
        return None

    value = read_config(config, section, key)
    try:
        channel = int(value)
    except ValueError:
        channel = 0  # not a number: the same error as a number below 1

    if channel < 1:
        raise SkyModelError(f"[{section}] {key} must be a whole number of at least 1, got '{value}'.")
    return channel


def _read_sky_model(config: ConfigParser, section: str, default_num_sources: int) -> SkyModel:
    """Read one ``[skymodel:<name>]`` section.

    Args:
        config: The loaded calvin_processor config.
        section: The section name.
        default_num_sources: The number of sources if the section has no num_sources.

    Returns:
        The sky model.

    Raises:
        SkyModelError: If a required key is missing or a value is not valid.
    """
    name = _section_name(section)
    if not name:
        raise SkyModelError(f"[{section}] needs a name after '{SECTION_SKY_MODEL_PREFIX}'.")

    for key in (SKY_MODEL_KEY_FILENAME, SKY_MODEL_KEY_TYPE):
        if not config.has_option(section, key):
            raise SkyModelError(f"[{section}] has no {key}.")

    filename = read_config(config, section, SKY_MODEL_KEY_FILENAME).strip()
    if not filename:
        raise SkyModelError(f"[{section}] {SKY_MODEL_KEY_FILENAME} is empty.")

    source_list_type = read_config(config, section, SKY_MODEL_KEY_TYPE).strip().lower()
    if source_list_type not in SKY_MODEL_TYPES:
        raise SkyModelError(
            f"[{section}] {SKY_MODEL_KEY_TYPE} '{source_list_type}' is not one of {', '.join(SKY_MODEL_TYPES)}."
        )

    num_sources = default_num_sources
    if config.has_option(section, SKY_MODEL_KEY_NUM_SOURCES):
        value = read_config(config, section, SKY_MODEL_KEY_NUM_SOURCES)
        try:
            num_sources = int(value)
        except ValueError:
            num_sources = 0
        if num_sources < 1:
            raise SkyModelError(
                f"[{section}] {SKY_MODEL_KEY_NUM_SOURCES} must be a whole number of at least 1, got '{value}'."
            )

    min_channel = _read_optional_channel(config, section, SKY_MODEL_KEY_MIN_CHANNEL)
    max_channel = _read_optional_channel(config, section, SKY_MODEL_KEY_MAX_CHANNEL)
    if min_channel is not None and max_channel is not None and min_channel > max_channel:
        raise SkyModelError(
            f"[{section}] {SKY_MODEL_KEY_MIN_CHANNEL} {min_channel} is above {SKY_MODEL_KEY_MAX_CHANNEL} {max_channel}."
        )

    return SkyModel(name, filename, source_list_type, num_sources, min_channel, max_channel)


def validate_sky_models(models: list[SkyModel]) -> None:
    """Check that the channel ranges of the sky models cover every channel exactly once.

    Args:
        models: The sky models, in any order.

    Raises:
        SkyModelError: If there are no models, if two ranges overlap (or are the same), or if there is a
            gap between ranges or below the lowest or above the highest.
    """
    if not models:
        raise SkyModelError(f"The config has no [{SECTION_SKY_MODEL_PREFIX}<name>] section. At least one is needed.")

    # No lower limit sorts first
    ordered = sorted(models, key=lambda m: (m.min_channel is not None, m.min_channel or 0))

    if ordered[0].min_channel is not None:
        raise SkyModelError(
            f"There is no sky model for channels below {ordered[0].min_channel}: the lowest range"
            f" ([{SECTION_SKY_MODEL_PREFIX}{ordered[0].name}]) must have no {SKY_MODEL_KEY_MIN_CHANNEL}."
        )

    for lower, upper in zip(ordered, ordered[1:], strict=False):
        if lower.max_channel is None or upper.min_channel is None:
            # Two ranges with no minimum, or a range with no maximum that is not the highest
            raise SkyModelError(
                f"The ranges of [{SECTION_SKY_MODEL_PREFIX}{lower.name}] ({lower.describe_range()}) and"
                f" [{SECTION_SKY_MODEL_PREFIX}{upper.name}] ({upper.describe_range()}) overlap."
            )
        if upper.min_channel <= lower.max_channel:
            raise SkyModelError(
                f"The ranges of [{SECTION_SKY_MODEL_PREFIX}{lower.name}] ({lower.describe_range()}) and"
                f" [{SECTION_SKY_MODEL_PREFIX}{upper.name}] ({upper.describe_range()}) overlap."
            )
        if upper.min_channel > lower.max_channel + 1:
            raise SkyModelError(
                f"There is no sky model for channels {lower.max_channel + 1} to {upper.min_channel - 1}:"
                f" the ranges of [{SECTION_SKY_MODEL_PREFIX}{lower.name}] and"
                f" [{SECTION_SKY_MODEL_PREFIX}{upper.name}] have a gap."
            )

    if ordered[-1].max_channel is not None:
        raise SkyModelError(
            f"There is no sky model for channels above {ordered[-1].max_channel}: the highest range"
            f" ([{SECTION_SKY_MODEL_PREFIX}{ordered[-1].name}]) must have no {SKY_MODEL_KEY_MAX_CHANNEL}."
        )


def sky_models_from_config(config: ConfigParser) -> list[SkyModel]:
    """Read and check the sky models from the ``[skymodel:<name>]`` sections.

    Args:
        config: The loaded calvin_processor config.

    Returns:
        The sky models, lowest channels first.

    Raises:
        SkyModelError: If a section is not valid, the ranges do not cover every channel exactly once, or the
            old ``[hyperdrive] source_list_filename`` or ``source_list_type`` is still in the config.
        configparser.NoOptionError: If ``[hyperdrive] num_sources`` is missing.
    """
    for old_key in (SKY_MODEL_KEY_FILENAME, SKY_MODEL_KEY_TYPE):
        if config.has_option(SECTION_HYPERDRIVE, old_key):
            raise SkyModelError(
                f"[{SECTION_HYPERDRIVE}] {old_key} is not used any more. Put the sky models in"
                f" [{SECTION_SKY_MODEL_PREFIX}<name>] sections."
            )

    default_num_sources = int(read_config(config, SECTION_HYPERDRIVE, "num_sources"))
    sections = [section for section in config.sections() if section.startswith(SECTION_SKY_MODEL_PREFIX)]
    models = [_read_sky_model(config, section, default_num_sources) for section in sections]
    validate_sky_models(models)

    return sorted(models, key=lambda m: (m.min_channel is not None, m.min_channel or 0))


def sky_model_file_problems(models: list[SkyModel]) -> list[str]:
    """Check that the source list file of every sky model exists.

    Args:
        models: The sky models.

    Returns:
        One line for each file that does not exist. Empty if all exist.
    """
    return [
        f"[{SECTION_SKY_MODEL_PREFIX}{m.name}] {SKY_MODEL_KEY_FILENAME} {m.filename} does not exist."
        for m in models
        if not os.path.exists(m.filename)
    ]


def select_sky_model(models: list[SkyModel], centre_channel: int) -> SkyModel:
    """Pick the sky model for an observation.

    Args:
        models: The sky models (already checked by validate_sky_models()).
        centre_channel: The observation's centre coarse channel.

    Returns:
        The model whose range includes the channel.

    Raises:
        SkyModelError: If no model includes the channel or more than one does. This cannot happen with
            models that passed validate_sky_models() and a channel number that is a whole number.
    """
    matches = [
        m
        for m in models
        if (m.min_channel is None or centre_channel >= m.min_channel)
        and (m.max_channel is None or centre_channel <= m.max_channel)
    ]
    if len(matches) != 1:
        raise SkyModelError(f"{len(matches)} sky models are for centre channel {centre_channel}. Expected exactly 1.")

    return matches[0]


def select_sky_model_for_metafits(models: list[SkyModel], metafits_context: MetafitsContext) -> SkyModel:
    """Pick the sky model for an observation from its metafits, and log the choice.

    Args:
        models: The sky models (already checked by validate_sky_models()).
        metafits_context: The observation's metafits (mwalib).

    Returns:
        The sky model for the observation's centre coarse channel.

    Raises:
        SkyModelError: If the centre channel cannot be worked out or no sky model is for it.
    """
    centre_channel = centre_coarse_channel(metafits_context.centre_freq_hz)
    if centre_channel < 1:
        raise SkyModelError(
            f"The centre frequency {metafits_context.centre_freq_hz} Hz gives centre channel {centre_channel}."
        )

    model = select_sky_model(models, centre_channel)
    logger.info(
        f"Centre channel {centre_channel}: using sky model [{SECTION_SKY_MODEL_PREFIX}{model.name}]"
        f" {model.filename} (type {model.source_list_type}, {model.num_sources} sources, {model.describe_range()})."
    )
    return model
