"""Tests for the WSClean image size and pixel scale that are worked out for each observation."""

import glob
import os

import mwalib
import numpy as np
import pytest
from astropy.io import fits

from mwax_mover.calvin.wsclean import (
    array_image_settings,
    array_image_settings_from_metafits,
    centre_coarse_channel,
    max_baseline_length_m,
)
from mwax_mover.constants import WSCLEAN_COMPACT_IMAGE_SIZE, WSCLEAN_EXTENDED_IMAGE_SIZE

COMPACT_MAX = 3500.0
# The sizes that the tests below that name 4000 and 8000 pixels use, whatever the defaults are
SIZES_4000_8000 = {"compact_image_size": 4000, "extended_image_size": 8000}


def _test_metafits(obs_id: int) -> str:
    return os.path.join("tests", "data", str(obs_id), f"{obs_id}_metafits.fits")


# ---------------------------------------------------------------------------
# centre channel
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("centre_freq_hz", "expected"),
    [
        (154_240_000, 121),  # 120.5: rounds up, python's round() would give 120
        (215_680_000, 169),  # 168.5: round() would give 168
        (147_840_000, 116),  # 115.5
        (163_200_000, 128),  # 127.5, but 163.2 / 1.28 is 127.49999999999999 in floating point
        (182_415_000, 143),  # 142.51
        (1.28e6 * 133, 133),  # a whole number of channels
        (1.28e6 * 133.49, 133),
    ],
)
def test_centre_coarse_channel_rounds_half_up(centre_freq_hz, expected):
    assert centre_coarse_channel(centre_freq_hz) == expected


def _all_test_metafits() -> list[str]:
    return sorted(glob.glob(os.path.join("tests", "data", "*", "*_metafits.fits")))


@pytest.mark.parametrize("metafits", _all_test_metafits())
def test_centre_coarse_channel_gives_the_metafits_centchan(metafits):
    try:
        context = mwalib.MetafitsContext(metafits, None)
    except mwalib.MwalibError:
        pytest.skip(f"mwalib cannot open {metafits}")

    assert centre_coarse_channel(context.centre_freq_hz) == fits.getheader(metafits)["CENTCHAN"]


# ---------------------------------------------------------------------------
# longest baseline
# ---------------------------------------------------------------------------


def test_max_baseline_length_is_the_longest_distance_between_any_two_tiles():
    positions = np.array([(0, 0, 0), (3, 4, 0), (0, 0, 12)])

    assert max_baseline_length_m(positions) == 13.0  # between the second and the third


def test_max_baseline_length_uses_the_height():
    assert max_baseline_length_m(np.array([(0, 0, 0), (0, 0, 10)])) == 10.0


@pytest.mark.parametrize("positions", [np.empty((0, 3)), np.array([(1.0, 2.0, 3.0)])])
def test_max_baseline_length_is_zero_for_fewer_than_two_tiles(positions):
    assert max_baseline_length_m(positions) == 0.0


# ---------------------------------------------------------------------------
# compact or extended
# ---------------------------------------------------------------------------


def test_a_compact_array_has_a_4000_pixel_image_and_a_base_scale_of_1_2():
    settings = array_image_settings(128, 2873.5, 1.28e6 * 133, COMPACT_MAX, **SIZES_4000_8000)

    assert settings.configuration == "compact"
    assert settings.size == (4000, 4000)
    assert settings.centre_channel == 133
    assert settings.scale == "0.0090226"  # 1.2 / 133


def test_an_extended_array_has_an_8000_pixel_image_and_a_base_scale_of_0_6():
    settings = array_image_settings(128, 5302.9, 1.28e6 * 133, COMPACT_MAX, **SIZES_4000_8000)

    assert settings.configuration == "extended"
    assert settings.size == (8000, 8000)
    assert settings.scale == "0.0045113"  # 0.6 / 133


def test_a_baseline_exactly_at_the_limit_is_compact():
    assert array_image_settings(128, 3500.0, 1.28e6 * 133, COMPACT_MAX).configuration == "compact"
    assert array_image_settings(128, 3500.001, 1.28e6 * 133, COMPACT_MAX).configuration == "extended"


def test_the_limit_is_the_one_that_is_given():
    assert array_image_settings(128, 2873.5, 1.28e6 * 133, 2000.0).configuration == "extended"
    assert array_image_settings(128, 2873.5, 1.28e6 * 133, 3000.0).configuration == "compact"


@pytest.mark.parametrize(("num_tiles", "configuration"), [(183, "compact"), (184, "extended"), (256, "extended")])
def test_184_tiles_or_more_is_always_extended_whatever_the_baseline(num_tiles, configuration):
    assert array_image_settings(num_tiles, 100.0, 1.28e6 * 133, COMPACT_MAX).configuration == configuration


def test_the_scale_has_seven_decimal_places():
    assert array_image_settings(256, 5000.0, 1.28e6 * 200, COMPACT_MAX).scale == "0.0030000"


def test_a_centre_frequency_that_gives_channel_zero_is_an_error():
    with pytest.raises(ValueError, match="centre channel 0"):
        array_image_settings(128, 1000.0, 0.0, COMPACT_MAX)


# ---------------------------------------------------------------------------
# from a real metafits
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("obs_id", "configuration", "size", "scale"),
    [
        (1220738720, "compact", 4000, "0.0099174"),  # 741 m, channel 121
        (1094488624, "compact", 4000, "0.0083916"),  # 2874 m, channel 143
        (1122979144, "compact", 4000, "0.0071006"),  # 2874 m, channel 169
        (1244973688, "extended", 8000, "0.0051724"),  # 5303 m, channel 116, only 128 tiles
    ],
)
def test_settings_from_a_real_metafits(obs_id, configuration, size, scale):
    metafits = _test_metafits(obs_id)
    if not os.path.exists(metafits):
        pytest.skip(f"{metafits} is not there")

    settings = array_image_settings_from_metafits(
        mwalib.MetafitsContext(metafits, None), COMPACT_MAX, **SIZES_4000_8000
    )

    assert settings.configuration == configuration
    assert settings.size == (size, size)
    assert settings.scale == scale
    assert settings.num_tiles == 128


def test_a_smaller_limit_makes_a_compact_observation_extended():
    metafits = _test_metafits(1094488624)
    if not os.path.exists(metafits):
        pytest.skip(f"{metafits} is not there")

    context = mwalib.MetafitsContext(metafits, None)

    assert array_image_settings_from_metafits(context, 3500.0).configuration == "compact"
    assert array_image_settings_from_metafits(context, 2500.0).configuration == "extended"


# ---------------------------------------------------------------------------
# the image sizes can be given (they are config keys)
# ---------------------------------------------------------------------------


def test_the_given_image_sizes_are_used_for_a_compact_and_an_extended_array():
    sizes = {"compact_image_size": 1500, "extended_image_size": 3000}

    compact = array_image_settings(128, 2873.5, 1.28e6 * 133, COMPACT_MAX, **sizes)
    extended = array_image_settings(128, 5302.9, 1.28e6 * 133, COMPACT_MAX, **sizes)

    assert (compact.configuration, compact.size) == ("compact", (1500, 1500))
    assert (extended.configuration, extended.size) == ("extended", (3000, 3000))


def test_the_image_sizes_do_not_change_the_scale():
    default = array_image_settings(128, 2873.5, 1.28e6 * 133, COMPACT_MAX)
    resized = array_image_settings(128, 2873.5, 1.28e6 * 133, COMPACT_MAX, compact_image_size=77)

    assert resized.scale == default.scale


def test_the_image_sizes_default_to_the_constants():
    compact = array_image_settings(128, 2873.5, 1.28e6 * 133, COMPACT_MAX)
    extended = array_image_settings(128, 5302.9, 1.28e6 * 133, COMPACT_MAX)

    assert compact.size == (WSCLEAN_COMPACT_IMAGE_SIZE, WSCLEAN_COMPACT_IMAGE_SIZE)
    assert extended.size == (WSCLEAN_EXTENDED_IMAGE_SIZE, WSCLEAN_EXTENDED_IMAGE_SIZE)


@pytest.mark.parametrize(
    ("obs_id", "configuration", "size"), [(1094488624, "compact", 1111), (1244973688, "extended", 2222)]
)
def test_the_given_image_sizes_are_used_with_a_real_metafits(obs_id, configuration, size):
    metafits = _test_metafits(obs_id)
    if not os.path.exists(metafits):
        pytest.skip(f"{metafits} is not there")

    settings = array_image_settings_from_metafits(
        mwalib.MetafitsContext(metafits, None), COMPACT_MAX, compact_image_size=1111, extended_image_size=2222
    )

    assert settings.configuration == configuration
    assert settings.size == (size, size)
