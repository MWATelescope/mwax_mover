"""Tests for calvin.sky_model: choosing the hyperdrive sky model from the observation centre channel."""

import os
from configparser import ConfigParser
from pathlib import Path

import mwalib
import pytest

from mwax_mover.calvin.sky_model import (
    SkyModel,
    SkyModelError,
    select_sky_model,
    select_sky_model_for_metafits,
    sky_model_file_problems,
    sky_models_from_config,
    validate_sky_models,
)

TWO_MODELS = """
[hyperdrive]
num_sources=1000

[skymodel:low]
max_centre_channel=168
source_list_filename=/srclists/GGSM_updated.fits
source_list_type=fits

[skymodel:high]
min_centre_channel=169
source_list_filename=/srclists/other.txt
source_list_type=rts
num_sources=2000
"""


def _config(text: str) -> ConfigParser:
    config = ConfigParser()
    config.read_string(text)
    return config


def _model(name: str, low: int | None, high: int | None) -> SkyModel:
    return SkyModel(name, f"/srclists/{name}.fits", "fits", 1000, low, high)


def _test_metafits(obs_id: int) -> str:
    return os.path.join("tests", "data", str(obs_id), f"{obs_id}_metafits.fits")


# ---------------------------------------------------------------------------
# reading the config
# ---------------------------------------------------------------------------


def test_reads_the_sky_models_with_their_file_type_and_channel_range():
    low, high = sky_models_from_config(_config(TWO_MODELS))

    assert low == SkyModel("low", "/srclists/GGSM_updated.fits", "fits", 1000, None, 168)
    assert high == SkyModel("high", "/srclists/other.txt", "rts", 2000, 169, None)


def test_the_models_are_in_channel_order_whatever_the_order_in_the_config():
    text = """
[hyperdrive]
num_sources=10

[skymodel:high]
min_centre_channel=169
source_list_filename=/srclists/high.fits
source_list_type=fits

[skymodel:low]
max_centre_channel=168
source_list_filename=/srclists/low.fits
source_list_type=fits
"""

    assert [m.name for m in sky_models_from_config(_config(text))] == ["low", "high"]


def test_num_sources_defaults_to_the_hyperdrive_value_and_can_be_set_per_model():
    low, high = sky_models_from_config(_config(TWO_MODELS))

    assert (low.num_sources, high.num_sources) == (1000, 2000)


def test_the_type_is_not_case_sensitive():
    text = TWO_MODELS.replace("source_list_type=fits", "source_list_type=FITS")

    assert sky_models_from_config(_config(text))[0].source_list_type == "fits"


@pytest.mark.parametrize("source_list_type", ["hyperdrive", "fits", "rts", "woden", "ao", "dp3", "skymodel", "bbs"])
def test_every_hyperdrive_source_list_type_is_accepted(source_list_type):
    text = TWO_MODELS.replace("source_list_type=fits", f"source_list_type={source_list_type}")

    assert sky_models_from_config(_config(text))[0].source_list_type == source_list_type


def test_a_type_that_hyperdrive_does_not_have_is_an_error():
    text = TWO_MODELS.replace("source_list_type=fits", "source_list_type=yaml")

    with pytest.raises(SkyModelError, match=r"\[skymodel:low\] source_list_type 'yaml' is not one of"):
        sky_models_from_config(_config(text))


@pytest.mark.parametrize("key", ["source_list_filename", "source_list_type"])
def test_a_missing_required_key_is_named_in_the_error(key):
    text = "".join(line for line in TWO_MODELS.splitlines(keepends=True) if not line.startswith(key))
    # Removes the key from both models: the first one in channel order is reported

    with pytest.raises(SkyModelError, match=rf"has no {key}"):
        sky_models_from_config(_config(text))


def _with_extra_key(section: str, line: str) -> str:
    """TWO_MODELS with one more line at the start of a section."""
    return TWO_MODELS.replace(f"[skymodel:{section}]\n", f"[skymodel:{section}]\n{line}\n", 1)


@pytest.mark.parametrize("value", ["0", "-3", "high", "1.5", ""])
def test_a_bad_num_sources_is_named_in_the_error(value):
    text = _with_extra_key("low", f"num_sources={value}")

    with pytest.raises(SkyModelError, match=r"\[skymodel:low\] num_sources must be a whole number of at least 1"):
        sky_models_from_config(_config(text))


@pytest.mark.parametrize("value", ["0", "-3", "high", "1.5", ""])
def test_a_bad_minimum_channel_is_named_in_the_error(value):
    # [skymodel:low] has a maximum already, so a minimum of 'value' is the one that is read first
    text = _with_extra_key("low", f"min_centre_channel={value}")

    with pytest.raises(SkyModelError, match=r"min_centre_channel must be a whole number of at least 1"):
        sky_models_from_config(_config(text))


@pytest.mark.parametrize("value", ["0", "-3", "high", "1.5", ""])
def test_a_bad_maximum_channel_is_named_in_the_error(value):
    text = _with_extra_key("high", f"max_centre_channel={value}")

    with pytest.raises(SkyModelError, match=r"max_centre_channel must be a whole number of at least 1"):
        sky_models_from_config(_config(text))


def test_the_minimum_above_the_maximum_in_one_section_is_an_error():
    text = TWO_MODELS.replace("max_centre_channel=168", "min_centre_channel=170\nmax_centre_channel=168")

    with pytest.raises(SkyModelError, match="is above max_centre_channel"):
        sky_models_from_config(_config(text))


def test_a_section_with_no_name_is_an_error():
    text = TWO_MODELS.replace("[skymodel:low]", "[skymodel:]")

    with pytest.raises(SkyModelError, match="needs a name"):
        sky_models_from_config(_config(text))


@pytest.mark.parametrize("old_key", ["source_list_filename", "source_list_type"])
def test_the_old_keys_in_the_hyperdrive_section_are_an_error(old_key):
    text = TWO_MODELS.replace("num_sources=1000\n", f"num_sources=1000\n{old_key}=/x\n", 1)

    with pytest.raises(SkyModelError, match=rf"\[hyperdrive\] {old_key} is not used any more"):
        sky_models_from_config(_config(text))


def test_a_config_with_no_sky_model_section_is_an_error():
    with pytest.raises(SkyModelError, match="At least one is needed"):
        sky_models_from_config(_config("[hyperdrive]\nnum_sources=1000\n"))


def test_a_single_section_with_no_range_covers_every_channel():
    text = """
[hyperdrive]
num_sources=10

[skymodel:all]
source_list_filename=/srclists/all.fits
source_list_type=fits
"""
    (model,) = sky_models_from_config(_config(text))

    assert model.describe_range() == "all channels"
    assert all(select_sky_model([model], channel) is model for channel in (1, 169, 255))


# ---------------------------------------------------------------------------
# the ranges must cover every channel exactly once
# ---------------------------------------------------------------------------


def test_touching_ranges_are_valid():
    validate_sky_models([_model("a", None, 168), _model("b", 169, 200), _model("c", 201, None)])


def test_the_order_of_the_models_does_not_matter():
    validate_sky_models([_model("c", 201, None), _model("a", None, 168), _model("b", 169, 200)])


def test_overlapping_ranges_are_an_error():
    with pytest.raises(SkyModelError, match=r"\[skymodel:a\].*\[skymodel:b\].*overlap"):
        validate_sky_models([_model("a", None, 169), _model("b", 169, None)])


def test_identical_ranges_are_an_error():
    with pytest.raises(SkyModelError, match="overlap"):
        validate_sky_models([_model("a", None, 20), _model("b", None, 20)])


def test_two_ranges_with_no_minimum_are_an_error():
    with pytest.raises(SkyModelError, match="overlap"):
        validate_sky_models([_model("a", None, 100), _model("b", None, 200)])


def test_a_range_with_no_maximum_that_is_not_the_highest_is_an_error():
    with pytest.raises(SkyModelError, match="overlap"):
        validate_sky_models([_model("a", None, None), _model("b", 169, None)])


def test_a_gap_between_ranges_is_an_error():
    with pytest.raises(SkyModelError, match="no sky model for channels 169 to 170"):
        validate_sky_models([_model("a", None, 168), _model("b", 171, None)])


def test_a_gap_below_the_lowest_range_is_an_error():
    with pytest.raises(SkyModelError, match=r"no sky model for channels below 10: .*\[skymodel:a\]"):
        validate_sky_models([_model("a", 10, 100), _model("b", 101, None)])


def test_a_gap_above_the_highest_range_is_an_error():
    with pytest.raises(SkyModelError, match=r"no sky model for channels above 200: .*\[skymodel:b\]"):
        validate_sky_models([_model("a", None, 100), _model("b", 101, 200)])


# ---------------------------------------------------------------------------
# choosing a model
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("channel", "name"),
    [(1, "low"), (143, "low"), (168, "low"), (169, "high"), (170, "high"), (255, "high")],
)
def test_the_boundary_is_168_low_and_169_high(channel, name):
    models = sky_models_from_config(_config(TWO_MODELS))

    assert select_sky_model(models, channel).name == name


def test_choosing_with_models_that_were_not_validated_can_fail():
    with pytest.raises(SkyModelError, match="0 sky models are for centre channel 169"):
        select_sky_model([_model("a", None, 168)], 169)
    with pytest.raises(SkyModelError, match="2 sky models are for centre channel 150"):
        select_sky_model([_model("a", None, 168), _model("b", 100, None)], 150)


@pytest.mark.parametrize(
    ("obs_id", "name"),
    [
        (1094488624, "low"),  # centre channel 143
        (1220738720, "low"),  # centre channel 121
        (1122979144, "high"),  # centre channel 169
    ],
)
def test_the_model_for_a_real_metafits(obs_id, name):
    metafits = _test_metafits(obs_id)
    if not os.path.exists(metafits):
        pytest.skip(f"{metafits} is not there")
    models = sky_models_from_config(_config(TWO_MODELS))

    assert select_sky_model_for_metafits(models, mwalib.MetafitsContext(metafits, None)).name == name


def test_a_centre_frequency_with_no_channel_is_an_error():
    class NoFrequency:
        centre_freq_hz = 0.0

    with pytest.raises(SkyModelError, match="centre channel 0"):
        select_sky_model_for_metafits(sky_models_from_config(_config(TWO_MODELS)), NoFrequency())  # ty: ignore[invalid-argument-type]


# ---------------------------------------------------------------------------
# the files
# ---------------------------------------------------------------------------


def test_every_sky_model_file_is_checked(tmp_path):
    low = tmp_path / "low.fits"
    low.write_text("x")
    models = [
        SkyModel("low", str(low), "fits", 10, None, 168),
        SkyModel("high", str(tmp_path / "missing.fits"), "fits", 10, 169, None),
    ]

    problems = sky_model_file_problems(models)

    assert len(problems) == 1
    assert "[skymodel:high] source_list_filename" in problems[0]
    assert "missing.fits does not exist" in problems[0]


def test_no_problems_if_all_the_files_exist(tmp_path):
    path = tmp_path / "all.fits"
    path.write_text("x")

    assert sky_model_file_problems([SkyModel("all", str(path), "fits", 10)]) == []


# ---------------------------------------------------------------------------
# the config files in the repo
# ---------------------------------------------------------------------------

REPO_CFG_DIR = Path(__file__).resolve().parents[2] / "cfg"


@pytest.mark.parametrize("cfg_name", ["calvin_processor.cfg", "calvin_processor_test.cfg"])
def test_the_cfg_files_in_the_repo_have_a_low_and_a_high_sky_model(cfg_name):
    config = ConfigParser()
    assert config.read(REPO_CFG_DIR / cfg_name) == [str(REPO_CFG_DIR / cfg_name)]

    low, high = sky_models_from_config(config)

    assert (os.path.basename(low.filename), low.source_list_type) == ("GGSM_updated.fits", "fits")
    assert (low.min_channel, low.max_channel) == (None, 168)
    assert (os.path.basename(high.filename), high.source_list_type) == ("Total-300MHz-skymodel.fits", "fits")
    assert (high.min_channel, high.max_channel) == (169, None)
    assert os.path.dirname(low.filename) == os.path.dirname(high.filename)
    assert low.num_sources == high.num_sources == 1000
