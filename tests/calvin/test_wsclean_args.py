"""Tests for the WSClean options: the command line, the config and the memory limit."""

from configparser import ConfigParser

import pytest

from mwax_mover.calvin.wsclean import (
    WSCleanArgs,
    calculate_abs_mem_gib,
    read_compact_array_baseline_max_length,
    wsclean_args_from_config,
)

CALVIN_WSCLEAN_CONFIG = {
    "enabled": "1",
    "max_mem_gib": "830",
    "compact_array_baseline_max_length": "3500",
    "briggs_robust": "0",
    "niter": "1000000",
    "nmiter": "2",
    "mgain": "0.85",
    "multiscale_gain": "0.15",
    "auto_threshold": "5",
    "auto_mask": "7",
    "minuv_l": "100",
    "channels_out": "4",
    "multiscale": "1",
    "join_channels": "1",
    "log_time": "1",
    "verbose": "1",
}


def _config(**overrides: str) -> ConfigParser:
    config = ConfigParser()
    config.read_dict({"wsclean": {**CALVIN_WSCLEAN_CONFIG, **overrides}})
    return config


def test_to_cmdline_args_with_all_the_calvin_options():
    args = wsclean_args_from_config(_config())
    # These are set for each job and each observation, not from the config
    args.name = "/out/123_wsclean"
    args.temp_dir = "/work"
    args.abs_mem_gib = 600
    args.size = (8000, 8000)
    args.scale = "0.0045113"

    assert args.to_cmdline_args() == [
        "-name",
        "/out/123_wsclean",
        "-temp-dir",
        "/work",
        "-abs-mem",
        "600",
        "-size",
        "8000",
        "8000",
        "-scale",
        "0.0045113",
        "-weight",
        "briggs",
        "0",
        "-niter",
        "1000000",
        "-nmiter",
        "2",
        "-mgain",
        "0.85",
        "-multiscale-gain",
        "0.15",
        "-auto-threshold",
        "5",
        "-auto-mask",
        "7",
        "-minuv-l",
        "100",
        "-channels-out",
        "4",
        "-gridder",
        "wgridder",
        "-pol",
        "i",
        "-multiscale",
        "-join-channels",
        "-log-time",
        "-verbose",
        "-no-update-model-required",
    ]


def test_to_cmdline_args_leaves_out_options_that_are_not_set():
    args = WSCleanArgs(name="/out/x", temp_dir="/work", multiscale=True)

    assert args.to_cmdline_args() == [
        "-name",
        "/out/x",
        "-temp-dir",
        "/work",
        "-multiscale",
        "-no-update-model-required",
    ]


def test_the_config_gives_no_size_or_scale_so_the_command_line_has_none_until_they_are_set():
    args = wsclean_args_from_config(_config())

    assert "-size" not in args.to_cmdline_args()
    assert "-scale" not in args.to_cmdline_args()


def test_to_cmdline_args_does_not_set_the_thread_count():
    assert "-j" not in wsclean_args_from_config(_config()).to_cmdline_args()


def test_to_cmdline_args_uses_the_wgridder_and_stokes_i_and_no_idg_options():
    command_line = wsclean_args_from_config(_config()).to_cmdline_args()

    assert command_line[command_line.index("-gridder") + 1] == "wgridder"
    assert command_line[command_line.index("-pol") + 1] == "i"
    for not_used in ("-idg-mode", "-use-idg", "-link-polarizations", "-join-polarizations"):
        assert not_used not in command_line


def test_wsclean_args_from_config_reads_the_values_and_fixes_gridder_and_pol():
    args = wsclean_args_from_config(_config(briggs_robust="0.5", multiscale="0", verbose="no", minuv_l="50.5"))

    assert args.briggs_robust == 0.5
    assert args.niter == 1000000
    assert args.nmiter == 2
    assert args.mgain == 0.85
    assert args.multiscale_gain == 0.15
    assert args.auto_threshold == 5
    assert args.auto_mask == 7
    assert args.minuv_l == 50.5
    assert args.channels_out == 4
    assert args.multiscale is False
    assert args.verbose is False
    assert args.gridder == "wgridder"
    assert args.polarizations == "i"
    assert args.name == ""
    assert args.abs_mem_gib is None
    assert args.size is None
    assert args.scale is None


@pytest.mark.parametrize(
    "key",
    [
        "briggs_robust",
        "niter",
        "nmiter",
        "mgain",
        "multiscale_gain",
        "auto_threshold",
        "auto_mask",
        "minuv_l",
        "channels_out",
    ],
)
def test_wsclean_args_from_config_needs_every_option(key):
    config = _config()
    config.remove_option("wsclean", key)

    with pytest.raises(Exception, match=key):
        wsclean_args_from_config(config)


def test_size_and_scale_are_no_longer_config_keys():
    # They are worked out for each observation, so a config that still has them works, and they are ignored
    args = wsclean_args_from_config(_config(size="3072 3072", scale="0.75amin"))

    assert args.size is None
    assert args.scale is None


@pytest.mark.parametrize("size", [(0, 100), (100, 0), (-5, 100)])
def test_a_size_that_is_not_positive_is_not_valid(size):
    with pytest.raises(ValueError, match="size"):
        WSCleanArgs(name="", temp_dir="", size=size).validate()


def test_join_channels_needs_more_than_one_output_channel():
    with pytest.raises(ValueError, match="join_channels"):
        wsclean_args_from_config(_config(channels_out="1", join_channels="1"))

    # Fine if the channels are not joined
    assert wsclean_args_from_config(_config(channels_out="1", join_channels="0")).channels_out == 1


def test_channels_out_must_be_at_least_one():
    with pytest.raises(ValueError, match="channels_out"):
        wsclean_args_from_config(_config(channels_out="0", join_channels="0"))


def test_read_compact_array_baseline_max_length():
    assert read_compact_array_baseline_max_length(_config()) == 3500.0
    assert read_compact_array_baseline_max_length(_config(compact_array_baseline_max_length="2999.5")) == 2999.5


def test_the_compact_array_baseline_max_length_is_needed():
    config = _config()
    config.remove_option("wsclean", "compact_array_baseline_max_length")

    with pytest.raises(Exception, match="compact_array_baseline_max_length"):
        read_compact_array_baseline_max_length(config)


@pytest.mark.parametrize("value", ["0", "-1", "long", "nan", ""])
def test_the_compact_array_baseline_max_length_must_be_a_number_of_metres_above_zero(value):
    with pytest.raises(ValueError, match="compact_array_baseline_max_length"):
        read_compact_array_baseline_max_length(_config(compact_array_baseline_max_length=value))


def test_calculate_abs_mem_gib_subtracts_the_ram_backed_files():
    # 100 GB is about 93.13 GiB, so 93 whole GiB are taken off
    assert calculate_abs_mem_gib(830, 100) == 737


def test_calculate_abs_mem_gib_with_files_on_disk_keeps_all_the_memory():
    assert calculate_abs_mem_gib(830, 0) == 830


def test_calculate_abs_mem_gib_can_be_negative_if_the_files_are_too_big():
    assert calculate_abs_mem_gib(830, 1000) < 0
