"""Tests for the WSClean options: the command line, the config and the memory limit."""

from configparser import ConfigParser

import pytest

from mwax_mover.calvin.wsclean import WSCleanArgs, calculate_abs_mem_gib, wsclean_args_from_config

CALVIN_WSCLEAN_CONFIG = {
    "enabled": "1",
    "max_mem_gib": "830",
    "size": "3072 3072",
    "scale": "0.75amin",
    "briggs_robust": "-1.0",
    "niter": "100000",
    "mgain": "0.95",
    "auto_threshold": "1",
    "auto_mask": "5",
    "channels_out": "4",
    "multiscale": "1",
    "join_channels": "1",
    "join_polarizations": "1",
    "log_time": "1",
    "verbose": "1",
}


def _config(**overrides: str) -> ConfigParser:
    config = ConfigParser()
    config.read_dict({"wsclean": {**CALVIN_WSCLEAN_CONFIG, **overrides}})
    return config


def test_to_cmdline_args_with_all_the_calvin_options():
    args = wsclean_args_from_config(_config())
    args.name = "/out/123_wsclean"
    args.temp_dir = "/work"
    args.abs_mem_gib = 600

    assert args.to_cmdline_args() == [
        "-name",
        "/out/123_wsclean",
        "-temp-dir",
        "/work",
        "-abs-mem",
        "600",
        "-size",
        "3072",
        "3072",
        "-scale",
        "0.75amin",
        "-weight",
        "briggs",
        "-1",
        "-niter",
        "100000",
        "-mgain",
        "0.95",
        "-auto-threshold",
        "1",
        "-auto-mask",
        "5",
        "-channels-out",
        "4",
        "-gridder",
        "idg",
        "-idg-mode",
        "hybrid",
        "-pol",
        "xx,yy",
        "-multiscale",
        "-join-channels",
        "-join-polarizations",
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


def test_to_cmdline_args_does_not_set_the_thread_count():
    assert "-j" not in wsclean_args_from_config(_config()).to_cmdline_args()


def test_wsclean_args_from_config_reads_the_values_and_fixes_gridder_and_pol():
    args = wsclean_args_from_config(_config(size="1024,2048", briggs_robust="0.5", multiscale="0", verbose="no"))

    assert args.size == (1024, 2048)
    assert args.briggs_robust == 0.5
    assert args.multiscale is False
    assert args.verbose is False
    assert args.gridder == "idg"
    assert args.idg_mode == "hybrid"
    assert args.polarizations == "xx,yy"
    assert args.name == ""
    assert args.abs_mem_gib is None


@pytest.mark.parametrize("key", ["size", "scale", "briggs_robust", "niter", "mgain", "auto_threshold", "channels_out"])
def test_wsclean_args_from_config_needs_every_option(key):
    config = _config()
    config.remove_option("wsclean", key)

    with pytest.raises(Exception, match=key):
        wsclean_args_from_config(config)


@pytest.mark.parametrize("size", ["3072", "1 2 3", "0 100", "-5 100", "big big"])
def test_wsclean_args_from_config_rejects_a_bad_size(size):
    with pytest.raises(ValueError, match="size"):
        wsclean_args_from_config(_config(size=size))


def test_join_channels_needs_more_than_one_output_channel():
    with pytest.raises(ValueError, match="join_channels"):
        wsclean_args_from_config(_config(channels_out="1", join_channels="1"))

    # Fine if the channels are not joined
    assert wsclean_args_from_config(_config(channels_out="1", join_channels="0")).channels_out == 1


def test_join_polarizations_needs_two_polarizations():
    args = WSCleanArgs(name="", temp_dir="", join_polarizations=True, polarizations="xx")

    with pytest.raises(ValueError, match="join_polarizations"):
        args.validate()


def test_channels_out_must_be_at_least_one():
    with pytest.raises(ValueError, match="channels_out"):
        wsclean_args_from_config(_config(channels_out="0", join_channels="0"))


def test_calculate_abs_mem_gib_subtracts_the_ram_backed_files():
    # 100 GB is about 93.13 GiB, so 93 whole GiB are taken off
    assert calculate_abs_mem_gib(830, 100) == 737


def test_calculate_abs_mem_gib_with_files_on_disk_keeps_all_the_memory():
    assert calculate_abs_mem_gib(830, 0) == 830


def test_calculate_abs_mem_gib_can_be_negative_if_the_files_are_too_big():
    assert calculate_abs_mem_gib(830, 1000) < 0
