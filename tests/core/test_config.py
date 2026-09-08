"""Tests for core.config's INI config-file reading helpers.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).
"""

from configparser import ConfigParser

import pytest

from tests_common import render_test_config

from mwax_mover.core.config import read_config_bool, read_config_list, read_optional_config


def test_config_get_list_valid():
    """Read a string from a config file, then
    split (by comma) into a list
    e.g. abc,def,ghi would result in ["abc", "def", "ghi"]
    An empty string would result in and empty list []
    """

    config_filename = render_test_config("config")
    config = ConfigParser()
    config.read(config_filename, encoding="utf-8")

    return_list = read_config_list(config, "correlator", "high_priority_vcs_projectids")

    assert return_list == ["D0006", "G0058"]


def test_config_get_bool_true():

    config_filename = render_test_config("config")
    config = ConfigParser()
    config.read(config_filename, encoding="utf-8")

    true_bool = read_config_bool(config, "mwax mover", "archiving_enabled")

    assert true_bool is True


def test_config_get_bool_false():

    config_filename = render_test_config("config")
    config = ConfigParser()
    config.read(config_filename, encoding="utf-8")

    false_bool = read_config_bool(config, "beamformer", "bf_keep_original_files_after_stitching")

    assert false_bool is False


def test_config_get_list_empty():
    """Read a string from a config file, then
    split (by comma) into a list
    e.g. abc,def,ghi would result in ["abc", "def", "ghi"]
    An empty string would result in and empty list []
    """

    config_filename = render_test_config("config")
    config = ConfigParser()
    config.read(config_filename, encoding="utf-8")

    return_list = read_config_list(config, "correlator", "high_priority_correlator_projectids")

    assert return_list == []


def test_config_get_optional_value():
    """Read an empty string from a config file and ensure it gets
    treated as None. Also test an non empty gets read right too"""

    config_filename = render_test_config("config")
    config = ConfigParser()
    config.read(config_filename, encoding="utf-8")

    empty_return_val = read_optional_config(config, "correlator", "high_priority_correlator_projectids")

    non_empty_return_val = read_optional_config(config, "correlator", "mwax_stats_timeout_sec")

    non_existing_key = read_optional_config(config, "correlator", "non_existant_key")

    assert empty_return_val is None
    assert non_empty_return_val is not None
    assert non_existing_key is None

    # Section that doesn't exist raises error
    with pytest.raises(KeyError):
        non_existing_key = read_optional_config(config, "non_existant_section", "non_existant_key")


def test_config_get_optional_value_spaces_not_empty_string():
    """Read an empty string which has spaces in it from a config file and ensure it gets
    treated as None."""

    config_filename = render_test_config("config")
    config = ConfigParser()
    config.read(config_filename, encoding="utf-8")

    empty_return_val = read_optional_config(config, "correlator", "test_with_spaces")

    assert empty_return_val is None
