"""INI config-file reading helpers, built on configparser.

Covers required values (read_config), optional values that may be absent or
empty (read_optional_config), comma-separated lists (read_config_list), and
booleans (read_config_bool). All accept an optional Base64-decode step for
values the config file stores encoded.
"""

import base64
import logging
from configparser import ConfigParser

logger = logging.getLogger(__name__)


def read_config(config: ConfigParser, section: str, key: str, b64encoded=False):
    """
    Read a required string value from a ConfigParser object.

    If ``b64encoded`` is True the stored value is treated as Base64-encoded
    UTF-8 and decoded before being returned. The decoded value is masked in
    log output.

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key.
        key: The key name within the section.
        b64encoded: If True, Base64-decode the raw string value before
            returning it. Defaults to False.

    Returns:
        The configuration value as a string, Base64-decoded if requested.

    Raises:
        configparser.NoSectionError: If the section does not exist.
        configparser.NoOptionError: If the key does not exist within the section.
    """
    raw_value = config.get(section, key)

    if b64encoded:
        value = base64.b64decode(raw_value).decode("utf-8")
        value_to_log = "*" * len(value)
    else:
        value = raw_value
        value_to_log = value

    logger.info(f"Read cfg [{section}].{key} == {value_to_log}")
    return value


def read_optional_config(config: ConfigParser, section: str, key: str, b64encoded=False) -> str | None:
    """
    Read an optional string value from a ConfigParser object.

    Returns None if the key is absent or its value is an empty string.
    If ``b64encoded`` is True the stored value is treated as Base64-encoded
    UTF-8 and decoded before being returned. The decoded value is masked in
    log output.

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key. The section must
            exist or KeyError is raised.
        key: The key name within the section. If absent, returns None.
        b64encoded: If True, Base64-decode the raw string value before
            returning it. Defaults to False. Has no effect if the value is
            absent or empty.

    Returns:
        The configuration value as a string (Base64-decoded if requested),
        or None if the key is missing or empty.

    Raises:
        KeyError: If ``section`` does not exist in the config.
    """
    value = None
    value_to_log = ""

    if config.has_section(section):
        if config.has_option(section, key):
            raw_value = config.get(section, key)
        else:
            raw_value = ""
    else:
        raise KeyError(f"Section {section} not found in config file")

    if raw_value == "":
        value = None
        value_to_log = "None"
    else:
        if b64encoded:
            value = base64.b64decode(raw_value).decode("utf-8")
            value_to_log = "*" * len(value)
        else:
            value = raw_value
            value_to_log = value

    logger.info(f"Read cfg [{section}].{key} == {value_to_log}")
    return value


def read_config_list(config: ConfigParser, section: str, key: str):
    """
    Read a comma-separated string value from a ConfigParser object and return it as a list.

    Leading and trailing whitespace is stripped from the raw value before
    splitting. An empty (or whitespace-only) value returns an empty list.

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key.
        key: The key name within the section whose value is a comma-separated list.

    Returns:
        A list of strings split on commas. Returns an empty list if the value
        is blank.

    Raises:
        configparser.NoSectionError: If the section does not exist.
        configparser.NoOptionError: If the key does not exist within the section.
    """
    string_value = read_config(config, section, key, False)

    # Ensure we trim string_value
    string_value = string_value.rstrip().lstrip()

    if len(string_value) > 0:
        return_list = string_value.split(",")
    else:
        return_list = []

    logger.info(
        f"Read cfg [{section}].{key}: '{string_value}' converted to list of {len(return_list)} items: {return_list}"
    )
    return return_list


def read_config_bool(config: ConfigParser, section: str, key: str):
    """
    Read a boolean value from a ConfigParser object.

    Delegates to ConfigParser.getboolean(), which accepts the standard
    truthy/falsy strings (``'1'``, ``'yes'``, ``'true'``, ``'on'`` and their
    negatives).

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key.
        key: The key name within the section.

    Returns:
        The configuration value as a bool.

    Raises:
        configparser.NoSectionError: If the section does not exist.
        configparser.NoOptionError: If the key does not exist within the section.
        ValueError: If the value cannot be interpreted as a boolean.
    """
    value = config.getboolean(section, key)

    logger.info(f"Read cfg [{section}].{key} == {value}")
    return value
