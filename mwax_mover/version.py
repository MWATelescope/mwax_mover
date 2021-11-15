#!/usr/bin/env python
#
# Version handling for mwax_mover
#
import typing

"""Returns the major, minor and patch version of mwax_mover as a string"""


def get_mwax_mover_version_string() -> str:
    import pkg_resources  # part of setuptools
    package_version = pkg_resources.require("mwax_mover")[0].version
    return package_version


"""Returns a the major, minor and patch version of mwax_mover"""


def get_pmwax_mover_version_number() -> typing.Tuple[int, int, int]:
    version = get_mwax_mover_version_string()
    try:
        return int(version.split(".")[0]), int(version.split(".")[1]), int(version.split(".")[2])
    except Exception as e:
        raise Exception(
            f"Unabled to determine pymwalib version: Got {version} which could not be parsed. Error: {e}")
