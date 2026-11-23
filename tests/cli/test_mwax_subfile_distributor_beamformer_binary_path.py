"""Test that MWAXSubfileDistributor quits at start-up if the configured beamformer binary does not exist."""

import re

import pytest
from tests_common import render_test_config, setup_test_directories
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.cli.mwax_subfile_distributor import MWAXSubfileDistributor
from mwax_mover.constants import EXIT_FAILURE

TEST_CODE = "beamformer_vdif"
MISSING_BINARY_NAME = "no_such_mwax_beamformer"


def test_beamformer_binary_path_exists():
    """The path from the config file is read into cfg_bf_binary_path."""
    setup_test_directories(TEST_CODE)

    sd = MWAXSubfileDistributor()
    sd.initialise(render_test_config(TEST_CODE), FakeMWAXDBHandler())

    assert sd.cfg_bf_binary_path.endswith("mwax_beamformer")


def test_beamformer_binary_path_missing_quits():
    """A bf_binary_path that does not exist must stop the processor (not give a version of Unknown)."""
    setup_test_directories(TEST_CODE)

    cfg_filename = render_test_config(TEST_CODE)
    with open(cfg_filename, encoding="utf-8") as f:
        cfg_text = f.read()
    cfg_text, num_subs = re.subn(
        r"^bf_binary_path=.*$", f"bf_binary_path=/{MISSING_BINARY_NAME}", cfg_text, flags=re.MULTILINE
    )
    assert num_subs == 1
    with open(cfg_filename, "w", encoding="utf-8") as f:
        f.write(cfg_text)

    sd = MWAXSubfileDistributor()
    with pytest.raises(SystemExit) as exc_info:
        sd.initialise(cfg_filename, FakeMWAXDBHandler())

    assert exc_info.value.code == EXIT_FAILURE
