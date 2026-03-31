import logging
import os
from mwax_mover.mwax_calvin_controller import MWAXCalvinController
import tests_common
from tests_fakedb import FakeMWAXDBHandler

logger = logging.getLogger(__name__)


def test_mwax_calvin_controller():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = tests_common.setup_test_directories("test016")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinController()

    # Override the hostname
    mcal.hostname = "calvin99"

    # Determine config file location
    config_filename = "tests/data/test016/test016.cfg"

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # Call to read config <-- this is what we're testing!
    mcal.initialise(
        config_filename,
        fake_db_handler,
    )

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcal.log_path == os.path.join(base_dir, "logs"), (
        f"log path mismatch: {mcal.log_path} {os.path.join(base_dir, 'logs')}"
    )

    assert mcal.health_multicast_interface_name == "eth0"
    assert mcal.health_multicast_ip == "127.0.0.1"
    assert mcal.health_multicast_port == 8012
    assert mcal.health_multicast_hops == 1
