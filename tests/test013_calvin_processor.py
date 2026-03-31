import logging
import os
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor, CalvinJobType
import tests_common
from tests_fakedb import FakeMWAXDBHandler

logger = logging.getLogger(__name__)


def test_mwax_calvin_processor():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = tests_common.setup_test_directories("test013")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()
    mcal.produce_debug_plots = False

    # Override the hostname
    mcal.hostname = "calvin99"

    # Determine config file location
    config_filename = "tests/data/test013/test013.cfg"

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # Call to read config <-- this is what we're testing!
    mcal.initialise(
        config_filename,
        1234567890,
        1234,
        CalvinJobType.realtime,
        "https//test.test",
        [
            123,
        ],
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
    assert mcal.health_multicast_port == 8011
    assert mcal.health_multicast_hops == 1

    assert mcal.job_input_path == os.path.join(base_dir, "data/calvin/in_jobs")
    assert mcal.job_output_path == os.path.join(base_dir, "data/calvin/out_jobs")
    assert mcal.source_list_filename == "../srclists/srclist_pumav3_EoR0aegean_fixedEoR1pietro+ForA_phase1+2.txt"
    assert mcal.source_list_type == "rts"
    assert mcal.hyperdrive_binary_path == "../mwa_hyperdrive/target/release/hyperdrive"
    assert mcal.hyperdrive_timeout == 7200
    assert mcal.birli_binary_path == "../Birli/target/release/birli"
    assert mcal.birli_timeout == 3600
    assert mcal.keep_completed_visibility_files == 0
    assert mcal.cal_export_max_age_hours == 24
