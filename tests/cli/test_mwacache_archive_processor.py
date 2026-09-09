"""
This is to test if MWACacheArchiveProcessor correctly reads the tonnes of
config correctly from a "mwacache_archiver" config file.
"""

import os
import shutil
import signal
import threading
import time

from tests_common import obs_metafits_path, render_test_config, setup_test_directories
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.cli.mwacache_archive_processor import MWACacheArchiveProcessor
from mwax_mover.filesystem.naming import ArchiveLocation


def test_mwacache_archiver_config_file():
    """Tests that MWACacheArchiver reads a config file ok"""
    # Setup all the paths
    base_dir = setup_test_directories("mwacache_archive_processor")

    # Start mwax_subfile_distributor using our test config
    mcap = MWACacheArchiveProcessor()

    # Determine config file location
    config_filename = render_test_config("mwacache_archive_processor")

    # Call to read config <-- this is what we're testing!
    mcap.initialise(config_filename)
    # Override db_handler with a fake one
    mcap.db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. mcap.db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcap.cfg_metafits_path == os.path.join(base_dir, "vulcan/metafits")
    assert mcap.archive_to_location == ArchiveLocation.AcaciaMWA

    assert mcap.cfg_health_multicast_interface_name == "lo"
    assert mcap.cfg_health_multicast_ip == "224.250.0.0"
    assert mcap.cfg_health_multicast_port == 8004
    assert mcap.cfg_health_multicast_hops == 1

    assert mcap.cfg_concurrent_archive_workers == 4
    assert mcap.cfg_archive_command_timeout_sec == 1800
    assert mcap.cfg_rclone_check_wait_secs == 60

    assert mcap.s3_profile == "test_profile"

    assert mcap.cfg_db_host == "dummy"
    assert mcap.cfg_db_name == "dummy"
    assert mcap.cfg_db_port == 5432
    assert mcap.cfg_db_user == "dummy"
    assert mcap.cfg_db_pass == "dummy"

    assert len(mcap.watch_dirs) == 3
    assert mcap.watch_dirs[0] == os.path.join(base_dir, "volume1/incoming")

    # test list of projects
    assert mcap.cfg_high_priority_correlator_projectids == ["D0006"]
    assert not mcap.cfg_high_priority_vcs_projectids


def test_mwacache_archiver_metafits_file():
    """Tests that MWACacheArchiver processes a file ok"""
    TEST_METAFITS = obs_metafits_path(1122979144)

    # Setup all the paths
    base_dir = setup_test_directories("mwacache_archive_processor")

    # Start mwax_subfile_distributor using our test config
    mcap = MWACacheArchiveProcessor()

    # Determine config file location
    config_filename = render_test_config("mwacache_archive_processor")

    # setup data
    incoming = os.path.join(os.path.join(base_dir, "volume1/incoming"), os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, incoming)

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    fake_db_handler.select_results = [
        [
            {
                "observation_num": 1122979144,
                "size": 74880,
                "checksum": "428e7e38ca40ff9cb473e5d78a0f9879",
            }
        ],
    ]

    # Call to read config <-- this is what we're testing!
    mcap.initialise(config_filename, fake_db_handler)

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="mcap_thread", target=mcap.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(20)

    # Quit
    # Ok time's up! Stop the processor
    mcap.signal_handler(signal.SIGINT, 0)

    thrd.join(30)
