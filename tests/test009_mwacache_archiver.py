"""
This is to test if MWACacheArchiveProcessor correctly reads the tonnes of
config correctly from a "mwacache_archiver" config file.
"""

import os
import shutil
import signal
import threading
import time


from mwax_mover.mwacache_archive_processor import MWACacheArchiveProcessor
from mwax_mover.utils import ArchiveLocation
from tests_common import setup_test_directories
from tests_fakedb import FakeMWAXDBHandler


def test_mwacache_archiver_config_file():
    """Tests that MWACacheArchiver reads a config file ok"""
    # Setup all the paths
    setup_test_directories("test009")

    # Start mwax_subfile_distributor using our test config
    mcap = MWACacheArchiveProcessor()

    # Determine config file location
    config_filename = "tests/data/test009/test009.cfg"

    # Call to read config <-- this is what we're testing!
    mcap.initialise(config_filename)
    # Override db_handler with a fake one
    mcap.mro_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. mcap.mro_db_handler_object.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]
    mcap.remote_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. mcap.remote_db_handler_object.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcap.metafits_path == "/home/gsleap/mwax_mover_testing/test009/vulcan/metafits"
    assert mcap.archive_to_location == ArchiveLocation.AcaciaMWA

    assert mcap.health_multicast_interface_name == "eth0"
    assert mcap.health_multicast_ip == "224.250.0.0"
    assert mcap.health_multicast_port == 8004
    assert mcap.health_multicast_hops == 1

    assert mcap.concurrent_archive_workers == 4
    assert mcap.archive_command_timeout_sec == 1800

    assert mcap.s3_profile == "gsleap4"

    assert mcap.remote_metadatadb_host == "dummy"
    assert mcap.remote_metadatadb_db == "dummy"
    assert mcap.remote_metadatadb_port == 5432
    assert mcap.remote_metadatadb_user == "dummy"
    assert mcap.remote_metadatadb_pass == "dummy"

    assert mcap.mro_metadatadb_host == "dummy"
    assert mcap.mro_metadatadb_db == "dummy"
    assert mcap.mro_metadatadb_port == 5432
    assert mcap.mro_metadatadb_user == "dummy"
    assert mcap.mro_metadatadb_pass == "dummy"

    assert len(mcap.watch_dirs) == 3
    assert mcap.watch_dirs[0] == "/home/gsleap/mwax_mover_testing/test009/volume1/incoming"

    # test list of projects
    assert mcap.high_priority_correlator_projectids == ["D0006"]
    assert not mcap.high_priority_vcs_projectids


def test_mwacache_archiver_metafits_file():
    """Tests that MWACacheArchiver processes a file ok"""
    TEST_METAFITS = "tests/data/1122979144/1122979144_metafits.fits"

    # Setup all the paths
    base_dir = setup_test_directories("test009")

    # Start mwax_subfile_distributor using our test config
    mcap = MWACacheArchiveProcessor()

    # Determine config file location
    config_filename = "tests/data/test009/test009.cfg"

    # setup data
    incoming = os.path.join(os.path.join(base_dir, "volume1/incoming"), os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, incoming)

    # Override db_handler with a fake one
    fake_mro_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. mcap.mro_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]
    fake_remote_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    fake_remote_db_handler.select_results = [
        [{"observation_num": 1122979144, "size": 74880, "checksum": "428e7e38ca40ff9cb473e5d78a0f9879"}],
    ]

    # Call to read config <-- this is what we're testing!
    mcap.initialise(config_filename, fake_mro_db_handler, fake_remote_db_handler)

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
