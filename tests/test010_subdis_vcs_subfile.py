"""
This is to test if MWAXSubfileDistributor correctly reads the tonnes of
config correctly from a "correlator" config file.
"""

import os
import shutil
import signal
import threading
import time

from mwax_mover.utils import MWAXSubfileDistirbutorMode
from tests_common import create_observation_subfiles, setup_test_directories
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from tests_fakedb import FakeMWAXDBHandler

TEST_CONFIG_FILE = "tests/data/test010/test010.cfg"
TEST_METAFITS = "tests/data/1369821496/1369821496_metafits.fits"


def test_correlator_config_file():
    """Tests that SubfileDistributor reads a correlator config file ok"""
    # Setup all the paths
    base_dir = setup_test_directories("test010")

    # Start mwax_subfile_distributor using our test config
    sd = MWAXSubfileDistributor()

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # Call to read config <-- this is what we're testing!
    sd.initialise(TEST_CONFIG_FILE, MWAXSubfileDistirbutorMode.CORRELATOR, fake_db_handler)

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert sd.cfg_webserver_port == 9999
    assert sd.cfg_health_multicast_interface_name == "eth0"
    assert sd.cfg_health_multicast_ip == "224.234.0.0"
    assert sd.cfg_health_multicast_port == 8005
    assert sd.cfg_health_multicast_hops == 1
    assert sd.cfg_subfile_incoming_path == os.path.join(base_dir, "dev/shm/mwax")
    assert sd.cfg_voltdata_incoming_path == os.path.join(base_dir, "voltdata/incoming")
    assert sd.cfg_voltdata_outgoing_path == os.path.join(base_dir, "voltdata/outgoing")
    assert sd.cfg_voltdata_dont_archive_path == os.path.join(base_dir, "voltdata/dont_archive")
    assert sd.cfg_always_keep_subfiles == 0
    assert sd.cfg_archive_command_timeout_sec == 300
    assert sd.cfg_psrdada_timeout_sec == 32
    assert sd.cfg_copy_subfile_to_disk_timeout_sec == 120
    assert sd.cfg_master_archiving_enabled == 0

    # correlator section
    assert sd.cfg_corr_input_ringbuffer_key == "0x1234"
    assert sd.cfg_corr_visdata_incoming_path == os.path.join(base_dir, "visdata/incoming")
    assert sd.cfg_corr_visdata_dont_archive_path == os.path.join(base_dir, "visdata/dont_archive")
    assert sd.cfg_corr_visdata_processing_stats_path == os.path.join(base_dir, "visdata/processing_stats")
    assert sd.cfg_corr_visdata_outgoing_path == os.path.join(base_dir, "visdata/outgoing")
    assert sd.cfg_corr_mwax_stats_binary_dir == "../mwax_stats/target/release"

    assert sd.cfg_corr_mwax_stats_dump_dir == os.path.join(base_dir, "vulcan/mwax_stats_dump")
    assert sd.cfg_corr_mwax_stats_timeout_sec == 600
    assert sd.cfg_corr_calibrator_outgoing_path == os.path.join(base_dir, "visdata/cal_outgoing")
    assert sd.cfg_corr_metafits_path == os.path.join(base_dir, "vulcan/metafits")
    assert sd.cfg_corr_high_priority_correlator_projectids == ["D0006"]
    assert not sd.cfg_corr_high_priority_vcs_projectids

    # metadata db section
    assert sd.cfg_metadatadb_host == "dummy"
    assert sd.cfg_metadatadb_db == "dummy"
    assert sd.cfg_metadatadb_port == 5432
    assert sd.cfg_metadatadb_user == "dummy"
    assert sd.cfg_metadatadb_pass == "dummy"

    # test_server section
    assert sd.cfg_corr_archive_destination_host == "host1.destination.com://dest/path"
    assert sd.cfg_corr_archive_destination_port == 1094
    assert sd.cfg_corr_archive_destination_enabled is False
    assert sd.cfg_corr_diskdb_numa_node == -1
    assert sd.cfg_corr_archive_command_numa_node == -1


def test_process_vcs_subfile():
    # Setup all the paths
    base_dir = setup_test_directories("test010")

    # Start mwax_subfile_distributor using our test config
    sd = MWAXSubfileDistributor()

    # Call to read config <-- this is what we're testing!
    sd.initialise(TEST_CONFIG_FILE, MWAXSubfileDistirbutorMode.CORRELATOR)
    # Override db_handler with a fake one
    sd.db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. sd.db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # setup data
    metafits = os.path.join(sd.cfg_corr_metafits_path, os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, metafits)

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=sd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(15)

    # throw some subfiles in!
    create_observation_subfiles(
        1369821496, 3, "MWAX_VCS", 109, 0, os.path.join(base_dir, "tmp"), sd.cfg_subfile_incoming_path
    )

    # allow things to process
    time.sleep(8)

    # Quit
    # Ok time's up! Stop the processor
    sd.signal_handler(signal.SIGINT, 0)
