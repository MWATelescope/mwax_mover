"""
This is to test if MWAXSubfileDistributor correctly reads the tonnes of
config correctly from a "correlator" config file.
"""

import os
import shutil
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor

TEST_BASE_PATH = "tests/mock_mwax"
TEST_CONFIG_FILE = "tests/mwax_subfile_distributor_correlator_test.cfg"


def get_base_path() -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), TEST_BASE_PATH)


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"{path} not found. Creating {path}")
        os.mkdir(path)


def setup_test_dirs():
    """Gets the test dirs ready"""
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path()

    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    check_and_make_dir(base_dir)

    # log path
    log_path = os.path.join(base_dir, "logs")
    check_and_make_dir(log_path)

    # subfile_incoming_path
    subfile_incoming_path = os.path.join(base_dir, "dev_shm")
    check_and_make_dir(subfile_incoming_path)

    # voltdata_incoming_path
    voltdata_incoming_path = os.path.join(base_dir, "voltdata_incoming")
    check_and_make_dir(voltdata_incoming_path)

    # voltdata_outgoing_path
    voltdata_outgoing_path = os.path.join(base_dir, "voltdata_outgoing")
    check_and_make_dir(voltdata_outgoing_path)

    # visdata_dont_archive_path
    visdata_dont_archive_path = os.path.join(base_dir, "dont_archive")
    check_and_make_dir(visdata_dont_archive_path)

    # visdata_dont_archive_path
    voltdata_dont_archive_path = os.path.join(base_dir, "dont_archive")
    check_and_make_dir(voltdata_dont_archive_path)

    # visdata_incoming_path
    visdata_incoming_path = os.path.join(base_dir, "visdata_incoming")
    check_and_make_dir(visdata_incoming_path)

    # visdata_processing_stats_path
    visdata_processing_stats_path = os.path.join(base_dir, "visdata_processing_stats")
    check_and_make_dir(visdata_processing_stats_path)

    # visdata_outgoing_path
    visdata_outgoing_path = os.path.join(base_dir, "visdata_outgoing")
    check_and_make_dir(visdata_outgoing_path)

    # mwax_stats_dump_dir
    mwax_stats_dump_dir = os.path.join(base_dir, "mwax_stats_dump")
    check_and_make_dir(mwax_stats_dump_dir)

    # calibrator_outgoing_path
    calibrator_outgoing_path = os.path.join(base_dir, "visdata_cal_outgoing")
    check_and_make_dir(calibrator_outgoing_path)

    # metafits_path
    metafits_path = os.path.join(base_dir, "vulcan_metafits")
    check_and_make_dir(metafits_path)

    packet_stats_dump_dir = os.path.join("packet_stats_dump_dir")
    check_and_make_dir(packet_stats_dump_dir)


def test_correlator_config_file():
    """Tests that SubfileDistributor reads a correlator config file ok"""
    # Setup all the paths
    setup_test_dirs()

    # This will test mwax_subfile_distributor based
    # on correlator_test.cfg
    base_dir = TEST_BASE_PATH

    # Start mwax_subfile_distributor using our test config
    msd = MWAXSubfileDistributor()

    # Override the hostname
    msd.hostname = "test_server"

    # Call to read config <-- this is what we're testing!
    msd.initialise(TEST_CONFIG_FILE)

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert msd.cfg_log_path == os.path.join(base_dir, "logs")
    assert msd.cfg_webserver_port == 7999
    assert msd.cfg_health_multicast_interface_name == "eth0"
    assert msd.cfg_health_multicast_ip == "224.234.0.0"
    assert msd.cfg_health_multicast_port == 8005
    assert msd.cfg_health_multicast_hops == 1
    assert msd.cfg_subfile_incoming_path == os.path.join(base_dir, "dev_shm")
    assert msd.cfg_voltdata_incoming_path == os.path.join(base_dir, "voltdata_incoming")
    assert msd.cfg_voltdata_outgoing_path == os.path.join(base_dir, "voltdata_outgoing")
    assert msd.cfg_voltdata_dont_archive_path == os.path.join(base_dir, "dont_archive")
    assert msd.cfg_always_keep_subfiles == 1
    assert msd.cfg_archive_command_timeout_sec == 300
    assert msd.cfg_psrdada_timeout_sec == 32
    assert msd.cfg_copy_subfile_to_disk_timeout_sec == 120
    assert msd.cfg_archiving_enabled == 1

    # correlator section
    assert msd.cfg_corr_input_ringbuffer_key == "0x1234"
    assert msd.cfg_corr_visdata_incoming_path == os.path.join(base_dir, "visdata_incoming")
    assert msd.cfg_corr_visdata_dont_archive_path == os.path.join(base_dir, "dont_archive")
    assert msd.cfg_corr_visdata_processing_stats_path == os.path.join(base_dir, "visdata_processing_stats")
    assert msd.cfg_corr_visdata_outgoing_path == os.path.join(base_dir, "visdata_outgoing")
    assert msd.cfg_corr_mwax_stats_executable == "../mwax_stats/target/release/mwax_stats"

    assert msd.cfg_corr_mwax_stats_dump_dir == os.path.join(base_dir, "mwax_stats_dump")
    assert msd.cfg_corr_mwax_stats_timeout_sec == 600
    assert msd.cfg_corr_calibrator_outgoing_path == os.path.join(base_dir, "visdata_cal_outgoing")
    assert msd.cfg_corr_calibrator_destination_host == "192.168.120.1://dest/path"
    assert msd.cfg_corr_calibrator_destination_port == 1094
    assert msd.cfg_corr_calibrator_destination_enabled == 1
    assert msd.cfg_corr_metafits_path == os.path.join(base_dir, "vulcan_metafits")
    assert not msd.cfg_corr_high_priority_correlator_projectids
    assert msd.cfg_corr_high_priority_vcs_projectids == ["D0006", "G0058"]

    # metadata db section
    assert msd.cfg_metadatadb_host == "dummy"
    assert msd.cfg_metadatadb_db == "dummy"
    assert msd.cfg_metadatadb_port == 5432
    assert msd.cfg_metadatadb_user == "dummy"
    assert msd.cfg_metadatadb_pass == "dummy"

    # test_server section
    assert msd.cfg_corr_archive_destination_host == "host2.destination.com://dest/path"
    assert msd.cfg_corr_archive_destination_port == 1094
    assert msd.cfg_corr_archive_destination_enabled is True
    assert msd.cfg_corr_diskdb_numa_node == -1
    assert msd.cfg_corr_archive_command_numa_node == -1
