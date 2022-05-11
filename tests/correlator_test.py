#
# This is to test if MWAXSubfileDistributor correctly reads the tonnes of
# config correctly from a "correlator" config file.
#
import os
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor


def get_base_path() -> str:
    TEST_BASE_PATH = "tests/mock_mwax"
    return os.path.join(os.path.curdir, TEST_BASE_PATH)


def check_and_make_dir(path):
    if not os.path.exists(path):
        print(f"{path} not found. Creating {path}")
        os.mkdir(path)


def setup_correaltor_test():
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path()
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

    # dont_archive_path
    dont_archive_path = os.path.join(base_dir, "dont_archive")
    check_and_make_dir(dont_archive_path)

    # visdata_incoming_path
    visdata_incoming_path = os.path.join(base_dir, "visdata_incoming")
    check_and_make_dir(visdata_incoming_path)

    # visdata_processing_stats_path
    visdata_processing_stats_path = os.path.join(
        base_dir, "visdata_processing_stats"
    )
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


def test_correlator_config_file():
    # Setup all the paths
    setup_correaltor_test()

    # This will test mwax_subfile_distributor based
    # on correlator_test.cfg
    base_dir = get_base_path()

    # Change working dir to our mock dir
    print(f"Changing dir to {base_dir}")
    os.chdir(base_dir)

    # Start mwax_subfile_distributor using our test config
    msd = MWAXSubfileDistributor()

    # Override the hostname
    msd.hostname = "test_server"

    # Determine config file location
    config_filename = "../correlator_test.cfg"

    # Call to read config <-- this is what we're testing!
    msd.initialise(config_filename)

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert msd.cfg_log_path == "./logs"
    assert msd.cfg_webserver_port == "9999"
    assert msd.cfg_health_multicast_interface_name == "eth0"
    assert msd.cfg_health_multicast_ip == "224.234.0.0"
    assert msd.cfg_health_multicast_port == 8005
    assert msd.cfg_health_multicast_hops == 1
    assert msd.cfg_subfile_incoming_path == "./dev_shm"
    assert msd.cfg_voltdata_incoming_path == "./voltdata_incoming"
    assert msd.cfg_voltdata_outgoing_path == "./voltdata_outgoing"
    assert msd.cfg_dont_archive_path == "./dont_archive"
    assert msd.cfg_always_keep_subfiles == 0
    assert msd.cfg_archive_command_timeout_sec == 300
    assert msd.cfg_psrdada_timeout_sec == 32
    assert msd.cfg_copy_subfile_to_disk_timeout_sec == 120
    assert msd.cfg_archiving_enabled == 1

    # correlator section
    assert msd.cfg_corr_input_ringbuffer_key == "0x1234"
    assert msd.cfg_corr_visdata_incoming_path == "./visdata_incoming"
    assert (
        msd.cfg_corr_visdata_processing_stats_path
        == "./visdata_processing_stats"
    )
    assert msd.cfg_corr_visdata_outgoing_path == "./visdata_outgoing"
    assert (
        msd.cfg_corr_mwax_stats_executable
        == "../../../mwax_stats/target/release/mwax_stats"
    )

    assert msd.cfg_corr_mwax_stats_dump_dir == "./mwax_stats_dump"
    assert msd.cfg_corr_mwax_stats_timeout_sec == 600
    assert msd.cfg_corr_calibrator_outgoing_path == "./visdata_cal_outgoing"
    assert (
        msd.cfg_corr_calibrator_destination_host == "192.168.120.1://dest/path"
    )
    assert msd.cfg_corr_calibrator_destination_port == "1094"
    assert msd.cfg_corr_calibrator_destination_enabled == 1
    assert msd.cfg_corr_metafits_path == "./vulcan_metafits"

    # metadata db section
    assert msd.cfg_metadatadb_host == "dummy"
    assert msd.cfg_metadatadb_db is None
    assert msd.cfg_metadatadb_port is None
    assert msd.cfg_metadatadb_user is None
    assert msd.cfg_metadatadb_pass is None

    # test_server section
    assert (
        msd.cfg_corr_archive_destination_host
        == "host2.destination.com://dest/path"
    )
    assert msd.cfg_corr_archive_destination_port == "1094"
    assert msd.cfg_corr_archive_destination_enabled is True
    assert msd.cfg_corr_diskdb_numa_node == -1
    assert msd.cfg_corr_archive_command_numa_node == -1
