"""
This is to test if MWAXSubfileDistributor correctly reads the tonnes of
config correctly from a "beamformer" config file.
"""
import os
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor

TEST_BASE_PATH = "tests/mock_mwax_bf"
TEST_CONFIG_FILE = "tests/mwax_subfile_distributor_beamformer_test.cfg"
TEST_BEAMFORMER_SETTINGS_FILE = "tests/beamformer_settings.txt"


def get_base_path() -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), TEST_BASE_PATH)


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"{path} not found. Creating {path}")
        os.mkdir(path)


def setup_beamformer_test():
    """Gets the correlator tests ready"""
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

    # visdata_dont_archive_path
    visdata_dont_archive_path = os.path.join(base_dir, "visdata_dont_archive")
    check_and_make_dir(visdata_dont_archive_path)

    # visdata_dont_archive_path
    voltdata_dont_archive_path = os.path.join(
        base_dir, "voltdata_dont_archive"
    )
    check_and_make_dir(voltdata_dont_archive_path)

    # fil_outgoing_path
    fildata_path = os.path.join(base_dir, "fildata_path")
    check_and_make_dir(fildata_path)


def test_beamformer_config_file():
    """Tests that SubfileDistributor reads a beamformer config file ok"""
    # Setup all the paths
    setup_beamformer_test()

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
    assert msd.cfg_webserver_port == "9998"
    assert msd.cfg_health_multicast_interface_name == "eth0"
    assert msd.cfg_health_multicast_ip == "224.234.0.0"
    assert msd.cfg_health_multicast_port == 8666
    assert msd.cfg_health_multicast_hops == 1
    assert msd.cfg_subfile_incoming_path == os.path.join(base_dir, "dev_shm")
    assert msd.cfg_voltdata_incoming_path == os.path.join(
        base_dir, "voltdata_incoming"
    )
    assert msd.cfg_voltdata_outgoing_path == os.path.join(
        base_dir, "voltdata_outgoing"
    )
    assert msd.cfg_visdata_dont_archive_path == os.path.join(
        base_dir, "visdata_dont_archive"
    )
    assert msd.cfg_voltdata_dont_archive_path == os.path.join(
        base_dir, "voltdata_dont_archive"
    )
    assert msd.cfg_always_keep_subfiles == 0
    assert msd.cfg_archive_command_timeout_sec == 300
    assert msd.cfg_psrdada_timeout_sec == 32
    assert msd.cfg_copy_subfile_to_disk_timeout_sec == 120
    assert msd.cfg_archiving_enabled == 0
    assert not msd.cfg_high_priority_correlator_projectids
    assert msd.cfg_high_priority_vcs_projectids == ["D0006", "G0058"]

    # beamformer section
    assert msd.cfg_bf_ringbuffer_key == "0x1234"
    assert msd.cfg_bf_fildata_path == os.path.join(base_dir, "fildata_path")
    assert msd.cfg_bf_settings_path == TEST_BEAMFORMER_SETTINGS_FILE

    # test_server section
    assert (
        msd.cfg_bf_archive_destination_host
        == "host2.destination.com://dest/path"
    )
    assert msd.cfg_bf_archive_destination_port == "1094"
    assert (
        msd.cfg_bf_archive_destination_enabled is False
    )  # this is due to archiving enabled=0
    assert msd.cfg_bf_numa_node == -1
    assert msd.cfg_bf_archive_command_numa_node == -1
