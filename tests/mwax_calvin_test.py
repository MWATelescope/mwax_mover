"""
This is to test if mwax_calvin_processor correctly reads the tonnes of
config correctly from a "mwacache_archiver" config file.
"""
import os
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor

TEST_BASE_PATH = "tests/mock_mwax_calvin"


def get_base_path() -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), TEST_BASE_PATH)


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"{path} not found. Creating {path}")
        os.mkdir(path)


def setup_mwax_calvin_test():
    """Gets the mwax_calvin tests ready"""
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path()
    check_and_make_dir(base_dir)

    # log path
    log_path = os.path.join(base_dir, "logs")
    check_and_make_dir(log_path)

    # watch path
    watch_path = os.path.join(base_dir, "watch")
    check_and_make_dir(watch_path)

    # work path
    work_path = os.path.join(base_dir, "work")
    check_and_make_dir(work_path)


def test_mwax_calvin_config_file():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    setup_mwax_calvin_test()

    # This will test mwax_subfile_distributor based
    # on mwacache_test.cfg
    base_dir = TEST_BASE_PATH

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwax_calvin_test.cfg"

    # Call to read config <-- this is what we're testing!
    mcal.initialise(config_filename)

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcal.log_path == os.path.join(base_dir, "logs")

    assert mcal.health_multicast_interface_name == "eth0"
    assert mcal.health_multicast_ip == "224.250.0.0"
    assert mcal.health_multicast_port == 8009
    assert mcal.health_multicast_hops == 1

    assert mcal.watch_path == os.path.join(base_dir, "watch")
    assert mcal.work_path == os.path.join(base_dir, "work")
