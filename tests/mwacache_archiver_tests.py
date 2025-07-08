"""
This is to test if MWACacheArchiveProcessor correctly reads the tonnes of
config correctly from a "mwacache_archiver" config file.
"""

import os
from mwax_mover.mwacache_archive_processor import MWACacheArchiveProcessor
from mwax_mover.utils import ArchiveLocation

TEST_BASE_PATH = "tests/mock_mwacache_archiver"


def get_base_path() -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), TEST_BASE_PATH)


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"{path} not found. Creating {path}")
        os.mkdir(path)


def setup_mwacache_archiver_test():
    """Gets the mwacache_archiver tests ready"""
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path()
    check_and_make_dir(base_dir)

    # log path
    log_path = os.path.join(base_dir, "logs")
    check_and_make_dir(log_path)


def test_mwacache_archiver_config_file():
    """Tests that MWACacheArchiver reads a config file ok"""
    # Setup all the paths
    setup_mwacache_archiver_test()

    # This will test mwax_subfile_distributor based
    # on mwacache_test.cfg
    base_dir = TEST_BASE_PATH

    # Start mwax_subfile_distributor using our test config
    mcap = MWACacheArchiveProcessor()

    # Override the hostname
    mcap.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwacache_archiver/mwacache_test.cfg"

    # Call to read config <-- this is what we're testing!
    mcap.initialise(config_filename)

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcap.log_path == os.path.join(base_dir, "logs")
    assert mcap.metafits_path == "tests/data/mwacache_vulcan_metafits"
    assert mcap.archive_to_location == ArchiveLocation.AcaciaIngest

    assert mcap.health_multicast_interface_name == "eth5"
    assert mcap.health_multicast_ip == "224.250.0.0"
    assert mcap.health_multicast_port == 8004
    assert mcap.health_multicast_hops == 1

    assert mcap.concurrent_archive_workers == 4
    assert mcap.archive_command_timeout_sec == 1800

    assert mcap.s3_profile == "gsleap"
    assert mcap.s3_ceph_endpoints == ["https://acacia.pawsey.org.au"]

    assert mcap.remote_metadatadb_host == "localhost"
    assert mcap.remote_metadatadb_db == "mwa"
    assert mcap.remote_metadatadb_port == 5432
    assert mcap.remote_metadatadb_user == "test"
    assert mcap.remote_metadatadb_pass == "test_pwd"

    assert mcap.mro_metadatadb_host == "dummy"
    assert mcap.mro_metadatadb_db == "dummy"
    assert mcap.mro_metadatadb_port == 5432
    assert mcap.mro_metadatadb_user == "dummy"
    assert mcap.mro_metadatadb_pass == "dummy"

    assert len(mcap.watch_dirs) == 1
    assert mcap.watch_dirs[0] == "tests/data/mwacache_test01"

    # test list of projects
    assert mcap.high_priority_correlator_projectids == ["D0006"]
    assert not mcap.high_priority_vcs_projectids
