"""
This is to test if MWAXSubfileDistributor correctly reads the tonnes of
config correctly from a "correlator" config file.
"""
import glob
import os
import shutil
import signal
import threading
import time
import requests
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from mwax_mover.utils import write_mock_subfile

TEST_BASE_PATH = "tests/mock_mwax_dump"
TEST_CONFIG_FILE = "tests/mwax_voltage_dump_test.cfg"


def get_base_path() -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), TEST_BASE_PATH)


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"Creating {path}")
        os.mkdir(path)


def setup_test_dirs():
    """Gets the test dirs ready"""
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path()

    # Remove the path first
    shutil.rmtree(base_dir)

    # Now create it
    check_and_make_dir(base_dir)

    # log path
    log_path = os.path.join(base_dir, "logs")
    check_and_make_dir(log_path)

    # subfile_incoming_path
    subfile_incoming_path = os.path.join(base_dir, "dev_shm")
    check_and_make_dir(subfile_incoming_path)

    # not in config file, but this is used by tests to simulate
    # u2s creating subfiles and then doing an mv into /dev/shm
    # so that subfile_processor can detect the rename
    dev_shm_temp_path = os.path.join(base_dir, "dev_shm_temp")
    check_and_make_dir(dev_shm_temp_path)

    # voltdata_incoming_path
    voltdata_incoming_path = os.path.join(base_dir, "voltdata_incoming")
    check_and_make_dir(voltdata_incoming_path)

    # voltdata_outgoing_path
    voltdata_outgoing_path = os.path.join(base_dir, "voltdata_outgoing")
    check_and_make_dir(voltdata_outgoing_path)

    # visdata_dont_archive_path
    visdata_dont_archive_path = os.path.join(base_dir, "dont_archive")
    check_and_make_dir(visdata_dont_archive_path)

    # voltdata_dont_archive_path
    voltdata_dont_archive_path = os.path.join(base_dir, "dont_archive")
    check_and_make_dir(voltdata_dont_archive_path)

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


def test_voltage_dump():
    """Tests that SubfileDistributor correctly processes a voltage dump"""
    # Setup all the paths
    setup_test_dirs()

    dev_shm_dir = os.path.join(get_base_path(), "dev_shm")
    dev_shm_temp_dir = os.path.join(get_base_path(), "dev_shm_temp")

    # Start mwax_subfile_distributor using our test config
    msd = MWAXSubfileDistributor()

    # Override the hostname
    msd.hostname = "test_server"

    # Call to read config
    msd.initialise(TEST_CONFIG_FILE)

    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=msd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # create mock subfiles
    subfile_count = 5
    start_subobs_id = 1300000000
    for subfile_number in range(0, subfile_count):
        # Basic params of the subfiles
        chan = 169
        obs_id = start_subobs_id
        sub_obs_id = start_subobs_id + (subfile_number * 8)
        tmp_subfile_filename = os.path.join(
            dev_shm_temp_dir,
            f"{obs_id}_{sub_obs_id}_{chan}.$$$",
        )
        offset = subfile_number * 8

        # Write new subfile to dev_shm_tmp
        write_mock_subfile(
            tmp_subfile_filename, obs_id, sub_obs_id, "MWAX_VCS", offset
        )

        # Now rename to real subfile for processing
        # This is what subfile processor triggers on (RENAME)
        subfile_filename = os.path.join(
            dev_shm_dir,
            f"{obs_id}_{sub_obs_id}_{chan}.sub",
        )
        os.rename(tmp_subfile_filename, subfile_filename)

        # simulate gap between subobs
        time.sleep(1)

    # Trigger a dump
    start = start_subobs_id
    sub_obs_to_dump = 2
    end = start_subobs_id + (sub_obs_to_dump * 8)
    response = requests.get(
        f"http://127.0.0.1:9997/dump_voltages?start={start}&end={end}"
    )
    msd.logger.debug(response.text)

    timeout = 10
    time_started = time.time()

    # Wait for processing to finish or timeout
    while time.time() - time_started < timeout:
        # keep running
        time.sleep(0.1)

    # Ok time's up! Stop the processor
    msd.signal_handler(signal.SIGINT, 0)
    while thrd.is_alive():
        # keep waiting
        time.sleep(0.1)

    #
    # Now check output
    #

    # Check dev/shm
    dev_shm_sub_files = glob.glob(os.path.join(dev_shm_dir, "*.sub"))
    dev_shm_free_files = glob.glob(os.path.join(dev_shm_dir, "*.free"))

    assert len(dev_shm_sub_files) == 0
    assert len(dev_shm_free_files) == subfile_count

    # Check destination
    destination_dir = os.path.join(get_base_path(), "dont_archive")
    destination_sub_files = glob.glob(os.path.join(destination_dir, "*.sub"))
    destination_free_files = glob.glob(os.path.join(destination_dir, "*.free"))

    assert len(destination_sub_files) == subfile_count
    assert len(destination_free_files) == 0

    # Check voltage dump destination
    dump_destination_dir = os.path.join(get_base_path(), "voltdata_incoming")
    dump_destination_sub_files = glob.glob(
        os.path.join(dump_destination_dir, "*.sub")
    )

    assert len(dump_destination_sub_files) == sub_obs_to_dump
