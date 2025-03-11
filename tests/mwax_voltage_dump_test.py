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
from mwax_mover.utils import write_mock_subfile, read_subfile_trigger_value

TEST_BASE_PATH = "tests/mock_mwax_dump"
TEST_CONFIG_FILE1 = "tests/mwax_voltage_dump_test1.cfg"
TEST_CONFIG_FILE2 = "tests/mwax_voltage_dump_test2.cfg"
TEST_CONFIG_FILE3 = "tests/mwax_voltage_dump_test3.cfg"


def get_base_path() -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), TEST_BASE_PATH)


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"Creating {path}")
        os.mkdir(path)


def create_observation_subfiles(
    obs_id: int,
    subfile_count: int,
    mode: str,
    rec_chan: int,
    corr_chan: int,
    dev_shm_temp_dir: str,
    dev_shm_dir: str,
):
    """Creates some test subfiles for an obs"""
    sub_obs_id = obs_id
    offset = 0

    for _ in range(0, subfile_count):
        tmp_subfile_filename = os.path.join(
            dev_shm_temp_dir,
            f"{obs_id}_{sub_obs_id}_{rec_chan}.$$$",
        )

        # Write new subfile to dev_shm_tmp
        write_mock_subfile(
            tmp_subfile_filename,
            obs_id,
            sub_obs_id,
            mode,
            offset,
            rec_chan,
            corr_chan,
        )

        # Now rename to real subfile for processing
        # This is what subfile processor triggers on (RENAME)
        subfile_filename = os.path.join(
            dev_shm_dir,
            f"{obs_id}_{sub_obs_id}_{rec_chan}.sub",
        )
        os.rename(tmp_subfile_filename, subfile_filename)

        # simulate gap between subobs
        time.sleep(2)

        # Increment subobsid and offset
        sub_obs_id += 8
        offset += 8


def setup_test_dirs():
    """Gets the test dirs ready"""
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path()

    # Remove the path first
    if os.path.exists(base_dir):
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
    visdata_dont_archive_path = os.path.join(base_dir, "vis_dont_archive")
    check_and_make_dir(visdata_dont_archive_path)

    # voltdata_dont_archive_path
    voltdata_dont_archive_path = os.path.join(base_dir, "volt_dont_archive")
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

    packet_stats_dump_dir = os.path.join(base_dir, "packet_stats_dump_dir")
    check_and_make_dir(packet_stats_dump_dir)

    packet_stats_destination_dir = os.path.join(base_dir, "packet_stats_destination_dir")
    check_and_make_dir(packet_stats_destination_dir)


def test_voltage_dump_test_params():
    """This tests that sending 0 and 0 for the start and end
    results in a 200 (OK) response from the webservice"""
    # Setup all the paths
    setup_test_dirs()

    # Start mwax_subfile_distributor using our test config
    msd = MWAXSubfileDistributor()

    # Override the hostname
    msd.hostname = "test_server"

    # Call to read config
    msd.initialise(TEST_CONFIG_FILE1)

    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=msd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # Trigger a dump
    start = 0
    end = 0
    trigger_id = 123
    response = requests.get(f"http://127.0.0.1:9997/dump_voltages?start={start}&end={end}&trigger_id={trigger_id}")

    assert response.status_code == 200
    assert response.text == "OK"

    # Quit
    # Ok time's up! Stop the processor
    msd.signal_handler(signal.SIGINT, 0)


def test_voltage_dump_in_the_past():
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
    msd.initialise(TEST_CONFIG_FILE2)

    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=msd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # create mock subfiles
    # 1300000000 - 1300000024 are NO_CAPTURE
    create_observation_subfiles(1300000000, 4, "NO_CAPTURE", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # 1300000032 - is MWAX_VCS
    create_observation_subfiles(1300000032, 1, "MWAX_VCS", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # Trigger a dump which ends in the past (i.e. does not encroach on new obs)
    start = 1300000000
    sub_obs_to_dump = 4
    end = start + (sub_obs_to_dump * 8)
    trigger_id = 123
    response = requests.get(f"http://127.0.0.1:9998/dump_voltages?start={start}&end={end}&trigger_id={trigger_id}")
    msd.logger.debug(response.text)

    # Wait for processing
    time.sleep(3)

    # Now send through a MWAX_VCS and then some NO_CAPTURES
    # To test that:
    # When we hit the MWAX_VCS we do not do any archiving of free files
    # When we hit the NO_CAPTUREs we DO archive free files (oldest first)
    # 1300000040 - 1300000048 is MWAX_VCS
    create_observation_subfiles(1300000040, 2, "MWAX_VCS", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # 1300000056 - 1300000080 is NO_CAPTURE
    create_observation_subfiles(1300000056, 4, "NO_CAPTURE", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # Ok time's up! Stop the processor
    msd.signal_handler(signal.SIGINT, 0)
    while thrd.is_alive():
        # keep waiting
        time.sleep(1)

    #
    # Now check output
    #

    # Check dev/shm- all the keep files should now be back as .free
    dev_shm_sub_files = glob.glob(os.path.join(dev_shm_dir, "*.sub"))
    dev_shm_free_files = glob.glob(os.path.join(dev_shm_dir, "*.free"))

    assert len(dev_shm_sub_files) == 0
    assert len(dev_shm_free_files) == 11

    # Check destination
    # Should be 2 .sub files 1300000016 and 1300000024 which were "kept"
    # (1300000000 and 1300000008 are ignored because we keep 2 free files)
    # Plus one "real" VCS subfile 1300000032
    # Plus two "real" VCS subfiles 1300000080 and 1300000088
    destination_dir = os.path.join(get_base_path(), "volt_dont_archive")
    destination_sub_files = glob.glob(os.path.join(destination_dir, "*.sub"))
    destination_free_files = glob.glob(os.path.join(destination_dir, "*.free"))

    assert os.path.exists(os.path.join(destination_dir, "1300000000_1300000016_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000000_1300000024_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000032_1300000032_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000040_1300000040_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000040_1300000048_169.sub"))

    assert len(destination_sub_files) == 2 + 1 + 2
    assert len(destination_free_files) == 0

    # check that each dumped subfile contains the trigger!

    # 2 Dumped files
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000000_1300000016_169.sub"),
        )
        == 123
    )
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000000_1300000024_169.sub"),
        )
        == 123
    )

    # 3 subsequent files
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000032_1300000032_169.sub"),
        )
        is None
    )
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000040_1300000040_169.sub"),
        )
        is None
    )
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000040_1300000048_169.sub"),
        )
        is None
    )


def test_voltage_dump_past_and_future():
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
    msd.initialise(TEST_CONFIG_FILE3)

    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=msd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # create mock subfiles
    # 1300000000 - 1300000024 are NO_CAPTURE
    create_observation_subfiles(1300000000, 4, "NO_CAPTURE", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # 1300000032 - is MWAX_VCS -  will not be dumped or assigned a trigger
    create_observation_subfiles(1300000032, 1, "MWAX_VCS", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # Trigger a dump which ends 1 subobs in the future
    start = 1300000000
    sub_obs_to_dump = 6
    end = start + (sub_obs_to_dump * 8)
    trigger_id = 123
    response = requests.get(f"http://127.0.0.1:9999/dump_voltages?start={start}&end={end}&trigger_id={trigger_id}")
    msd.logger.debug(response.text)

    # Wait for processing
    time.sleep(3)

    # Now send through a MWAX_VCS and then some NO_CAPTURES
    # To test that:
    # When we hit the MWAX_VCS we do not do any archiving of free files
    # When we hit the NO_CAPTUREs we DO archive free files (oldest first)
    # 1300000040 - 1300000048 is MWAX_VCS
    create_observation_subfiles(1300000040, 2, "MWAX_VCS", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # 1300000056 - 1300000080 is NO_CAPTURE
    create_observation_subfiles(1300000056, 4, "NO_CAPTURE", 169, 0, dev_shm_temp_dir, dev_shm_dir)

    # Ok time's up! Stop the processor
    msd.signal_handler(signal.SIGINT, 0)
    while thrd.is_alive():
        # keep waiting
        time.sleep(1)

    #
    # Now check output
    #

    # Check dev/shm- all the keep files should now be back as .free
    dev_shm_sub_files = glob.glob(os.path.join(dev_shm_dir, "*.sub"))
    dev_shm_free_files = glob.glob(os.path.join(dev_shm_dir, "*.free"))

    assert len(dev_shm_sub_files) == 0
    assert len(dev_shm_free_files) == 11

    # Check destination
    # Should be 2 .sub files 1300000016 and 1300000024 which were "kept"
    # (1300000000 and 1300000008 are ignored because we keep 2 free files)
    # Plus one "real" VCS subfile 1300000032
    # Plus two "real" VCS subfiles 1300000080 and 1300000088
    destination_dir = os.path.join(get_base_path(), "volt_dont_archive")
    destination_sub_files = glob.glob(os.path.join(destination_dir, "*.sub"))
    destination_free_files = glob.glob(os.path.join(destination_dir, "*.free"))

    # 2 Dumped files
    assert os.path.exists(os.path.join(destination_dir, "1300000000_1300000016_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000000_1300000024_169.sub"))

    # 3 subsequent files
    assert os.path.exists(os.path.join(destination_dir, "1300000032_1300000032_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000040_1300000040_169.sub"))
    assert os.path.exists(os.path.join(destination_dir, "1300000040_1300000048_169.sub"))

    assert len(destination_sub_files) == 2 + 1 + 2
    assert len(destination_free_files) == 0

    # check that each dumped subfile contains the trigger!
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000000_1300000016_169.sub"),
        )
        == 123
    )
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000000_1300000024_169.sub"),
        )
        == 123
    )

    # This is a VCS subobs, so it is already on it's way to be archived by
    # the time we saw the trigger, so this will NOT have the triggerid in
    # the header.
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000032_1300000032_169.sub"),
        )
        is None
    )

    # This will be tagged with the trigger
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000040_1300000040_169.sub"),
        )
        == 123
    )
    assert (
        read_subfile_trigger_value(
            os.path.join(destination_dir, "1300000040_1300000048_169.sub"),
        )
        is None
    )
