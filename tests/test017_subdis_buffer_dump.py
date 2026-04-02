"""
This is to test if MWAXSubfileDistributor does a buffer dump.
"""

import glob

import requests

import os
import shutil
import signal
import threading
import time

from mwax_mover.utils import MWAXSubfileDistirbutorMode, running_under_pytest
from tests_common import create_observation_subfiles, setup_test_directories
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from tests_fakedb import FakeMWAXDBHandler

TEST_CONFIG_FILE = "tests/data/test017/test017.cfg"


def test_running_under_pytest():
    assert running_under_pytest()


def test_corr_buffer_dump():
    obsid = 1369821496
    obs_exp = 48

    dump_start = obsid
    dump_end = dump_start + 32
    trigger_id = 123

    file_dict = do_buffer_dump(MWAXSubfileDistirbutorMode.CORRELATOR, obsid, obs_exp, dump_start, dump_end, trigger_id)

    expected_dumped_files = int(((dump_end - dump_start) + 8) / 8)

    assert len(file_dict["voltdata_dont_archive_sub_files"]) == expected_dumped_files


def do_buffer_dump(
    config_mode: MWAXSubfileDistirbutorMode,
    obs_id: int,
    obs_exp_time: int,
    dump_start: int,
    dump_end: int,
    dump_trigger_id: int,
) -> dict:
    """Helper function to create test scenarios for voltage buffer dumps.

    1. Setup test dirs (standard)
    2. Creates a SubfileDistributor and uses the config file to configure it
    3. We ensure the metafits file for the passed in obsid is in the correct place
    4. We start SubfileDistributor
    5. Now we loop through from obsid to obsid+obs_exp_time and create new test subfiles.
    7. At the end of the loops, we call the dump_voltages web service
    7. Then we stop SubfileDistributor
    8. We then collect all the files and return their names for the caller to analyse to see if the test succeeded.

    Args:
        config_mode (MWAXSubfileDistirbutorMode): The config mode SubfileDistributor is in (CORRELATOR=normal; BEAMFORMER=beamformer)
        obs_id (int): An obsid to use, which we have the metafits for in test_data/OBSID/OBSID_metafits.fits.
        obs_exp_time (int): How long is the observation? The length determines how many subfiles we create for the test (and how long the test goes for).
        dump_start (int): The gps time we pass to dump_voltages that specifies the first subobs to dump.
        dump_end (int): The gps time we pass to dump_voltages that specifies the last subobs to dump.
        dump_trigger_id (int): The id of the trigger so we can inject it into the subfile headers of the dumped files

    Returns:
        dict: {
        "dev_shm_mwax_free_files": filenames of all the .free files /dev/shm/mwax,
        "dev_shm_mwax_keep_files": filenames of all the .keep files /dev/shm/mwax,
        "dev_shm_mwax_sub_files": filenames of all the .sub files /dev/shm/mwax,
        "voltdata_dont_archive_sub_files": filenames of all the .sub files /voltdata/dont_archive)
        }

    Raises:

    """

    # Setup all the paths
    base_dir = setup_test_directories("test017")

    # Start mwax_subfile_distributor using our test config
    sd = MWAXSubfileDistributor()

    # Call to read config <-- this is what we're testing!
    sd.initialise(TEST_CONFIG_FILE, config_mode)

    # Override db_handler with a fake one
    sd.db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. sd.db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # setup data
    src_metafits = f"tests/data/{obs_id}/{obs_id}_metafits.fits"
    metafits = os.path.join(sd.cfg_corr_metafits_path, os.path.basename(src_metafits))
    shutil.copyfile(src_metafits, metafits)

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=sd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(15)

    expected_subfiles = int(obs_exp_time / 8)

    # throw some subfiles in!
    create_observation_subfiles(
        obs_id,
        expected_subfiles,
        "MWAX_CORRELATOR",
        109,
        0,
        os.path.join(base_dir, "tmp"),
        sd.cfg_subfile_incoming_path,
        1,
    )

    assert len(glob.glob(os.path.join(sd.cfg_subfile_incoming_path, "*.sub"))) == expected_subfiles

    print("Dump triggered!")
    # Now do a buffer dump!
    dump_success = call_dump_voltages(sd.cfg_webserver_port, dump_start, dump_end, dump_trigger_id)

    assert dump_success

    # allow things to process
    time.sleep(8)

    # We now need to wait a bit for all files to be moved to /voltdata/incoming
    keep_files_left = 99

    while keep_files_left > 0:
        keep_files_left = len(glob.glob(f"{sd.cfg_subfile_incoming_path}/*.keep"))
        time.sleep(3)

    # Ok just give a bit more leeway
    time.sleep(5)

    # Quit
    # Ok time's up! Stop the processor
    sd.signal_handler(signal.SIGINT, 0)

    time.sleep(20)

    # Test!
    # We should have ((dump_end - dump_start)+ 8) / 8 dumped files, but specific tests are left to the caller
    # Let's just find all the files and let the caller sort through it!
    return {
        "dev_shm_mwax_free_files": glob.glob(f"{sd.cfg_subfile_incoming_path}/*.free"),
        "dev_shm_mwax_keep_files": glob.glob(f"{sd.cfg_subfile_incoming_path}/*.keep"),
        "dev_shm_mwax_sub_files": glob.glob(f"{sd.cfg_subfile_incoming_path}/*.sub"),
        "voltdata_dont_archive_sub_files": glob.glob(f"{sd.cfg_voltdata_incoming_path}/*.sub"),
    }


def call_dump_voltages(web_port: int, start: int, end: int, trigger_id: int) -> bool:
    url = f"http://localhost:{web_port}/dump_voltages"
    params = {
        "start": start,
        "end": end,
        "trigger_id": trigger_id,
    }
    response = requests.get(url, params=params)
    return response.status_code == 200
