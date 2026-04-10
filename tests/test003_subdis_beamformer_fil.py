"""
This is to test if MWAXSubfileDistributor correctly handles a MWAX_BEAMFORMER observation, stitching Filterbank files.
"""

import os
import shutil
import signal
import threading
import time

from mwax_mover.utils import MWAXSubfileDistirbutorMode
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from tests_common import setup_test_directories
from tests_fakedb import FakeMWAXDBHandler

TEST_CONFIG_FILE = "tests/data/test003/test003.cfg"
TEST_METAFITS = "tests/data/1451758560/1451758560_metafits.fits"

# Filterbank
TEST_FIL = [
    "tests/data/1451758560/1451758560_1451758560_ch109_beam00",
    "tests/data/1451758560/1451758560_1451758568_ch109_beam00",
    "tests/data/1451758560/1451758560_1451758560_ch109_beam01",
    "tests/data/1451758560/1451758560_1451758568_ch109_beam01",
]


def test_beamformer_archiver_fil():
    #
    # This test will test how the mwax_archive_processor handles new fil files
    #
    # Setup dirs
    setup_test_directories("test003")

    # Create a subfile distributor
    sd = MWAXSubfileDistributor()

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    sd.initialise(TEST_CONFIG_FILE, MWAXSubfileDistirbutorMode.BEAMFORMER, fake_db_handler)
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. sd.db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # setup test data (metafits file and cal files)
    metafits = os.path.join(sd.cfg_corr_metafits_path, os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, metafits)

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=sd.start, daemon=True)

    # Copy VDIF data in (we are simulating the beamformer dumping in files as tmp then renaming)
    print("Beamformer files incoming!")
    for v in TEST_FIL:
        # Create the fil file
        shutil.copyfile(v + ".fil", os.path.join(sd.cfg_bf_incoming_path, os.path.basename(v) + ".fil"))

    #
    # NOTE! wsl does not support iNotify, so the only way to test is to put the test files in the incoming dirs
    # BEFORE we start the processor as there is a glob.glob call that scans for and processes existing files.
    #

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(20)

    # Quit
    # Ok time's up! Stop the processor
    sd.signal_handler(signal.SIGINT, 0)

    thrd.join(30)
