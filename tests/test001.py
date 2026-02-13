"""
This is to test if MWAXSubfileDistributor correctly handles a MWAX_BEAMFORMER observation.
"""

import os
import shutil
import signal
import threading
import time
from mwax_mover.utils import MWAXSubfileDistirbutorMode
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from tests_common import setup_test_directories, create_observation_subfiles

TEST_CONFIG_FILE = "tests/data/test001/test001.cfg"
TEST_METAFITS = "tests/data/test001/1454743816_metafits.fits"
TEST_AOCAL_FILES = [f"tests/data/test001/1454343616_256_32_{c}_calfile.bin" for c in range(109, 109 + 24)]


def test_beamformer_subfile():
    #
    # This test checks how the mwax_subfile_processor handles a beamformer subfile
    #

    # Setup dirs
    base_dir = setup_test_directories(__file__)

    # Create a subfile distributor
    sd = MWAXSubfileDistributor()
    sd.initialise(TEST_CONFIG_FILE, MWAXSubfileDistirbutorMode.BEAMFORMER)

    # setup data
    metafits = os.path.join(sd.cfg_corr_metafits_path, os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, metafits)

    create_observation_subfiles(
        1454743816, 2, "MWAX_BEAMFORMER", 109, 0, os.path.join(base_dir, "tmp"), sd.cfg_subfile_incoming_path
    )

    for c in TEST_AOCAL_FILES:
        shutil.copyfile(c, os.path.join(sd.cfg_bf_aocal_path, os.path.basename(c)))

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=sd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(10)

    # Quit
    # Ok time's up! Stop the processor
    sd.signal_handler(signal.SIGINT, 0)

    # Check the redis server for a message
