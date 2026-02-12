"""
This is to test if MWAXSubfileDistributor correctly handles a MWAX_BEAMFORMER observation.
"""

import os
import shutil
import threading
from mwax_mover.utils import MWAXSubfileDistirbutorMode
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from tests_common import setup_test_directories

TEST_CONFIG_FILE = "tests/data/test002/test002.cfg"
TEST_METAFITS = "tests/data/test002/1454343736_metafits.fits"

# VDIF
TEST_VDIF = [
    "tests/data/test002/1454343736_1454343736_ch109_beam00.vdif",
    "tests/data/test002/1454343736_1454343744_ch109_beam00.vdif",
    "tests/data/test002/1454343736_1454343736_ch109_beam01.vdif",
    "tests/data/test002/1454343736_1454343744_ch109_beam01.vdif",
]


def test_beamformer_archiver_vdif():
    #
    # This test will test how the mwax_archive_processor handles new vdif files
    #

    # Setup dirs
    setup_test_directories(__file__)

    # Create a subfile distributor
    sd = MWAXSubfileDistributor()
    sd.initialise(TEST_CONFIG_FILE, MWAXSubfileDistirbutorMode.BEAMFORMER)

    # setup test data
    metafits = os.path.join(sd.cfg_corr_metafits_path, os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, metafits)

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=sd.start, daemon=True)

    # Start the processor
    thrd.start()

    # Copy data in
