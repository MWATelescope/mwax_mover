"""
This is to test if MWAXSubfileDistributor correctly handles a MWAX_BEAMFORMER observation.
"""

import os
import shutil
from mwax_mover.utils import MWAXSubfileDistirbutorMode
from mwax_mover.mwax_subfile_distributor import MWAXSubfileDistributor
from tests_common import setup_test_directories

TEST_CONFIG_FILE = "tests/data/test001/test001.cfg"
TEST_SUBFILE = "tests/data/test001/1454743816_1454743816_55.sub"
TEST_METAFITS = "tests/data/test001/1454743816_metafits.fits"


def test_beamformer_subfile():
    # Setup dirs
    setup_test_directories(__file__)

    # Create a subfile distributor
    sd = MWAXSubfileDistributor()
    sd.initialise(TEST_CONFIG_FILE, MWAXSubfileDistirbutorMode.BEAMFORMER)

    # Copy the test subfile into the correct test dir
    subfile_name = os.path.join(sd.cfg_subfile_incoming_path, os.path.basename(TEST_SUBFILE))

    # Initiate processing
    shutil.copyfile(TEST_SUBFILE, subfile_name)

    # start processor
    sd.start()

    #
