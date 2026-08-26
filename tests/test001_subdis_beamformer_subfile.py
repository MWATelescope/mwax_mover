"""
This is to test if MWAXSubfileDistributor correctly handles a MWAX_BEAMFORMER observation.
"""

import os
import shutil
import signal
import threading
import time

from tests_common import (
    create_observation_subfiles,
    data_path,
    obs_metafits_path,
    render_test_config,
    setup_test_directories,
)
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.cli.mwax_subfile_distributor import MWAXSubfileDistributor

TEST_METAFITS = obs_metafits_path(1454743816)
TEST_AOCAL_FILES = [data_path("1454343616", f"1454343616_256_0032_{c}_calfile.bin") for c in range(109, 109 + 24)]


def test_beamformer_subfile():
    #
    # This test checks how the mwax_subfile_processor handles a beamformer subfile
    #

    # Setup dirs
    base_dir = setup_test_directories("test001")

    # Create a subfile distributor
    sd = MWAXSubfileDistributor()

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    sd.initialise(render_test_config("test001"), fake_db_handler)

    # setup data
    metafits = os.path.join(sd.cfg_corr_metafits_path, os.path.basename(TEST_METAFITS))
    shutil.copyfile(TEST_METAFITS, metafits)

    create_observation_subfiles(
        1454743816,
        2,
        "MWAX_BEAMFORMER",
        109,
        0,
        os.path.join(base_dir, "tmp"),
        sd.cfg_subfile_incoming_path,
    )

    for c in TEST_AOCAL_FILES:
        shutil.copyfile(c, os.path.join(sd.cfg_bf_cal_path, os.path.basename(c)))

    # start processor
    # Create and start a thread for the processor
    thrd = threading.Thread(name="msd_thread", target=sd.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(20)

    # Quit
    # Ok time's up! Stop the processor
    sd.signal_handler(signal.SIGINT, 0)

    thrd.join(30)
