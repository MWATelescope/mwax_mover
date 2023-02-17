"""
This is to test if mwax_calvin_processor correctly reads the tonnes of
config correctly from a "mwacache_archiver" config file.
"""
import glob
import threading
import os
import random
import shutil
import signal
import time
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor

TEST_BASE_PATH = "tests/mock_mwax_calvin"

#
# For testing, I have chosen a very small 24 file observation.
# It is expected that to run the tests here you will need to have
# downloaded this observation (as a tar) and then extracted it
# to the TEST_OBS_ID_LOCATION (feel free to change)
#
TEST_OBS_ID = 1339580448
TEST_OBS_LOCATION = f"/data/{TEST_OBS_ID}"


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

    # Remove the path first
    shutil.rmtree(base_dir)

    check_and_make_dir(base_dir)

    # log path
    log_path = os.path.join(base_dir, "logs")
    check_and_make_dir(log_path)

    # watch path
    watch_path = os.path.join(base_dir, "watch")
    check_and_make_dir(watch_path)

    # assemble path
    assemble_path = os.path.join(base_dir, "assemble")
    check_and_make_dir(assemble_path)

    # processing path
    processing_path = os.path.join(base_dir, "processing")
    check_and_make_dir(processing_path)

    # processing error path
    processing_error_path = os.path.join(base_dir, "processing_errors")
    check_and_make_dir(processing_error_path)

    # processing complete path
    processing_error_path = os.path.join(base_dir, "processing_complete")
    check_and_make_dir(processing_error_path)

    # Now check that the TEST_OBS files exist
    assert os.path.exists(TEST_OBS_LOCATION), (
        "The module level var TEST_OBS_LOCATION is set to"
        f" {TEST_OBS_LOCATION}, but that does not appear to exist. Please"
        f" download the {TEST_OBS_ID} observation from MWA ASVO and extract"
        " the files to this directory before doing more tests for"
        " mwax_calvin_processor."
    )

    # And check we have all the files
    assert (
        len(
            glob.glob(
                os.path.join(TEST_OBS_LOCATION, f"{TEST_OBS_ID}_*_ch*.fits")
            )
        )
        == 24
    ), (
        "There seem to be files missing in"
        f" {TEST_OBS_LOCATION} for {TEST_OBS_ID} observation. Please"
        f" download the {TEST_OBS_ID} observation from MWA ASVO and extract"
        " the files to this directory before doing more tests for"
        " mwax_calvin_processor."
    )


def test_mwax_calvin_config_file():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    setup_mwax_calvin_test()

    # This will test mwax_calvin_processor based
    # on mwax_calvin_test.cfg
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

    assert mcal.incoming_watch_path == os.path.join(base_dir, "watch")
    assert mcal.assemble_path == os.path.join(base_dir, "assemble")
    assert mcal.assemble_check_seconds == 10

    assert mcal.processing_path == os.path.join(base_dir, "processing")
    assert mcal.processing_error_path == os.path.join(
        base_dir, "processing_errors"
    )
    assert (
        mcal.source_list_filename
        == "../srclists/srclist_pumav3_EoR0aegean_fixedEoR1pietro+ForA_phase1+2.txt"
    )
    assert mcal.source_list_type == "rts"
    assert (
        mcal.hyperdrive_binary_path
        == "../mwa_hyperdrive/target/release/hyperdrive"
    )
    assert mcal.hyperdrive_timeout == 7200
    assert mcal.birli_binary_path == "../Birli/target/release/birli"
    assert mcal.birli_timeout == 3600
    assert mcal.processing_complete_path == os.path.join(
        base_dir, "processing_complete"
    )
    assert mcal.keep_completed_visibility_files == 1


def test_mwax_calvin_normal_pipeline_run():
    """Tests that mwax_calvin does a normal
    simple pipeline run ok"""
    # Setup all the paths
    setup_mwax_calvin_test()

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwax_calvin_test.cfg"

    # Call to read config <-- this is what we're testing!
    mcal.initialise(config_filename)

    # Start the pipeline
    # Create and start a thread for the processor
    thrd = threading.Thread(name="mcal_thread", target=mcal.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # Now we simulate TEST_OBS files being delivered into the watch dir
    incoming_files = glob.glob(
        os.path.join(TEST_OBS_LOCATION, f"{TEST_OBS_ID}_*_ch*.fits")
    )

    for filename in incoming_files:
        dest_filename = os.path.join(
            mcal.incoming_watch_path, os.path.basename(filename)
        )

        shutil.copyfile(filename, dest_filename)
        # delay by up to 1 sec
        time.sleep(random.random() / 2.0)

    # Wait for processing (up to 5 mins for birli and hyperdrive)
    time.sleep(60 * 3)

    # Quit
    # Ok time's up! Stop the processor
    mcal.signal_handler(signal.SIGINT, 0)
    thrd.join()

    # Assembly
    assemble_files = glob.glob(
        os.path.join(mcal.assemble_path, f"{TEST_OBS_ID}/{TEST_OBS_ID}*.fits")
    )
    assert len(assemble_files) == 0

    # processing path should have been removed
    assert (
        os.path.exists(os.path.join(mcal.processing_path, f"{TEST_OBS_ID}"))
        is False
    )

    # processing complete
    # Files are left here because we successfully completed
    # there was no error as such
    processing_complete_files = glob.glob(
        os.path.join(
            mcal.processing_complete_path, f"{TEST_OBS_ID}/{TEST_OBS_ID}*.fits"
        )
    )
    assert (
        len(processing_complete_files) == 26
    )  # metafits plus the gpubox files plus solution fits

    # also look for uvfits output from birli
    birli_files = glob.glob(
        os.path.join(
            mcal.processing_complete_path,
            f"{TEST_OBS_ID}/{TEST_OBS_ID}*.uvfits",
        )
    )
    assert len(birli_files) == 1, f"{TEST_OBS_ID}*.uvfits not found"

    assert os.path.exists(
        os.path.join(
            mcal.processing_complete_path, f"{TEST_OBS_ID}/readme_birli.txt"
        )
    ), "readme_birli.txt not found"

    assert os.path.exists(
        os.path.join(
            mcal.processing_complete_path,
            f"{TEST_OBS_ID}/readme_hyperdrive.txt",
        )
    ), "readme_hyperdrive.txt not found"

    # look for solutions
    hyperdrive_solution_files = glob.glob(
        os.path.join(
            mcal.processing_complete_path,
            f"{TEST_OBS_ID}/{TEST_OBS_ID}_solutions.fits",
        )
    )
    assert len(hyperdrive_solution_files) == 1

    bin_solution_files = glob.glob(
        os.path.join(
            mcal.processing_complete_path, f"{TEST_OBS_ID}/{TEST_OBS_ID}.bin"
        )
    )
    assert len(bin_solution_files) == 1

    # look for stats.txt
    assert os.path.exists(
        os.path.join(mcal.processing_complete_path, f"{TEST_OBS_ID}/stats.txt")
    )

    # processing errors
    processing_error_files = glob.glob(
        os.path.join(
            mcal.processing_error_path, f"{TEST_OBS_ID}/{TEST_OBS_ID}*.fits"
        )
    )
    assert len(processing_error_files) == 0


def test_mwax_calvin_hyperdrive_timeout():
    """Tests that mwax_calvin does a normal
    simple pipeline run but hyperdrive times out"""
    # Setup all the paths
    setup_mwax_calvin_test()

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwax_calvin_test.cfg"

    # Call to read config <-- this is what we're testing!
    mcal.initialise(config_filename)
    mcal.hyperdrive_timeout = 5

    # Start the pipeline
    # Create and start a thread for the processor
    thrd = threading.Thread(name="mcal_thread", target=mcal.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # Now we simulate TEST_OBS files being delivered into the watch dir
    incoming_files = glob.glob(
        os.path.join(TEST_OBS_LOCATION, f"{TEST_OBS_ID}_*_ch*.fits")
    )

    for filename in incoming_files:
        dest_filename = os.path.join(
            mcal.incoming_watch_path, os.path.basename(filename)
        )

        shutil.copyfile(filename, dest_filename)
        # delay by up to 1 sec
        time.sleep(random.random() / 2.0)

    # Wait for processing
    time.sleep(30)

    # Quit
    # Ok time's up! Stop the processor
    mcal.signal_handler(signal.SIGINT, 0)
    thrd.join()

    # Now check results

    # Assembly
    assemble_files = glob.glob(
        os.path.join(mcal.assemble_path, f"{TEST_OBS_ID}/{TEST_OBS_ID}*.fits")
    )
    assert len(assemble_files) == 0

    # processing
    processing_files = glob.glob(
        os.path.join(
            mcal.processing_path, f"{TEST_OBS_ID}/{TEST_OBS_ID}*.fits"
        )
    )
    assert len(processing_files) == 0

    # processing errors
    # hyperdrive timed out so it is an error so we file it away
    # into the processing errors dir
    processing_error_files = glob.glob(
        os.path.join(
            mcal.processing_error_path,
            f"{TEST_OBS_ID}/{TEST_OBS_ID}*.fits",
        )
    )
    assert len(processing_error_files) == 25  # metafits plus the gpubox files
