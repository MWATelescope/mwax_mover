"""
This is to test if mwax_calvin_processor correctly reads the tonnes of
config correctly from a "mwacache_archiver" config file.
"""
import glob
import threading
import os
import shutil
import signal
import time
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor
from mwax_mover.utils import get_hostname

TEST_BASE_PATH = "tests/mock_mwax_calvin"

#
# For testing, I have chosen a very small contiguous 24 file observation.
# It is expected that to run the tests here you will need to have
# downloaded this observation (as a tar) and then extracted it
# to the TEST_OBS_ID_LOCATION (feel free to change)
#
TEST_OBS_ID = 1339580448
TEST_PICKETFENCE_OBS_ID = 1361707216

hostname = get_hostname()

if hostname == "calvin1" or hostname == "calvin2":
    # Special case for the real machines
    TEST_OBS_LOCATION = f"/data/test_data/{TEST_OBS_ID}"
    TEST_PICKETFENCE_OBS_LOCATION = (
        f"/data/test_data/{TEST_PICKETFENCE_OBS_ID}"
    )
else:
    TEST_OBS_LOCATION = f"/data/{TEST_OBS_ID}"
    TEST_PICKETFENCE_OBS_LOCATION = f"/data/{TEST_PICKETFENCE_OBS_ID}"


def get_base_path(test_name: str) -> str:
    """Utility function to get the base path for these tests"""
    return f"{TEST_BASE_PATH}_{test_name}"


def get_full_base_path(test_name: str) -> str:
    """Utility function to get the base path for these tests"""
    return os.path.join(os.getcwd(), get_base_path(test_name))


def check_and_make_dir(path):
    """If dir does not exist, make it"""
    if not os.path.exists(path):
        print(f"{path} not found. Creating {path}")
        os.mkdir(path)


def setup_mwax_calvin_test(test_name: str) -> str:
    """Gets the mwax_calvin tests ready and returns the test base path"""
    # Setup dirs first!
    # Make the base dir
    base_dir = get_base_path(test_name)

    # Remove the path first
    if os.path.exists(base_dir):
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

    # upload path
    upload_path = os.path.join(base_dir, "upload")
    check_and_make_dir(upload_path)

    # complete path
    complete_path = os.path.join(base_dir, "complete")
    check_and_make_dir(complete_path)

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

    return base_dir


def test_mwax_calvin_test01():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = setup_mwax_calvin_test("test01")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwax_calvin_test01.cfg"

    # Call to read config <-- this is what we're testing!
    mcal.initialise(config_filename)

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcal.log_path == os.path.join(
        base_dir, "logs"
    ), f"log path mismatch: {mcal.log_path} {os.path.join(base_dir, 'logs')}"

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
    assert mcal.complete_path == os.path.join(base_dir, "complete")
    assert mcal.upload_path == os.path.join(base_dir, "upload")
    assert mcal.keep_completed_visibility_files == 1


def test_mwax_calvin_test02():
    """Tests that mwax_calvin does a normal
    simple contigous pipeline run ok"""
    mwax_calvin_normal_pipeline_run(False)


def test_mwax_calvin_test03():
    """Tests that mwax_calvin does a normal
    non-contigous pipeline (picket fence) run ok"""
    mwax_calvin_normal_pipeline_run(True)


def mwax_calvin_normal_pipeline_run(picket_fence: bool):
    """Creates a CalvinProcessor copies files in
    and allows the process to run normally.
    Param: picket_fence: bool, is there so we
    can run the code twice- once for picket fence
    and once for contiguous without copy and pasting"""
    test_obs_location = (
        TEST_PICKETFENCE_OBS_LOCATION if picket_fence else TEST_OBS_LOCATION
    )
    test_obs_id = TEST_PICKETFENCE_OBS_ID if picket_fence else TEST_OBS_ID

    # Setup all the paths
    if picket_fence:
        _base_dir = setup_mwax_calvin_test("test03")
        # Determine config file location
        config_filename = "tests/mwax_calvin_test03.cfg"
    else:
        _base_dir = setup_mwax_calvin_test("test02")
        # Determine config file location
        config_filename = "tests/mwax_calvin_test02.cfg"

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

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
        os.path.join(test_obs_location, f"{test_obs_id}_*_ch*.fits")
    )

    for _file_number, filename in enumerate(incoming_files):
        dest_filename = os.path.join(
            mcal.incoming_watch_path, os.path.basename(filename)
        )

        shutil.copyfile(filename, dest_filename)

    # Wait for processing (very dependent on hardware!)
    if picket_fence:
        time.sleep(110)
    else:
        time.sleep(90)

    # Quit
    # Ok time's up! Stop the processor
    mcal.signal_handler(signal.SIGINT, 0)
    thrd.join()

    # Assembly
    assemble_files = glob.glob(
        os.path.join(mcal.assemble_path, f"{test_obs_id}/{test_obs_id}*.fits")
    )
    assert len(assemble_files) == 0, "number of assembled files is 0"

    # processing path should have been removed
    assert (
        os.path.exists(os.path.join(mcal.processing_path, f"{test_obs_id}"))
        is False
    ), "processing path still exists"

    # processing complete
    # Files are left here because we successfully completed
    # there was no error as such
    processing_complete_files = glob.glob(
        os.path.join(mcal.complete_path, f"{test_obs_id}/{test_obs_id}*.fits")
    )

    # non picket fence = 24 gpu + 1 metafits + 2 solution fits
    # picket fence = 24 gpu + 1 metafits + 1 solution fits
    expected_processing_complete_files = 27 if picket_fence else 26

    assert len(processing_complete_files) == expected_processing_complete_files

    # metafits plus the gpubox files plus solution fits

    # also look for uvfits output from birli
    birli_files = glob.glob(
        os.path.join(
            mcal.complete_path,
            f"{test_obs_id}/{test_obs_id}*.uvfits",
        )
    )

    expected_birli_files = 2 if picket_fence else 1

    assert (
        len(birli_files) == expected_birli_files
    ), "Number of uvfits files found != expected uvfits files"

    assert os.path.exists(
        os.path.join(
            mcal.complete_path,
            f"{test_obs_id}/{test_obs_id}_birli_readme.txt",
        )
    ), "test_obs_id_birli_readme.txt not found"

    # look for hyperdrive readme files
    hyperdrive_readme_files = glob.glob(
        os.path.join(
            mcal.complete_path,
            f"{test_obs_id}/{test_obs_id}*_hyperdrive_readme.txt",
        )
    )

    expected_hyperdrive_readme_files = 2 if picket_fence else 1
    assert (
        len(hyperdrive_readme_files) == expected_hyperdrive_readme_files
    ), "correct number of readme hyperdrive file not found"

    # look for solutions
    hyperdrive_solution_files = glob.glob(
        os.path.join(
            mcal.complete_path,
            f"{test_obs_id}/{test_obs_id}*_solutions.fits",
        )
    )

    expected_hyperdrive_solution_files = 2 if picket_fence else 1

    assert (
        len(hyperdrive_solution_files) == expected_hyperdrive_solution_files
    ), "correct number of hyperdrive solutions files not found"

    bin_solution_files = glob.glob(
        os.path.join(mcal.complete_path, f"{test_obs_id}/{test_obs_id}*.bin")
    )
    # expected bin files should == expected solution files
    assert (
        len(bin_solution_files) == expected_hyperdrive_solution_files
    ), "correct number of bin solution files not found"

    # look for stats.txt
    stats_files = glob.glob(
        os.path.join(
            mcal.complete_path,
            f"{test_obs_id}/{test_obs_id}*_stats.txt",
        )
    )

    expected_hyperdrive_stats_files = 2 if picket_fence else 1

    assert (
        len(stats_files) == expected_hyperdrive_stats_files
    ), "correct number of stats files not found"

    # processing errors
    processing_error_files = glob.glob(
        os.path.join(
            mcal.processing_error_path, f"{test_obs_id}/{test_obs_id}*.fits"
        )
    )
    assert len(processing_error_files) == 0, "processing_error_files is not 0"


def test_mwax_calvin_test04():
    """Tests that mwax_calvin does a normal
    simple pipeline run but hyperdrive times out"""
    # Setup all the paths
    _base_path = setup_mwax_calvin_test("test04")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwax_calvin_test04.cfg"

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

    # Wait for processing
    time.sleep(20)

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
