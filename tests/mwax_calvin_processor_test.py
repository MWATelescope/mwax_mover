import logging
import os
import shutil
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor
from mwax_mover.mwax_db import MWAXDBHandler
import tests_common
import pytest
import glob
import threading
import signal
import time
from mwax_mover.utils import get_hostname


@pytest.fixture
def setup_calvin_processor_tables():
    logger = logging.getLogger(__name__)

    # Configure a db handler
    db_handler = MWAXDBHandler(logger, "localhost", 5432, "mwax_mover_test", "postgres", "postgres")
    db_handler.start_database_pool()

    # Setup database tables
    tests_common.run_create_test_db_object_script(db_handler, "tests/mwax_calvin_processor.sql")

    return db_handler


def get_datetime_dir_name(path) -> str:
    # Given a passed in path, return the datetime path created
    # by calvin_processor.

    # List all files and dirs under path
    dirs = os.listdir(path)

    for d in dirs:
        if not os.path.isdir(os.path.join(path, d)):
            # Remove any non-dirs
            dirs.remove(d)

    # We should be left with one directory
    assert len(dirs) == 1

    # Return the dir
    return os.path.join(path, dirs[0])


def upload_handler_helper(setup_calvin_processor_tables, base_dir: str, obsid: int):
    # Base dir should be for example "tests/data/solution_handler_no_low_picket"
    # Obsid is the obsid being tested

    processing_dir = os.path.join(base_dir, str(obsid))

    # Set up MWAXCalvinProcessor instance
    processor = MWAXCalvinProcessor()
    # Configure a db handler
    processor.db_handler_object = setup_calvin_processor_tables

    processor.complete_path = os.path.join(base_dir, "complete")

    if not os.path.exists(processor.complete_path):
        os.mkdir(processor.complete_path)

    # Call upload_handler method with test path from mock_mwax_calvin_test03
    success = processor.upload_handler(processing_dir)
    # Assert that the success flag is True
    assert success is True
    # We need to move the files back from processing_dir/complete/date_time/. to processing_dir
    if not os.path.exists(processing_dir):
        complete_path = os.path.join(processor.complete_path, str(obsid))

        # Determine the dir name- it will be a date!
        dirs = os.listdir(complete_path)

        for d in dirs:
            if not os.path.isdir(os.path.join(complete_path, d)):
                dirs.remove(d)

        assert len(dirs) == 1
        dated_dir = os.path.join(complete_path, dirs[0])

        # It got moved! Move it back
        shutil.move(dated_dir, processing_dir)


def test_upload_handler_no_low_picket(setup_calvin_processor_tables):
    upload_handler_helper(
        setup_calvin_processor_tables,
        "tests/data/solution_handler_no_low_picket",
        1361707216,
    )


def test_upload_handler_lightning(setup_calvin_processor_tables):
    upload_handler_helper(setup_calvin_processor_tables, "tests/data/solution_handler_lightning", 1365977896)


def test_upload_handler_compact_hyda_picket(setup_calvin_processor_tables):
    upload_handler_helper(setup_calvin_processor_tables, "tests/data/solution_handler_hyda_picket3", 1369821496)


def test_upload_handler_1094488624_50(setup_calvin_processor_tables):
    upload_handler_helper(
        setup_calvin_processor_tables, "tests/data/solution_handler_1094488624.50-Tile073", 1094488624
    )


#
# Testing of the processor in full
#
TEST_BASE_PATH = "tests/mock_mwax_calvin"

hostname = get_hostname()

#
# For testing, I have chosen a very small contiguous 24 file observations.
#
TEST_CONTIG_OBS_ID = 1184928224  # CenA
TEST_PICKETFENCE_OBS_ID = 1119683928  # HydA, 24 pickets


def check_test_data_exists(obs_id: int):
    #
    # Check if the data exists, if not user will need to download it or skip test!
    #
    test_obs_location = "tests/data"

    data_location = os.path.join(test_obs_location, str(obs_id))

    error_message = (
        f"Missing test data! Please use MWA ASVO to download the visbilities of obs_id {obs_id} into {data_location}"
    )
    assert os.path.exists(data_location), error_message + f" Directory {data_location} does not exist."

    # Check we have the right number of files
    count_of_data_files = len(glob.glob(os.path.join(data_location, f"{obs_id}_*_gpubox*_00.fits")))
    assert count_of_data_files == 24, (
        error_message + f" Only found {count_of_data_files} gpubox files when there should have been 24."
    )


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
    # Check test data exists
    check_test_data_exists(TEST_CONTIG_OBS_ID)
    check_test_data_exists(TEST_PICKETFENCE_OBS_ID)

    # Setup dirs
    # Make the base dir
    base_dir = get_base_path(test_name)

    # Remove the path first
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    check_and_make_dir(base_dir)

    # log path
    log_path = os.path.join(base_dir, "logs")
    check_and_make_dir(log_path)

    # watch paths
    watch_path = os.path.join(base_dir, "watch_realtime")
    check_and_make_dir(watch_path)
    watch_path = os.path.join(base_dir, "watch_asvo")
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

    # upload_errors path
    upload_errors_path = os.path.join(base_dir, "upload_errors")
    check_and_make_dir(upload_errors_path)

    # complete path
    complete_path = os.path.join(base_dir, "complete")
    check_and_make_dir(complete_path)

    return base_dir


def test_mwax_calvin_test01(setup_calvin_processor_tables):
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = setup_mwax_calvin_test("test01")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()
    mcal.produce_debug_plots = False

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
    assert mcal.health_multicast_ip == "127.0.0.1"
    assert mcal.health_multicast_port == 8801
    assert mcal.health_multicast_hops == 1

    assert mcal.incoming_realtime_watch_path == os.path.join(base_dir, "watch_realtime")
    assert mcal.remove_partial_files_check_seconds == 3600
    assert mcal.incoming_asvo_watch_path == os.path.join(base_dir, "watch_asvo")
    assert mcal.assemble_path == os.path.join(base_dir, "assemble")
    assert mcal.assemble_check_seconds == 10

    assert mcal.job_output_path == os.path.join(base_dir, "processing")
    assert mcal.processing_error_path == os.path.join(base_dir, "processing_errors")
    assert mcal.source_list_filename == "../srclists/srclist_pumav3_EoR0aegean_fixedEoR1pietro+ForA_phase1+2.txt"
    assert mcal.source_list_type == "rts"
    assert mcal.hyperdrive_binary_path == "../mwa_hyperdrive/target/release/hyperdrive"
    assert mcal.hyperdrive_timeout == 7200
    assert mcal.birli_binary_path == "../Birli/target/release/birli"
    assert mcal.birli_timeout == 3600
    assert mcal.complete_path == os.path.join(base_dir, "complete")
    assert mcal.upload_path == os.path.join(base_dir, "upload")
    assert mcal.upload_error_path == os.path.join(base_dir, "upload_errors")
    assert mcal.keep_completed_visibility_files == 0


def test_mwax_calvin_test02(setup_calvin_processor_tables):
    """Tests that mwax_calvin does a normal
    simple contigous pipeline run ok"""
    mwax_calvin_normal_pipeline_run(False)


def test_mwax_calvin_test03(setup_calvin_processor_tables):
    """Tests that mwax_calvin does a normal
    non-contigous pipeline (picket fence) run ok"""
    mwax_calvin_normal_pipeline_run(True)


def mwax_calvin_normal_pipeline_run(picket_fence: bool):
    """Creates a CalvinProcessor copies files in
    and allows the process to run normally.
    Param: picket_fence: bool, is there so we
    can run the code twice- once for picket fence
    and once for contiguous without copy and pasting"""
    test_obs_id = TEST_PICKETFENCE_OBS_ID if picket_fence else TEST_CONTIG_OBS_ID
    test_obs_location = f"tests/data/{test_obs_id}"

    # Setup all the paths
    if picket_fence:
        # Determine config file location
        setup_mwax_calvin_test("test03")
        config_filename = "tests/mwax_calvin_test03.cfg"
    else:
        # Determine config file location
        setup_mwax_calvin_test("test02")
        config_filename = "tests/mwax_calvin_test02.cfg"

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Call to read config <-- this is what we're testing!
    mcal.initialise(config_filename)
    mcal.produce_debug_plots = False
    mcal.logger.level = logging.INFO

    # Start the pipeline
    # Create and start a thread for the processor
    thrd = threading.Thread(name="mcal_thread", target=mcal.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # Now we simulate TEST_OBS files being delivered into the realtime watch dir
    incoming_files = glob.glob(os.path.join(test_obs_location, f"{test_obs_id}_*_gpubox*_00.fits"))

    for _file_number, filename in enumerate(incoming_files):
        dest_filename = os.path.join(mcal.incoming_realtime_watch_path, os.path.basename(filename))

        shutil.copyfile(filename, dest_filename)

    # Wait for processing (very dependent on hardware!)
    if picket_fence:
        time.sleep(200)
    else:
        time.sleep(180)

    # Quit
    # Ok time's up! Stop the processor
    mcal.signal_handler(signal.SIGINT, 0)
    thrd.join()

    # Assembly
    assemble_files = glob.glob(os.path.join(mcal.assemble_path, f"{test_obs_id}/{test_obs_id}*.fits"))
    assert len(assemble_files) == 0, "number of assembled files is 0"

    # processing path should have been removed
    assert os.path.exists(os.path.join(mcal.job_output_path, f"{test_obs_id}")) is False, "processing path still exists"

    # processing complete

    # Determine the final complete path- it will be mcal.complete_path/obsid/DATETIME/
    complete_path = get_datetime_dir_name(os.path.join(mcal.complete_path, f"{test_obs_id}"))
    assert os.path.exists(complete_path)

    # Files are left here because we successfully completed
    # there was no error as such
    processing_complete_files = glob.glob(os.path.join(complete_path, f"{test_obs_id}*.fits"))

    # non picket fence = 24 gpu + 1 metafits + 1 solution fits
    # picket fence = 24 gpu + 1 metafits + 12 solution fits
    expected_processing_complete_files = 37 if picket_fence else 26

    assert len(processing_complete_files) == expected_processing_complete_files

    # metafits plus the gpubox files plus solution fits

    # also look for uvfits output from birli
    birli_files = glob.glob(
        os.path.join(
            complete_path,
            f"{test_obs_id}*.uvfits",
        )
    )

    expected_birli_files = 12 if picket_fence else 1

    assert len(birli_files) == expected_birli_files, "Number of uvfits files found != expected uvfits files"

    assert os.path.exists(
        os.path.join(
            complete_path,
            f"{test_obs_id}_birli_readme.txt",
        )
    ), "test_obs_id_birli_readme.txt not found"

    # look for hyperdrive readme files
    hyperdrive_readme_files = glob.glob(
        os.path.join(
            complete_path,
            f"{test_obs_id}*_hyperdrive_readme.txt",
        )
    )

    expected_hyperdrive_readme_files = 12 if picket_fence else 1
    assert (
        len(hyperdrive_readme_files) == expected_hyperdrive_readme_files
    ), "correct number of readme hyperdrive file not found"

    # look for solutions
    hyperdrive_solution_files = glob.glob(
        os.path.join(
            complete_path,
            f"{test_obs_id}*_solutions.fits",
        )
    )

    expected_hyperdrive_solution_files = 12 if picket_fence else 1

    assert (
        len(hyperdrive_solution_files) == expected_hyperdrive_solution_files
    ), "correct number of hyperdrive solutions files not found"

    bin_solution_files = glob.glob(os.path.join(complete_path, f"{test_obs_id}*.bin"))
    # expected bin files should == expected solution files
    assert (
        len(bin_solution_files) == expected_hyperdrive_solution_files
    ), "correct number of bin solution files not found"

    # look for stats.txt
    stats_files = glob.glob(
        os.path.join(
            complete_path,
            f"{test_obs_id}*_stats.txt",
        )
    )

    expected_hyperdrive_stats_files = 12 if picket_fence else 1

    assert len(stats_files) == expected_hyperdrive_stats_files, "correct number of stats files not found"

    # processing errors
    processing_error_files = glob.glob(os.path.join(mcal.processing_error_path, f"{test_obs_id}/{test_obs_id}*.fits"))
    assert len(processing_error_files) == 0, "processing_error_files is not 0"


def test_mwax_calvin_test04(setup_calvin_processor_tables):
    """Tests that mwax_calvin does a normal
    simple pipeline run but hyperdrive gets interrupted by a SIGINT"""
    setup_mwax_calvin_test("test04")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinProcessor()

    # Override the hostname
    mcal.hostname = "test_server"

    # Determine config file location
    config_filename = "tests/mwax_calvin_test04.cfg"

    # Call to read config <-- this is what we're testing!
    mcal.initialise(config_filename)
    mcal.produce_debug_plots = False
    mcal.hyperdrive_timeout = 5

    # Start the pipeline
    # Create and start a thread for the processor
    thrd = threading.Thread(name="mcal_thread", target=mcal.start, daemon=True)

    # Start the processor
    thrd.start()

    # allow things to start
    time.sleep(5)

    # Now we simulate TEST_OBS files being delivered into the realtime watch dir
    incoming_files = glob.glob(
        os.path.join(f"tests/data/{TEST_CONTIG_OBS_ID}", f"{TEST_CONTIG_OBS_ID}_*_gpubox*_00.fits")
    )

    for filename in incoming_files:
        dest_filename = os.path.join(mcal.incoming_realtime_watch_path, os.path.basename(filename))

        shutil.copyfile(filename, dest_filename)

    # Wait for processing
    time.sleep(15)

    # Quit
    # Ok time's up! Stop the processor
    mcal.signal_handler(signal.SIGINT, 0)
    thrd.join()

    # Now check results

    # Assembly
    assemble_files = glob.glob(os.path.join(mcal.assemble_path, f"{TEST_CONTIG_OBS_ID}/{TEST_CONTIG_OBS_ID}*.fits"))
    assert len(assemble_files) == 0

    # processing
    # we interrupted the processing, so the obs should stay in the "processing" dir
    processing_files = glob.glob(
        os.path.join(
            mcal.job_output_path,
            f"{TEST_CONTIG_OBS_ID}/{TEST_CONTIG_OBS_ID}*.fits",
        )
    )
    assert len(processing_files) == 25  # solution fits, metafits, 24 gpubox files
