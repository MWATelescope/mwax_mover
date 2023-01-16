"""
Tests for the utils.py module

NOTE: some tests (e.g. validate_filename and get_priority) use filenames
which do not exist in the git repo. This is fine as the main thing being
tested is the filename and metafits file (which is included).
"""
from configparser import ConfigParser
import logging
import os
import queue
from mwax_mover import utils


def test_validate_filename_valid1():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/correlator_C001")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1244973688_20190619100110_ch114_000.fits",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1244973688
    assert val.filetype_id == utils.MWADataFileType.MWAX_VISIBILITIES.value
    assert val.file_ext == ".fits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_validate_filename_valid2():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(
        os.getcwd(), "tests/data/correlator_calibrator"
    )
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1347318488_20190619100110_ch114_000.fits",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1347318488
    assert val.filetype_id == utils.MWADataFileType.MWAX_VISIBILITIES.value
    assert val.file_ext == ".fits"
    assert val.calibrator is True
    assert val.project_id == "G0080"


def test_validate_filename_valid3():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/vcs_G0024")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1220738720_1220738720_123.sub",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1220738720
    assert val.filetype_id == utils.MWADataFileType.MWAX_VOLTAGES.value
    assert val.file_ext == ".sub"
    assert val.calibrator is False
    assert val.project_id == "G0024"


def test_validate_filename_valid4():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/vcs_G0024")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1220738720_1220738720_13.sub",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1220738720
    assert val.filetype_id == utils.MWADataFileType.MWAX_VOLTAGES.value
    assert val.file_ext == ".sub"
    assert val.calibrator is False
    assert val.project_id == "G0024"


def test_validate_filename_valid5():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/vcs_G0024")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1220738720_1220738720_1.sub",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1220738720
    assert val.filetype_id == utils.MWADataFileType.MWAX_VOLTAGES.value
    assert val.file_ext == ".sub"
    assert val.calibrator is False
    assert val.project_id == "G0024"


def test_validate_filename_valid6():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/metafits_ppd")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1328239120_metafits_ppds.fits",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1328239120
    assert val.filetype_id == utils.MWADataFileType.MWA_PPD_FILE.value
    assert val.file_ext == ".fits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_validate_filename_valid7():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/metafits_ppd")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1328239120.metafits",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(
        logger, filename, metafits_path
    )

    assert val.valid is True
    assert val.obs_id == 1328239120
    assert val.filetype_id == utils.MWADataFileType.MWA_PPD_FILE.value
    assert val.file_ext == ".metafits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_get_metafits_values_correlator():
    """
    Test that we can find out if the obs is a calibrator and it's
    project id from the metafits file
    """
    #
    # Run test
    #
    is_calibrator, project_id = utils.get_metafits_values(
        "tests/data/correlator_C001/1244973688_metafits.fits"
    )
    assert is_calibrator is False
    assert project_id == "C001"


def test_scan_for_existing_files_and_add_to_queue():
    """Test we can find files and add to a queue"""
    queue_target = queue.Queue()
    watch_dir = "tests/data/correlator_C001"
    pattern = ".fits"
    recursive = False
    logger = logging.getLogger("test")

    #
    # Run test
    #
    utils.scan_for_existing_files_and_add_to_queue(
        logger, watch_dir, pattern, recursive, queue_target
    )

    assert queue_target.qsize() == 2
    assert queue_target.get() == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert queue_target.get() == os.path.join(
        os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits")
    )


def test_scan_for_existing_files_and_add_to_priority_queue():
    """Test we can find files and add to a priority queue"""
    queue_target = queue.PriorityQueue()
    watch_dir = "tests/data/correlator_C001"
    pattern = ".fits"
    recursive = False
    metafits_path = watch_dir
    logger = logging.getLogger("test")

    #
    # Run test
    #
    utils.scan_for_existing_files_and_add_to_priority_queue(
        logger,
        metafits_path,
        watch_dir,
        pattern,
        recursive,
        queue_target,
        ["D0006"],
        ["C001"],
    )

    assert queue_target.qsize() == 2

    # Get first item
    item1 = queue_target.get()

    assert str(item1[1]) == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert item1[0] == 30  # Regular correlator obs

    # get second item
    item2 = queue_target.get()

    assert str(item2[1]) == os.path.join(
        os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits")
    )
    assert item2[0] == 100  # metafits ppd file


def test_scan_directory():
    """Tests we can get a list of files in a dir"""
    watch_dir = "tests/data/correlator_C001"
    pattern = ".fits"
    recursive = False
    logger = logging.getLogger("test")

    #
    # Run test
    #
    list_of_files = utils.scan_directory(
        logger, watch_dir, pattern, recursive, exclude_pattern=None
    )

    assert len(list_of_files) == 2
    assert list_of_files[0] == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert list_of_files[1] == os.path.join(
        os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits")
    )


def test_get_priority_correlator_calibrator():
    """Test that a correlator calibrator observation gets correct priority"""
    #
    # Run test
    #
    logger = logging.getLogger("test")

    priority = utils.get_priority(
        logger,
        "tests/data/correlator_calibrator/1347318488_20190619100110_ch101_000.fits",
        "tests/data/correlator_calibrator/",
        ["D0006"],
        ["C001"],
    )
    assert priority == 1


def test_get_priority_correlator_high_priority_list():
    """
    Test that a correlator observation for a project in the high priority list gets
    correct priority
    """
    #
    # Run test
    #
    logger = logging.getLogger("test")

    priority = utils.get_priority(
        logger,
        "tests/data/correlator_D0006_not_cal/1122979144_20190619100110_ch101_000.fits",
        "tests/data/correlator_D0006_not_cal/",
        ["D0006"],
        ["C001"],
    )
    assert priority == 2


def test_get_priority_vcs_c001():
    """Test that a high priority VCS observation gets correct priority"""
    #
    # Run test
    #
    logger = logging.getLogger("test")

    priority = utils.get_priority(
        logger,
        "tests/data/vcs_C001/1347063304_1347063304_114.sub",
        "tests/data/vcs_C001/",
        ["D0006"],
        ["C001"],
    )
    assert priority == 20


def test_get_priority_correlator_c001():
    """that a normal correlator observation gets correct priority"""
    #
    # Run test
    #
    logger = logging.getLogger("test")

    priority = utils.get_priority(
        logger,
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
        "tests/data/correlator_C001/",
        ["D0006"],
        ["C001"],
    )
    assert priority == 30


def test_get_priority_vcs_g0024():
    """that a normal VCS observation gets correct priority"""
    #
    # Run test
    #
    logger = logging.getLogger("test")

    priority = utils.get_priority(
        logger,
        "tests/data/vcs_G0024/1220738720_1220738720_123.sub",
        "tests/data/vcs_G0024/",
        ["D0006"],
        ["C001"],
    )
    assert priority == 90


def test_get_priority_metafits_ppd():
    """that a metafits_ppd file gets correct priority"""
    #
    # Run test
    #
    logger = logging.getLogger("test")

    priority = utils.get_priority(
        logger,
        "tests/data/metafits_ppd/1328239120_metafits_ppds.fits",
        "tests/data/metafits_ppd/",
        ["D0006"],
        ["C001"],
    )
    assert priority == 100


def test_do_checksum_md5():
    """Tests that we can correctly get the MD5 of a file"""

    logger = logging.getLogger("test")
    filename = os.path.join(
        os.getcwd(),
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
    )

    numa_node = None
    timeout = 30

    #
    # Run test
    #
    md5sum = utils.do_checksum_md5(logger, filename, numa_node, timeout)

    assert md5sum == "c1024dd2184887bc293cffe07406046f"


def test_determine_bucket_and_folder_acacia():
    """Tests we get the correct bucket and folder given a filename and location"""
    full_filename = os.path.join(
        os.getcwd(),
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
    )
    location = 2
    #
    # Run test
    #
    bucket, folder = utils.determine_bucket_and_folder(full_filename, location)
    assert bucket == "mwaingest-12449"
    assert folder is None


def test_determine_bucket_and_folder_banksia():
    """Tests we get the correct bucket and folder given a filename and location"""
    full_filename = os.path.join(
        os.getcwd(),
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
    )
    location = 3
    #
    # Run test
    #
    bucket, folder = utils.determine_bucket_and_folder(full_filename, location)
    assert bucket == "mwaingest-12449"
    assert folder is None


def test_get_bucket_name_from_filename():
    """Test getting a bucket name for a filename"""
    filename = os.path.join(
        os.getcwd(),
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
    )

    #
    # Run test
    #
    bucket = utils.get_bucket_name_from_filename(filename)

    assert bucket == "mwaingest-12449"


def test_get_bucket_name_from_obs_id():
    """Test getting the bucket name from an obs_id)"""
    obs_id = 1234567890

    #
    # Run test
    #
    bucket = utils.get_bucket_name_from_obs_id(obs_id)

    assert bucket == "mwaingest-12345"


def test_config_get_list_valid():
    """Read a string from a config file, then
    split (by comma) into a list
    e.g. abc,def,ghi would result in ["abc", "def", "ghi"]
    An empty string would result in and empty list []
    """
    logger = logging.getLogger("test")
    config_filename = os.path.join(
        os.getcwd(), "tests/mwax_subfile_distributor_correlator_test.cfg"
    )
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    return_list = utils.read_config_list(
        logger, config, "correlator", "high_priority_vcs_projectids"
    )

    assert return_list == ["D0006", "G0058"]


def test_config_get_list_empty():
    """Read a string from a config file, then
    split (by comma) into a list
    e.g. abc,def,ghi would result in ["abc", "def", "ghi"]
    An empty string would result in and empty list []
    """
    logger = logging.getLogger("test")
    config_filename = os.path.join(
        os.getcwd(), "tests/mwax_subfile_distributor_correlator_test.cfg"
    )
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    return_list = utils.read_config_list(
        logger, config, "correlator", "high_priority_correlator_projectids"
    )

    assert return_list == []


def test_download_metafits_file():
    """Test that we can download a metafits file by obsid
    from the web service"""
    obs_id = 1244973688
    metafits_path = "tests/data/"
    metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    utils.download_metafits_file(obs_id, metafits_path)

    assert os.path.exists(metafits_filename)

    # remove the metafits file
    os.remove(metafits_filename)


def test_inject_beamformer_headers():
    """Test that, given a sub file which has a 4096 byte header
    that we can find the end of header and 'paste' in the beamformer
    header to the end (and still maintain a 4096 byte header!)"""

    # Generate test beamformer settings
    beamformer_settings_string = (
        "NUM_INCOHERENT_BEAMS 2\n"
        "INCOHERENT_BEAM_01_CHANNELS 1280000\n"
        "INCOHERENT_BEAM_01_TIME_INTEG 1\n"
        "INCOHERENT_BEAM_02_CHANNELS 128\n"
        "INCOHERENT_BEAM_02_TIME_INTEG 100\n"
        "NUM_COHERENT_BEAMS 0\n"
    )

    # Generate a test header
    test_header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        "OBS_ID 1357616008\n"
        "SUBOBS_ID 1357623888\n"
        "MODE NO_CAPTURE\n"
        "UTC_START 2023-01-13-03:33:10\n"
        "OBS_OFFSET 7880\n"
        "NBIT 8\n"
        "NPOL 2\n"
        "NTIMESAMPLES 64000\n"
        "NINPUTS 256\n"
        "NINPUTS_XGPU 256\n"
        "APPLY_PATH_WEIGHTS 0\n"
        "APPLY_PATH_DELAYS 1\n"
        "APPLY_PATH_PHASE_OFFSETS 1\n"
        "INT_TIME_MSEC 500\n"
        "FSCRUNCH_FACTOR 200\n"
        "APPLY_VIS_WEIGHTS 0\n"
        "TRANSFER_SIZE 5275648000\n"
        "PROJ_ID G0060\n"
        "EXPOSURE_SECS 200\n"
        "COARSE_CHANNEL 169\n"
        "CORR_COARSE_CHANNEL 12\n"
        "SECS_PER_SUBOBS 8\n"
        "UNIXTIME 1673580790\n"
        "UNIXTIME_MSEC 0\n"
        "FINE_CHAN_WIDTH_HZ 40000\n"
        "NFINE_CHAN 32\n"
        "BANDWIDTH_HZ 1280000\n"
        "SAMPLE_RATE 1280000\n"
        "MC_IP 0.0.0.0\n"
        "MC_PORT 0\n"
        "MC_SRC_IP 0.0.0.0\n"
        "MWAX_U2S_VER 2.09-87\n"
        "IDX_PACKET_MAP 0+200860892\n"
        "IDX_METAFITS 32+1\n"
        "IDX_DELAY_TABLE 16383744+0\n"
        "IDX_MARGIN_DATA 256+0\n"
        "MWAX_SUB_VER 2\n"
    )

    assert len(test_header) == 720

    # Append the remainder of the 4096 bytes
    remainder_len = 4096 - len(test_header)
    padding = [0x0 for _ in range(remainder_len)]
    assert len(padding) == remainder_len
    # add 255 bytes of data to this subfile
    data_padding = [x for x in range(256)]
    assert len(data_padding) == 256

    # Convert to bytes
    test_header_bytes = bytes(test_header, "UTF-8")

    # Generate a test sub file
    subfile_name = "tests/data/beamformer/test_subfile.sub"
    with open(subfile_name, "wb") as write_file:
        write_file.write(test_header_bytes)
        write_file.write(bytearray(padding))
        write_file.write(bytearray(data_padding))

    utils.inject_beamformer_headers(subfile_name, beamformer_settings_string)

    # Check file size
    assert os.path.getsize(subfile_name) == 4096 + len(bytearray(data_padding))

    # we can also test utils.read_subfile_value(item, key)
    assert utils.read_subfile_value(subfile_name, "MODE") == "NO_CAPTURE"
    assert (
        utils.read_subfile_value(subfile_name, "NUM_INCOHERENT_BEAMS") == "2"
    )
