"""
Tests for the utils.py module

NOTE: some tests (e.g. validate_filename and get_priority) use filenames
which do not exist in the git repo. This is fine as the main thing being
tested is the filename and metafits file (which is included).
"""

from configparser import ConfigParser
import logging
import os
import pytest
import queue
import numpy as np
from mwax_mover import utils


def test_correlator_mode_class():
    assert utils.CorrelatorMode.is_no_capture("NO_CAPTURE")
    assert not utils.CorrelatorMode.is_correlator("NO_CAPTURE")
    assert not utils.CorrelatorMode.is_vcs("NO_CAPTURE")
    assert not utils.CorrelatorMode.is_voltage_buffer("NO_CAPTURE")

    assert not utils.CorrelatorMode.is_no_capture("MWAX_CORRELATOR")
    assert utils.CorrelatorMode.is_correlator("MWAX_CORRELATOR")
    assert not utils.CorrelatorMode.is_vcs("MWAX_CORRELATOR")
    assert not utils.CorrelatorMode.is_voltage_buffer("MWAX_CORRELATOR")

    assert not utils.CorrelatorMode.is_no_capture("MWAX_VCS")
    assert not utils.CorrelatorMode.is_correlator("MWAX_VCS")
    assert utils.CorrelatorMode.is_vcs("MWAX_VCS")
    assert not utils.CorrelatorMode.is_voltage_buffer("MWAX_VCS")

    assert not utils.CorrelatorMode.is_no_capture("MWAX_BUFFER")
    assert not utils.CorrelatorMode.is_correlator("MWAX_BUFFER")
    assert not utils.CorrelatorMode.is_vcs("MWAX_BUFFER")
    assert utils.CorrelatorMode.is_voltage_buffer("MWAX_BUFFER")


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
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1244973688
    assert val.filetype_id == utils.MWADataFileType.MWAX_VISIBILITIES.value
    assert val.file_ext == ".fits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_validate_filename_valid2():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = os.path.join(os.getcwd(), "tests/data/correlator_calibrator")
    logger = logging.getLogger("test")

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1347318488_20190619100110_ch114_000.fits",
    )

    #
    # Run test
    #
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

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
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

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
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

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
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

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
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

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
    val: utils.ValidationData = utils.validate_filename(logger, filename, metafits_path)

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
    is_calibrator, project_id = utils.get_metafits_values("tests/data/correlator_calibrator/1347318488_metafits.fits")
    assert is_calibrator is True
    assert project_id == "G0080"


def test_get_metafits_values_non_cal():
    """
    Test that we can find out project and cal info from a
    metafits which is not a calibrator- i.e. it has
    CALIBRAT=False and no CALIBSRC key
    """
    is_calibrator, project_id = utils.get_metafits_values("tests/data/correlator_C001/1244973688_metafits.fits")
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
    utils.scan_for_existing_files_and_add_to_queue(logger, watch_dir, pattern, recursive, queue_target)

    assert queue_target.qsize() == 2
    assert queue_target.get() == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert queue_target.get() == os.path.join(os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits"))


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

    assert str(item1[1]) == os.path.join(os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits"))
    assert item1[0] == 1  # metafits ppd file

    # get second item
    item2 = queue_target.get()

    assert str(item2[1]) == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert item2[0] == 30  # Regular correlator obs


def test_scan_directory():
    """Tests we can get a list of files in a dir"""
    watch_dir = "tests/data/correlator_C001"
    pattern = ".fits"
    recursive = False
    logger = logging.getLogger("test")

    #
    # Run test
    #
    list_of_files = utils.scan_directory(logger, watch_dir, pattern, recursive, exclude_pattern=None)

    assert len(list_of_files) == 2
    assert (
        os.path.join(
            os.getcwd(),
            os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
        )
        in list_of_files
    )
    assert os.path.join(os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits")) in list_of_files


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
    assert priority == 2


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
    assert priority == 3


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
    assert priority == 1


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


def test_determine_bucket_acacia():
    """Tests we get the correct bucket and folder given a filename and location"""
    full_filename = os.path.join(
        os.getcwd(),
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
    )
    location = utils.ArchiveLocation.Acacia
    #
    # Run test
    #
    bucket = utils.determine_bucket(full_filename, location)
    assert bucket == "mwaingest-12449"


def test_determine_bucket_banksia():
    """Tests we get the correct bucket and folder given a filename and location"""
    full_filename = os.path.join(
        os.getcwd(),
        "tests/data/correlator_C001/1244973688_20190619100110_ch114_000.fits",
    )
    location = utils.ArchiveLocation.Banksia
    #
    # Run test
    #
    bucket = utils.determine_bucket(full_filename, location)
    assert bucket == "mwaingest-12449"


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
    config_filename = os.path.join(os.getcwd(), "tests/mwax_subfile_distributor_correlator_test.cfg")
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    return_list = utils.read_config_list(logger, config, "correlator", "high_priority_vcs_projectids")

    assert return_list == ["D0006", "G0058"]


def test_config_get_bool_true():
    logger = logging.getLogger("test")

    config_filename = os.path.join(os.getcwd(), "tests/mwax_calvin_test02.cfg")
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    true_bool = utils.read_config_bool(logger, config, "complete", "keep_completed_visibility_files")

    assert true_bool is True


def test_config_get_bool_false():
    logger = logging.getLogger("test")
    config_filename = os.path.join(os.getcwd(), "tests/mwax_calvin_test01.cfg")
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    false_bool = utils.read_config_bool(logger, config, "complete", "keep_completed_visibility_files")

    assert false_bool is False


def test_config_get_list_empty():
    """Read a string from a config file, then
    split (by comma) into a list
    e.g. abc,def,ghi would result in ["abc", "def", "ghi"]
    An empty string would result in and empty list []
    """
    logger = logging.getLogger("test")
    config_filename = os.path.join(os.getcwd(), "tests/mwax_subfile_distributor_correlator_test.cfg")
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    return_list = utils.read_config_list(logger, config, "correlator", "high_priority_correlator_projectids")

    assert return_list == []


def test_config_get_optional_value():
    """Read an empty string from a config file and ensure it gets
    treated as None. Also test an non empty gets read right too"""
    logger = logging.getLogger("test")
    config_filename = os.path.join(os.getcwd(), "tests/mwacache_test.cfg")
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    empty_return_val = utils.read_optional_config(logger, config, "banksia", "max_concurrency")

    non_empty_return_val = utils.read_optional_config(logger, config, "acacia", "max_concurrency")

    assert empty_return_val is None
    assert non_empty_return_val is not None


def test_config_get_optional_value_spaces_not_empty_string():
    """Read an empty string which has spaces in it from a config file and ensure it gets
    treated as None. Also test an non empty gets read right too"""
    logger = logging.getLogger("test")
    config_filename = os.path.join(os.getcwd(), "tests/mwacache_test.cfg")
    config = ConfigParser()
    config.read_file(open(config_filename, "r", encoding="utf-8"))

    empty_return_val = utils.read_optional_config(logger, config, "banksia", "chunk_size_bytes")

    non_empty_return_val = utils.read_optional_config(logger, config, "acacia", "chunk_size_bytes")

    assert empty_return_val is None
    assert non_empty_return_val is not None


def test_download_metafits_file():
    """Test that we can download a metafits file by obsid
    from the web service"""
    logger = logging.getLogger("test")
    obs_id = 1244973688
    metafits_path = "tests/data/"
    metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    utils.download_metafits_file(logger, obs_id, metafits_path)

    assert os.path.exists(metafits_filename)

    # remove the metafits file
    os.remove(metafits_filename)


def test_write_mock_subfile():
    """Test that our mock subfile is correct"""
    output_filename = "/tmp/mock_subfile.sub"

    # Write out the mock subfile
    utils.write_mock_subfile(
        output_filename,
        obs_id=1234567890,
        subobs_id=1234567898,
        mode="MWAX_VCS",
        obs_offset=8,
        rec_channel=123,
        corr_channel=5,
    )

    # This is what it should look like
    expected_header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        "OBS_ID 1234567890\n"
        "SUBOBS_ID 1234567898\n"
        "MODE MWAX_VCS\n"
        "UTC_START 2023-01-13-03:33:10\n"
        "OBS_OFFSET 8\n"
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
        "COARSE_CHANNEL 123\n"
        "CORR_COARSE_CHANNEL 5\n"
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

    # Read in the first 4096 bytes as text/ascii
    with open(output_filename, "rb") as subfile:
        # Read header
        header_bytes = subfile.read(len(expected_header))

    # convert bytes to ascii
    header_text = header_bytes.decode()

    assert header_text == expected_header


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
    remainder_len = utils.PSRDADA_HEADER_BYTES - len(test_header)
    padding = [0x0 for _ in range(remainder_len)]
    assert len(padding) == remainder_len
    # add 255 bytes of data to this subfile
    data_padding = [x for x in range(256)]
    assert len(data_padding) == 256

    # Write the subfile
    subfile_name = "tests/data/beamformer/test_subfile.sub"
    utils.write_mock_subfile_from_header(subfile_name, test_header)

    # inject the beamformer settings
    utils.inject_beamformer_headers(subfile_name, beamformer_settings_string)

    # Check file size
    assert os.path.getsize(subfile_name) == utils.PSRDADA_HEADER_BYTES + len(bytearray(data_padding))

    # we can also test utils.read_subfile_value(item, key)
    assert utils.read_subfile_value(subfile_name, utils.PSRDADA_MODE) == "NO_CAPTURE"
    assert utils.read_subfile_value(subfile_name, "NUM_INCOHERENT_BEAMS") == "2"

    # check for None on a non-existant key
    assert utils.read_subfile_value(subfile_name, "MISSING_KEY123") is None


def test_should_project_be_archived():
    assert utils.should_project_be_archived("C001") is True
    assert utils.should_project_be_archived("c001") is True
    assert utils.should_project_be_archived("C123") is False
    assert utils.should_project_be_archived("c123") is False


def test_get_data_files_for_obsid_from_webservice_404():
    logger = logging.getLogger("test")

    # Uknown obsid- raises exception
    with pytest.raises(Exception):
        utils.get_data_files_for_obsid_from_webservice(logger, 1234567890)


def test_get_data_files_for_obsid_from_webservice_200():
    logger = logging.getLogger("test")

    # Good obsid with 24 gpubox files and 1 flags and 1 metafits. Only return the 24 gpubox files
    file_list = utils.get_data_files_for_obsid_from_webservice(logger, 1157306584)
    assert len(file_list) == 24

    assert file_list == [
        "1157306584_20160907180249_gpubox01_00.fits",
        "1157306584_20160907180249_gpubox02_00.fits",
        "1157306584_20160907180249_gpubox03_00.fits",
        "1157306584_20160907180249_gpubox04_00.fits",
        "1157306584_20160907180249_gpubox05_00.fits",
        "1157306584_20160907180249_gpubox06_00.fits",
        "1157306584_20160907180249_gpubox07_00.fits",
        "1157306584_20160907180249_gpubox08_00.fits",
        "1157306584_20160907180249_gpubox09_00.fits",
        "1157306584_20160907180249_gpubox10_00.fits",
        "1157306584_20160907180249_gpubox11_00.fits",
        "1157306584_20160907180249_gpubox12_00.fits",
        "1157306584_20160907180249_gpubox13_00.fits",
        "1157306584_20160907180249_gpubox14_00.fits",
        "1157306584_20160907180249_gpubox15_00.fits",
        "1157306584_20160907180249_gpubox16_00.fits",
        "1157306584_20160907180249_gpubox17_00.fits",
        "1157306584_20160907180249_gpubox18_00.fits",
        "1157306584_20160907180249_gpubox19_00.fits",
        "1157306584_20160907180249_gpubox20_00.fits",
        "1157306584_20160907180249_gpubox21_00.fits",
        "1157306584_20160907180249_gpubox22_00.fits",
        "1157306584_20160907180249_gpubox23_00.fits",
        "1157306584_20160907180249_gpubox24_00.fits",
    ]


def test_get_packet_map():
    # For this test, the IDX_PACKET_MAP is:
    # IDX_PACKET_MAP 1859328+180000
    #
    #
    logger = logging.getLogger("test")
    filename = "tests/data/1416028864_1416028872_109.sub"

    # Check test file exists firstly!
    assert os.path.exists(filename), f"Test file {filename} doesn't exist."

    packet_map_data = utils.get_subfile_packet_map_data(logger, filename)

    assert packet_map_data is not None

    # Check length of bytes
    assert len(packet_map_data) == 180000


def test_convert_occupany_bitmap_to_array():
    input = bytes([255, 0, 255])

    assert input[0] == 255
    assert input[1] == 0
    assert input[2] == 255

    input = np.frombuffer(input, dtype=np.uint8)
    assert input.shape == (3,)

    assert np.array_equal(
        utils.convert_occupany_bitmap_to_array(input),
        [
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
        ],
    )


def test_summarise_packet_map():
    num_rf_inputs = 2

    # This should look like:
    # 01111111 for each of the packets in the first rfinput
    # and
    # 00000001 for each of the packets in the second rfinput
    input = bytes([127] * 625) + bytes([1] * 625)
    assert len(input) == 625 * 2

    packets_lost = utils.summarise_packet_map(num_rf_inputs, input)

    assert packets_lost.shape == (2,)

    # 0.875
    assert packets_lost[0] == 1 * 625

    # 0.125
    assert packets_lost[1] == 7 * 625
