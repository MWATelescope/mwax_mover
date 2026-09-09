"""Tests for filesystem.naming: filename validation, bucket naming,
archiving priority, and data-file webservice queries.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).

NOTE: some tests (e.g. validate_filename and get_priority) use filenames
which do not exist in the git repo. This is fine as the main thing being
tested is the filename and metafits file (which is included).
"""

import os

import pytest
import requests

from tests_common import data_path, obs_data_dir

from mwax_mover.filesystem.naming import (
    ArchiveLocation,
    MWADataFileType,
    ValidationData,
    get_bucket_name_for_location,
    get_bucket_name_from_filename,
    get_bucket_name_from_obs_id,
    get_data_files_for_obsid_from_webservice,
    get_priority,
    should_project_be_archived,
    validate_filename,
)


def test_validate_filename_valid1():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1244973688)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1244973688_20190619100110_ch114_000.fits",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1244973688
    assert val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value
    assert val.file_ext == ".fits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_validate_filename_valid2():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1347318488)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1347318488_20190619100110_ch114_000.fits",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1347318488
    assert val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value
    assert val.file_ext == ".fits"
    assert val.calibrator is True
    assert val.project_id == "G0080"


def test_validate_filename_valid3():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1220738720)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1220738720_1220738720_123.sub",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1220738720
    assert val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value
    assert val.file_ext == ".sub"
    assert val.calibrator is False
    assert val.project_id == "G0024"


def test_validate_filename_valid4():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1220738720)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1220738720_1220738720_13.sub",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1220738720
    assert val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value
    assert val.file_ext == ".sub"
    assert val.calibrator is False
    assert val.project_id == "G0024"


def test_validate_filename_valid5():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1220738720)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1220738720_1220738720_1.sub",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1220738720
    assert val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value
    assert val.file_ext == ".sub"
    assert val.calibrator is False
    assert val.project_id == "G0024"


def test_validate_filename_valid6():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1328239120)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1328239120_metafits_ppds.fits",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1328239120
    assert val.filetype_id == MWADataFileType.MWA_PPD_FILE.value
    assert val.file_ext == ".fits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_validate_filename_valid7():
    """Test that validate_filename() correctly identifies attributes based on filename"""
    metafits_path = obs_data_dir(1328239120)

    # Test for a normal MWAX correlator file
    filename = os.path.join(
        metafits_path,
        "1328239120.metafits",
    )

    #
    # Run test
    #
    val: ValidationData = validate_filename(filename, metafits_path)

    assert val.valid is True
    assert val.obs_id == 1328239120
    assert val.filetype_id == MWADataFileType.MWA_PPD_FILE.value
    assert val.file_ext == ".metafits"
    assert val.calibrator is False
    assert val.project_id == "C001"


def test_get_priority_correlator_calibrator():
    """Test that a correlator calibrator observation gets correct priority"""
    #
    # Run test
    #

    priority = get_priority(
        data_path("1347318488", "1347318488_20190619100110_ch101_000.fits"),
        obs_data_dir(1347318488),
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

    priority = get_priority(
        data_path("1122979144", "1122979144_20190619100110_ch101_000.fits"),
        obs_data_dir(1122979144),
        ["D0006"],
        ["C001"],
    )
    assert priority == 3


def test_get_priority_vcs_c001():
    """Test that a high priority VCS observation gets correct priority"""
    #
    # Run test
    #

    priority = get_priority(
        data_path("1347063304", "1347063304_1347063304_114.sub"),
        obs_data_dir(1347063304),
        ["D0006"],
        ["C001"],
    )
    assert priority == 20


def test_get_priority_correlator_c001():
    """that a normal correlator observation gets correct priority"""
    #
    # Run test
    #

    priority = get_priority(
        data_path("1244973688", "1244973688_20190619100110_ch114_000.fits"),
        obs_data_dir(1244973688),
        ["D0006"],
        ["C001"],
    )
    assert priority == 30


def test_get_priority_vcs_g0024():
    """that a normal VCS observation gets correct priority"""
    #
    # Run test
    #

    priority = get_priority(
        data_path("1220738720", "1220738720_1220738720_123.sub"),
        obs_data_dir(1220738720),
        ["D0006"],
        ["C001"],
    )
    assert priority == 90


def test_get_priority_metafits_ppd():
    """that a metafits_ppd file gets correct priority"""
    #
    # Run test
    #

    priority = get_priority(
        data_path("1328239120", "1328239120_metafits_ppds.fits"),
        obs_data_dir(1328239120),
        ["D0006"],
        ["C001"],
    )
    assert priority == 1


def test_get_bucket_name_for_location_acacia():
    """Tests we get the correct bucket and folder given a filename and location"""
    full_filename = os.path.join(
        os.getcwd(),
        data_path("1244973688", "1244973688_20190619100110_ch114_000.fits"),
    )
    location = ArchiveLocation.AcaciaIngest
    #
    # Run test
    #
    bucket = get_bucket_name_for_location(full_filename, location)
    assert bucket == "mwaingest-12449"


def test_get_bucket_name_for_location_banksia():
    """Tests we get the correct bucket and folder given a filename and location"""
    full_filename = os.path.join(
        os.getcwd(),
        data_path("1244973688", "1244973688_20190619100110_ch114_000.fits"),
    )
    location = ArchiveLocation.Banksia
    #
    # Run test
    #
    bucket = get_bucket_name_for_location(full_filename, location)
    assert bucket == "mwaingest-12449"


def test_get_bucket_name_from_filename():
    """Test getting a bucket name for a filename"""
    filename = os.path.join(
        os.getcwd(),
        data_path("1244973688", "1244973688_20190619100110_ch114_000.fits"),
    )

    #
    # Run test
    #
    bucket = get_bucket_name_from_filename(filename)

    assert bucket == "mwaingest-12449"


def test_get_bucket_name_from_obs_id():
    """Test getting the bucket name from an obs_id)"""
    obs_id = 1234567890

    #
    # Run test
    #
    bucket = get_bucket_name_from_obs_id(obs_id)

    assert bucket == "mwaingest-12345"


def test_should_project_be_archived():
    assert should_project_be_archived("C001") is True
    assert should_project_be_archived("c001") is True
    assert should_project_be_archived("C123") is False
    assert should_project_be_archived("c123") is False


@pytest.mark.integration
def test_get_data_files_for_obsid_from_webservice_404():

    # Unknown obsid- call_webservice raises once every url/retry is exhausted
    with pytest.raises(requests.RequestException):
        get_data_files_for_obsid_from_webservice(1234567890)


@pytest.mark.integration
def test_get_data_files_for_obsid_from_webservice_200():

    # Good obsid with 24 gpubox files and 1 flags and 1 metafits. Only return the 24 gpubox files
    file_list = get_data_files_for_obsid_from_webservice(1157306584)
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
