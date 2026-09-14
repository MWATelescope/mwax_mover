"""Tests for fits.metafits: downloading and reading metafits FITS files.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).
"""

import os

import pytest

from tests_common import obs_metafits_path

from mwax_mover.fits.metafits import download_metafits_file, get_calibrator_info


def test_get_calibrator_info_correlator():
    """
    Test that we can find out if the obs is a calibrator and it's
    project id from the metafits file
    """
    #
    # Run test
    #
    is_calibrator, project_id, calib_src = get_calibrator_info(obs_metafits_path(1347318488))
    assert is_calibrator is True
    assert project_id == "G0080"
    assert calib_src == "J063633-204225"


def test_get_calibrator_info_non_cal():
    """
    Test that we can find out project and cal info from a
    metafits which is not a calibrator- i.e. it has
    CALIBRAT=False and no CALIBSRC key
    """
    is_calibrator, project_id, calib_src = get_calibrator_info(obs_metafits_path(1244973688))
    assert is_calibrator is False
    assert project_id == "C001"
    assert calib_src == ""


@pytest.mark.integration
def test_download_metafits_file():
    """Test that we can download a metafits file by obsid
    from the web service"""

    obs_id = 1244973688
    metafits_path = "/tmp"
    metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    download_metafits_file(obs_id, metafits_path)

    assert os.path.exists(metafits_filename)

    # remove the metafits file
    os.remove(metafits_filename)
