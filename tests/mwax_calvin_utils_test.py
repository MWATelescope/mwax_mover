from mwax_mover import mwax_calvin_utils


def test_read_cal_solutions():
    mwax_calvin_utils.read_cal_solutions(
        None,
        1339580448,
        "/home/gsleap/work/github/mwax_mover/tests/mock_mwax_calvin_test02/complete/1339580448/1339580448_metafits.fits",
        "/home/gsleap/work/github/mwax_mover/tests/mock_mwax_calvin_test02/complete/1339580448/1339580448_solutions.bin",
    )
