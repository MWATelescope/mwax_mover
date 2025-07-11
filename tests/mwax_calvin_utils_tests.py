"""
Tests for the mwax_calvin_utils.py module
"""

import mwalib
import mwax_mover
import mwax_mover.mwax_calvin_utils


def test_estimate_birli_output_GB():
    test_metafits = "tests/data/correlator_C001/1244973688_metafits.fits"

    metafits_context = mwalib.MetafitsContext(test_metafits, None)

    calc_bytes: float = mwax_mover.mwax_calvin_utils.estimate_birli_output_bytes(metafits_context, 40, 2.0)

    # Manually calculate the gigabytes
    # manual = baselines * coarse_channels * fine_channels * pols * values * bytes_per_value * timesteps
    manual_bytes: float = 8256 * 24 * 32 * 4 * 2 * 4 * 60

    assert calc_bytes == manual_bytes
