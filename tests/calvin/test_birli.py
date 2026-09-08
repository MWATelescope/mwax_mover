"""Tests for calvin.birli: running Birli and estimating its output size.

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import mwalib

from tests_common import obs_metafits_path

from mwax_mover.calvin.birli import estimate_birli_output_bytes


def test_estimate_birli_output_bytes():
    test_metafits = obs_metafits_path(1244973688)
    metafits_context = mwalib.MetafitsContext(test_metafits, None)
    calc_bytes: float = estimate_birli_output_bytes(metafits_context, 40, 2.0)
    # Manually calculate the gigabytes
    # manual = timesteps * baselines * coarse_channels * fine_channels * pols * bytes_per_r_i (from Birli)
    manual_bytes: float = 60 * 8256 * 24 * 32 * 4 * 13
    assert calc_bytes == manual_bytes
