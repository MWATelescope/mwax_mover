"""Tests for calvin.birli: running Birli and estimating its output size.

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import mwalib
from tests_common import obs_metafits_path

from mwax_mover.calvin.birli import estimate_birli_output_bytes, estimate_working_space_gb
from mwax_mover.constants import WORKING_PATH_MAX_GB


def test_estimate_birli_output_bytes():
    test_metafits = obs_metafits_path(1244973688)
    metafits_context = mwalib.MetafitsContext(test_metafits, None)
    calc_bytes: float = estimate_birli_output_bytes(metafits_context, 40, 2.0)
    # Manually calculate the gigabytes
    # manual = timesteps * baselines * coarse_channels * fine_channels * pols * bytes_per_r_i (from Birli)
    manual_bytes: float = 60 * 8256 * 24 * 32 * 4 * 13
    assert calc_bytes == manual_bytes


def test_estimate_working_space_gb_without_imaging_is_the_birli_size():
    assert estimate_working_space_gb(120, False) == 120


def test_estimate_working_space_gb_with_imaging_is_scaled():
    assert estimate_working_space_gb(120, True) == 240


def test_estimate_working_space_gb_scaling_can_cross_the_working_path_limit():
    """A Birli size under the limit can be over it once imaging doubles the need (Birli output moves to the job dir)."""
    assert estimate_working_space_gb(300, False) < WORKING_PATH_MAX_GB
    assert estimate_working_space_gb(300, True) >= WORKING_PATH_MAX_GB
