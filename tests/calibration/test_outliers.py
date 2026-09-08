"""Tests for calibration.outliers: reject_outliers.

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import pandas as pd

from mwax_mover.calibration.outliers import reject_outliers


def _make_phase_fits_df(lengths: list[float], pol: str = "XX", flavor: str = "RRI") -> pd.DataFrame:
    """Minimal phase fits DataFrame for reject_outliers tests."""
    rows = [
        {
            "tile_id": i + 1,
            "pol": pol,
            "flavor": flavor,
            "length": length_val,
            "chi2dof": length_val,  # reuse length as chi2dof for convenience
            "sigma_resid": 0.1,
            "quality": 1.0,
        }
        for i, length_val in enumerate(lengths)
    ]
    return pd.DataFrame(rows)


def test_reject_outliers_marks_high_value():
    """One extreme outlier should be flagged; clustered values should not be."""
    # Use nstd=1 so the threshold is low enough to flag 1000.0
    # With 9 values of 1.0 and one of 1000.0: mean≈100.9, std≈298, threshold≈100.9+298≈399
    lengths = [1.0] * 9 + [1000.0]
    df = _make_phase_fits_df(lengths)
    result = reject_outliers(df, "chi2dof", nstd=1.0)
    assert result.loc[result["chi2dof"] == 1000.0, "outlier"].all()
    assert not result.loc[result["chi2dof"] == 1.0, "outlier"].any()


def test_reject_outliers_nstd_zero_no_changes():
    """nstd=0 returns early without adding the outlier column."""
    lengths = [1.0, 2.0, 100.0]
    df = _make_phase_fits_df(lengths)
    result = reject_outliers(df, "chi2dof", nstd=0)
    # The function returns early before adding the outlier column
    assert "outlier" not in result.columns


def test_reject_outliers_per_pol_independent():
    """An outlier in XX pol must not cause YY rows to be flagged."""
    # reject_outliers mutates in-place and tracks outliers per pol independently.
    # We pass a combined DataFrame and check that YY rows are not flagged
    # even though XX has an outlier — using nstd=1 to ensure XX outlier is found.
    xx_rows = _make_phase_fits_df([1.0] * 9 + [1000.0], pol="XX")
    yy_rows = _make_phase_fits_df([1.0] * 10, pol="YY")
    df = pd.concat([xx_rows, yy_rows], ignore_index=True)
    result = reject_outliers(df, "chi2dof", nstd=1.0)
    # XX outlier should be flagged
    xx_outlier = result[(result["pol"] == "XX") & (result["chi2dof"] == 1000.0)]
    assert xx_outlier["outlier"].all()
    # YY rows all have chi2dof == 1.0, i.e. zero variance -- nothing stands
    # out, so none of them should be flagged (see
    # test_reject_outliers_zero_std_flags_nobody for the dedicated case).
    yy_result = result[result["pol"] == "YY"]
    assert not yy_result["outlier"].any()


def test_reject_outliers_zero_std_flags_nobody():
    """A population with zero variance flags no one, not everyone.

    Regression test: threshold = mean + nstd*std collapses to threshold ==
    mean when std == 0, and "value >= threshold" would otherwise flag
    every row via trivial equality -- the opposite of correct behaviour
    when nothing actually stands out. Found via a real pipeline failure:
    a synthetic test fixture with identical (unit-gain) Jones matrices for
    every tile produced identical chi2dof for all of them, which flagged
    every tile as an outlier and NaN'd the entire observation.
    """
    lengths = [2.960881] * 6  # all rows identical, matching the real failure
    df = _make_phase_fits_df(lengths)
    result = reject_outliers(df, "chi2dof", nstd=3.0)
    assert not result["outlier"].any()


def test_reject_outliers_adds_outlier_column_if_missing():
    df = _make_phase_fits_df([1.0, 2.0, 3.0])
    assert "outlier" not in df.columns
    result = reject_outliers(df, "chi2dof")
    assert "outlier" in result.columns


def test_reject_outliers_catches_clustered_bad_tiles_without_masking():
    """Several comparably-bad tiles must not mask each other.

    Regression test for a real pipeline failure: a mean+nstd*std
    threshold is inflated by a cluster of comparably-bad tiles (they drag
    the population mean/std along with them), which can push the
    threshold high enough that none of them cross it -- even though each
    is obviously anomalous next to the many well-behaved tiles. With 20
    good tiles (~1.0) and 4 clustered bad tiles (20.0) here, mean+std
    gives threshold ~= 25.4 (catching nobody); the median/MAD threshold
    stays anchored to the majority-good population and catches all 4.
    """
    lengths = [1.0, 0.95, 1.05, 0.9, 1.1] * 4 + [20.0] * 4
    df = _make_phase_fits_df(lengths)
    result = reject_outliers(df, "chi2dof", nstd=3.0)
    assert result.loc[result["chi2dof"] == 20.0, "outlier"].all()
    assert not result.loc[result["chi2dof"] != 20.0, "outlier"].any()


def test_reject_outliers_does_not_leak_threshold_across_pols():
    """A threshold computed from one pol's population must not flag the other.

    Regression test for a pre-existing bug: the previous implementation
    computed quality_thresh from a pol-specific population but applied it
    via a mask with no pol filter, so XX's threshold could incorrectly
    flag YY rows (and vice versa) if their scales differed enough.
    """
    # XX: tight population around 1.0 (low threshold).
    xx_rows = _make_phase_fits_df([1.0, 0.95, 1.05, 0.9, 1.1], pol="XX")
    # YY: a much larger but internally-consistent population around 15.0
    # -- none of these are outliers within YY's own population, but they
    # would all exceed a threshold derived from XX's tight spread.
    yy_rows = _make_phase_fits_df([15.0, 14.0, 16.0, 13.0, 17.0], pol="YY")
    df = pd.concat([xx_rows, yy_rows], ignore_index=True)
    result = reject_outliers(df, "chi2dof", nstd=3.0)
    assert not result["outlier"].any()


def test_reject_outliers_default_group_cols_matches_pol_only_behaviour():
    """group_cols defaults to ("pol",), preserving pre-flavour-scoping behaviour.

    Regression test for the group_cols parameter's default: an explicit
    group_cols=("pol",) call must produce an identical result to omitting
    it entirely, so existing callers (that don't pass group_cols at all)
    keep behaving exactly as before.
    """
    lengths = [1.0] * 9 + [1000.0]
    df_default = _make_phase_fits_df(lengths)
    df_explicit = _make_phase_fits_df(lengths)
    result_default = reject_outliers(df_default, "chi2dof", nstd=1.0)
    result_explicit = reject_outliers(df_explicit, "chi2dof", group_cols=("pol",), nstd=1.0)
    assert result_default["outlier"].to_list() == result_explicit["outlier"].to_list()


def test_reject_outliers_flavor_scoping_does_not_leak_threshold_across_flavors():
    """A threshold computed from one flavour's population must not flag another.

    Same shape as test_reject_outliers_does_not_leak_threshold_across_pols,
    but for group_cols=("pol", "flavor") -- confirms flavour-scoping is a
    real, independent grouping axis rather than just a relabelling of pol.
    """
    # RRI: tight population around 1.0 (low threshold).
    rri_rows = _make_phase_fits_df([1.0, 0.95, 1.05, 0.9, 1.1], flavor="RRI")
    # SHAO: a much larger but internally-consistent population around 15.0
    # -- none of these are outliers within SHAO's own population, but they
    # would all exceed a threshold derived from RRI's tight spread.
    shao_rows = _make_phase_fits_df([15.0, 14.0, 16.0, 13.0, 17.0], flavor="SHAO")
    df = pd.concat([rri_rows, shao_rows], ignore_index=True)
    result = reject_outliers(df, "chi2dof", group_cols=("pol", "flavor"), nstd=3.0)
    assert not result["outlier"].any()


def test_reject_outliers_flavor_scoping_catches_outlier_within_its_own_flavor():
    """An outlier that's only extreme relative to its own flavour is still caught.

    Mirrors test_reject_outliers_marks_high_value but at group_cols=("pol",
    "flavor") -- confirms flavour-scoping doesn't just loosen detection,
    it also catches tiles that a flavour-blind pooled threshold would miss
    because a larger, noisier flavour's spread dominates the pooled MAD.
    """
    good_rri = _make_phase_fits_df([1.0] * 9, flavor="RRI")
    bad_rri = _make_phase_fits_df([1000.0], flavor="RRI")
    bad_rri["tile_id"] += 100  # avoid colliding tile_id with good_rri
    # A much noisier flavour with many more tiles, which would otherwise
    # dominate a flavour-blind pooled median/MAD and mask the RRI outlier.
    noisy_shao = _make_phase_fits_df([50.0 + i for i in range(30)], flavor="SHAO")
    noisy_shao["tile_id"] += 200
    df = pd.concat([good_rri, bad_rri, noisy_shao], ignore_index=True)
    # nstd=1.0 to match test_reject_outliers_marks_high_value's precedent for
    # this exact 9x1.0+1x1000.0 shape: the single outlier collapses RRI's own
    # MAD to zero (9 of 10 residuals are identical), falling back to a
    # mean+nstd*std threshold, which nstd=3.0 would not cross for this shape.
    result = reject_outliers(df, "chi2dof", group_cols=("pol", "flavor"), nstd=1.0)
    assert result.loc[result["chi2dof"] == 1000.0, "outlier"].all()
    assert not result.loc[(result["flavor"] == "RRI") & (result["chi2dof"] != 1000.0), "outlier"].any()
