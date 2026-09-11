"""Robust outlier detection and rejection for calibration fit quality metrics.

reject_outliers() is the core robust (MAD-based), iteratively-refined
per-group threshold test. annotate_phase_outliers() is the single shared
definition of "phase outlier" used everywhere in the Calvin pipeline
(both calvin.hyperdrive's reporting-only detection and
calvin.plots.stats_table/phase_fits's stats/debug plots route through it,
so the threshold can never silently disagree between the two).
iterative_poly_clip_batch() fits a robust, sigma-clipped polynomial
(batched across tiles) and flags outliers.
"""

import numpy as np
import pandas as pd

from mwax_mover.calibration.df_columns import (
    COL_CHI2DOF,
    COL_FLAVOR,
    COL_OUTLIER,
    COL_POL,
    COL_SIGMA_RESID,
    COL_SOLN_IDX,
    COL_TILE_ID,
    COL_XX,
    COL_YY,
)
from mwax_mover.constants import MAD_TO_STD_SCALE_FACTOR


def iterative_poly_clip_batch(
    x: np.ndarray,
    Y: np.ndarray,
    degree: int,
    residual_threshold: float,
    initial_valid: np.ndarray,
    max_iter: int = 10,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Fit a robust, sigma-clipped polynomial to every row (tile) of Y at once.

    Iteratively fits a degree-N polynomial per tile on that tile's
    currently-valid points (against a design matrix shared across all
    tiles, since every tile shares the same x-grid), computes residuals
    against that fit, rejects points whose residual exceeds
    residual_threshold MADs (median absolute deviations) from the tile's
    median residual, and refits -- repeating per tile until its valid set
    reaches a fixed point (new_valid == valid) or max_iter is reached.
    This guards against a single extreme outlier dragging a one-shot
    least-squares fit far enough off course that it masks the very
    outlier it should catch.

    A tile with fewer than degree + 2 initially-valid points is never fit
    at all and keeps its original valid mask -- too few points to fit a
    degree-N polynomial meaningfully. A tile whose residual MAD is
    (numerically) zero -- see the zero_mad tolerance comment below -- is
    treated as a perfect fit: its residual is set to 0.0 everywhere and
    it stops iterating early.

    Batches every tile's fit in one pass instead of looping and calling
    np.polyfit per tile: replaces what would otherwise be up to
    (n_tiles * max_iter) separate np.polyfit/np.polyval calls with a
    handful of batched numpy operations per outer iteration. Since every
    tile shares the same x-grid (chanblock index), a per-tile weighted
    least-squares fit is just a per-tile (degree+1)x(degree+1) normal-
    equations solve against a shared design matrix, which batches
    trivially across tiles via einsum + batched np.linalg.solve, instead
    of paying np.polyfit's (comparatively large) fixed per-call overhead
    thousands of times over. This is also why
    docs/img/make_illustrations.py uses this function rather than a
    per-tile loop -- so the illustrations show what the pipeline actually
    does.

    Args:
        x: 1D array of independent variable values (e.g. chanblock
            index), shape (n_chan,), shared across all tiles.
        Y: 2D array of dependent variable values, shape (n_tiles, n_chan).
        degree: Polynomial degree to fit.
        residual_threshold: Number of residual-MADs beyond which a point
            is considered an outlier.
        initial_valid: Boolean array, shape (n_tiles, n_chan), of points
            eligible to be fit at all per tile.
        max_iter: Maximum number of fit/clip iterations.

    Returns:
        A tuple (valid, residual, fit, mad, med), each with a leading
        tile axis:
        - valid: Boolean array, shape (n_tiles, n_chan), True for points
          considered good (within initial_valid and not rejected as an
          outlier) per tile.
        - residual: Float array, shape (n_tiles, n_chan), |y - fit -
          median_residual| / mad at every point per tile (including
          points outside initial_valid, computed against that tile's
          final fit). NaN wherever the tile's Y is NaN, or everywhere for
          a tile whose fit could never be computed (too few valid
          points).
        - fit: Float array, shape (n_tiles, n_chan), each tile's final
          polynomial fit evaluated at every x. NaN everywhere for a tile
          whose fit could not be computed at all.
        - mad: Float array, shape (n_tiles,), the median absolute
          deviation of residuals from each tile's final fit iteration
          (same units as Y). NaN for a tile whose fit could not be
          computed.
        - med: Float array, shape (n_tiles,), the median residual from
          each tile's final fit iteration (same units as Y). NaN for a
          tile whose fit could not be computed.
    """
    n_tiles, n = Y.shape
    valid = initial_valid.copy()
    residual = np.full((n_tiles, n), np.nan, dtype=np.float64)
    fit = np.full((n_tiles, n), np.nan, dtype=np.float64)
    mad = np.full(n_tiles, np.nan, dtype=np.float64)
    med = np.full(n_tiles, np.nan, dtype=np.float64)

    min_points = degree + 2
    # Mirrors the per-tile early return: a tile with too few initially-
    # valid points is never fit at all, and keeps its original valid mask.
    done = valid.sum(axis=1) < min_points

    # Shared design matrix (every tile has the same x-grid).
    design = np.vander(x, degree + 1, increasing=True)  # (n, degree+1)

    for _ in range(max_iter):
        active = np.where(~done)[0]
        if len(active) == 0:
            break

        weights = valid[active].astype(np.float64)  # (n_active, n)
        # Y can contain NaN at invalid positions (e.g. a pre-existing-NaN
        # Jones entry) -- weight=0 there doesn't zero out a NaN
        # (0 * nan == nan), so replace those entries before weighting.
        # The per-tile version never has this problem since it indexes
        # y[valid] directly, never touching the invalid entries at all.
        y_true = Y[active]
        y_for_fit = np.where(weights > 0, y_true, 0.0)

        # Batched normal equations: A[t] = designᵗ diag(weights[t]) design;
        # b[t] = designᵗ diag(weights[t]) y_for_fit[t]. Exact for a
        # weighted least-squares fit against the shared design matrix --
        # same answer np.polyfit(x[valid], y[valid], degree) would give.
        gram = np.einsum("tk,ki,kj->tij", weights, design, design)
        rhs = (weights * y_for_fit) @ design  # (n_active, degree+1)

        try:
            coeffs = np.linalg.solve(gram, rhs[..., np.newaxis])[..., 0]
        except np.linalg.LinAlgError:
            # Extremely unlikely given the min_points guard above (would
            # need a degenerate x-distribution among the valid points),
            # but fall back tile-by-tile rather than losing the whole
            # batch if it ever happens.
            coeffs = np.full((len(active), degree + 1), np.nan)
            for i in range(len(active)):
                try:
                    coeffs[i] = np.linalg.solve(gram[i], rhs[i])
                except np.linalg.LinAlgError:
                    pass

        fit_active = coeffs @ design.T  # (n_active, n)
        # Residuals use the TRUE y, not y_for_fit -- a point temporarily
        # excluded this round (weight=0, but still within initial_valid,
        # i.e. not NaN) must be re-evaluated against its real value each
        # iteration so it can rejoin if the updated fit now passes it.
        # Using the zeroed y_for_fit here instead would compare a fake
        # zero against the fit for every currently-excluded point,
        # corrupting exactly the re-inclusion check the iteration depends
        # on. Genuinely-NaN positions still propagate NaN here, same as
        # the per-tile version's resid_all = y - fit.
        resid_all = y_true - fit_active

        # Per-tile median/MAD over that tile's currently-valid points only.
        masked_resid = np.where(weights > 0, resid_all, np.nan)
        med_active = np.nanmedian(masked_resid, axis=1)
        mad_active = np.nanmedian(np.abs(masked_resid - med_active[:, None]), axis=1)

        fit[active] = fit_active
        med[active] = med_active
        mad[active] = mad_active

        # A tolerance, not exact equality: a genuinely (near-)perfect fit
        # can land at a different tiny floating-point residue (~1e-15 to
        # 1e-16) depending on the numerical method used -- np.polyfit's
        # SVD-based approach per-tile vs. this function's batched normal-
        # equations solve are mathematically equivalent but not
        # bit-identical. Dividing by an almost-but-not-exactly-zero MAD
        # would otherwise amplify that noise unpredictably in `residual`
        # below. 1e-9 is far below any real measurement noise in gain
        # amplitude data (realistically ~1e-2 to 1e0 in these units).
        zero_mad = mad_active < 1e-9
        if zero_mad.any():
            zero_idx = active[zero_mad]
            residual[zero_idx] = 0.0
            done[zero_idx] = True

        nonzero = ~zero_mad
        if nonzero.any():
            nz_idx = active[nonzero]
            residual_nz = np.abs(resid_all[nonzero] - med_active[nonzero, None]) / mad_active[nonzero, None]
            residual[nz_idx] = residual_nz

            new_valid_nz = initial_valid[nz_idx] & (residual_nz <= residual_threshold)
            too_few = new_valid_nz.sum(axis=1) < min_points
            done[nz_idx[too_few]] = True

            keep_going = ~too_few
            kg_idx = nz_idx[keep_going]
            new_valid_kg = new_valid_nz[keep_going]
            unchanged = np.all(new_valid_kg == valid[kg_idx], axis=1)

            # Apply new_valid regardless of whether it changed -- valid
            # must reflect the latest clip either way; only the stopping
            # decision (done) differs between the "unchanged" and
            # "keep going" cases.
            valid[kg_idx] = new_valid_kg
            done[kg_idx[unchanged]] = True

    # Y positions that were never valid may have been zeroed internally
    # above to avoid 0*NaN propagation; the per-tile version's residual
    # is NaN wherever the original Y is NaN (resid_all = y - fit, and y
    # itself is NaN there), so match that here even though nothing
    # currently consumes this field.
    residual = np.where(np.isnan(Y), np.nan, residual)

    return valid, residual, fit, mad, med


def reject_outliers(data, quality_key, group_cols=(COL_POL,), nstd=3.0, max_iter=10):
    """Mark outliers in a DataFrame based on a quality metric.

    Uses a robust, iteratively-refined threshold per group (see
    group_cols): threshold = median + nstd * 1.4826 * MAD, computed from
    that group's not-yet-flagged rows, with newly-flagged rows removed
    from the population before recomputing the threshold and repeating
    (until nothing new is flagged, or max_iter is reached).

    This replaces a single-pass mean + nstd*std threshold, which is
    vulnerable to masking (aka swamping): if several rows are comparably
    bad, they inflate the population mean/std together, raising the
    threshold enough that only the single most extreme one crosses it
    while the rest hide beneath it. A robust median/MAD centre and scale
    resists being dragged by the very outliers it's meant to catch, and
    iterating lets the threshold tighten again each time an outlier is
    set aside, so a cluster of comparably-bad rows gets caught round by
    round instead of masking each other. Mirrors the median/MAD +
    iterative-clip approach already used by iterative_poly_clip_batch for
    amplitude-outlier detection.

    Also fixes a pre-existing bug: the previous implementation computed
    quality_thresh from a `pol`-specific population but then applied it
    via a mask with no `pol` filter, so a threshold derived from one
    polarisation's population could incorrectly flag rows of the other.
    Flagging is now scoped to the current group throughout.

    Grouping only by `pol` (the default, and the only behaviour before
    group_cols was added) pools every tile of every receiver flavour into
    one population per polarisation before thresholding. On real MWA
    observations, different receiver flavours (e.g. RRI/SHAO/NI) have
    measurably different natural chi2dof/sigma_resid distributions even
    after each tile's own cable delay is fit out -- so pooling them
    together lets whichever flavour has the most tiles set a threshold
    that's too strict for a naturally-noisier minority flavour
    (over-flagging it) and too lenient for a naturally-tighter one
    (under-flagging it). Passing group_cols=("pol", "flavor") scopes the
    threshold to each flavour's own population instead. See CALVIN.md's
    "Phase-outlier detection" section for a worked example on a real
    observation.

    Args:
        data: Input DataFrame with the columns named in group_cols, plus
            the quality column.
        quality_key: Name of the column to use for outlier detection.
        group_cols: Column name(s) defining the population each row is
            compared against -- a separate threshold is computed and
            applied independently per unique combination of these
            columns' values (default: ("pol",), i.e. one threshold per
            polarisation, matching this function's original behaviour).
        nstd: Number of (MAD-derived, approximately Gaussian-equivalent)
            standard deviations beyond the population median before a
            row is an outlier (default: 3.0). Negative flags low
            outliers instead of high ones.
        max_iter: Maximum number of threshold/clip iterations per group.

    Returns:
        DataFrame with an 'outlier' column added/updated marking outliers.
    """
    if nstd == 0:
        return data
    if COL_OUTLIER not in data.columns:
        data[COL_OUTLIER] = False

    # Scales a normal-distribution MAD to be comparable to a standard
    # deviation, so nstd keeps roughly the same meaning as the previous
    # mean+nstd*std threshold for a population with few/no outliers.
    mad_to_std = MAD_TO_STD_SCALE_FACTOR

    quality_values = data[quality_key].to_numpy()
    outlier_values = data[COL_OUTLIER].to_numpy().copy()

    # A single string key per row, combining every group_cols value --
    # lets the loop below treat any number of grouping columns the same
    # way it previously treated just "pol", with one iteration per unique
    # combination rather than one nested loop per column.
    group_cols = list(group_cols)
    group_key = data[group_cols].astype(str).agg("|".join, axis=1).to_numpy()

    for grp in np.unique(group_key):
        grp_mask = group_key == grp

        for _ in range(max_iter):
            idx_grp_good = np.where(grp_mask & ~outlier_values)[0]
            if len(idx_grp_good) == 0:
                break

            grp_values = quality_values[idx_grp_good]
            grp_median = np.median(grp_values)
            grp_mad = np.median(np.abs(grp_values - grp_median))

            if grp_mad == 0 or np.isnan(grp_mad):
                if np.ptp(grp_values) == 0:
                    # Truly zero spread across this group's currently-good
                    # population (or too few points to compute a
                    # meaningful spread, e.g. a single row) -- nothing
                    # stands out, so stop iterating for this group.
                    # Without this guard, zero spread gives threshold ==
                    # median, and ">=" trivially flags every remaining
                    # row via equality -- the opposite of correct
                    # behaviour.
                    break
                # MAD's ~50% breakdown point means a minority of extreme
                # values can collapse it to zero even though real spread
                # exists (e.g. 9 identical values + 1 extreme one: the
                # median residual is 0 for the majority, so is the MAD).
                # Fall back to a mean+std threshold for this round only,
                # as a safety net -- std isn't fooled by a minority
                # outlier the way MAD's breakdown point is here.
                grp_mean = np.mean(grp_values)
                grp_std = np.std(grp_values, ddof=1)
                if grp_std == 0:
                    break
                quality_thresh = grp_mean + nstd * grp_std
            else:
                quality_thresh = grp_median + nstd * mad_to_std * grp_mad

            if nstd >= 0:
                newly_bad = grp_mask & ~outlier_values & (quality_values >= quality_thresh)
            else:
                newly_bad = grp_mask & ~outlier_values & (quality_values <= quality_thresh)

            if not newly_bad.any():
                break
            outlier_values[newly_bad] = True

    data[COL_OUTLIER] = outlier_values
    return data


def annotate_phase_outliers(
    phase_fits: pd.DataFrame,
    tiles: pd.DataFrame,
    nstd: float = 3.0,
) -> pd.DataFrame:
    """Merge tile metadata into a phase-fits DataFrame and mark population outliers.

    Merges tiles (e.g. HyperfitsSolutionGroup.metafits_tiles_df or
    Metafits.tiles_df -- anything with 'id' and 'flavor' columns) into
    phase_fits on tile_id/id, then scopes reject_outliers's
    population-outlier test to (pol, flavor) groups, on chi2dof then
    sigma_resid, sequentially.

    This is the single, shared definition of "phase outlier" used
    everywhere in the Calvin pipeline: HyperfitsSolutionGroup.
    detect_phase_outliers (which only reports the result -- see its
    docstring for why phase outliers are no longer flagged or modified),
    and calvin.plots.stats_table.write_before_after_stats (which feeds the
    same annotated DataFrame to both the stats.txt Flavor/PhOutlier
    columns and, via calvin.plots.phases.write_debug_phase_fit_plots,
    the phase-fit debug plots). Routing every caller through
    one function keeps that definition consistent -- previously the
    plotting path independently recomputed this with a hardcoded nstd,
    which could silently disagree with the actual detection threshold.

    Args:
        phase_fits: DataFrame from process_phase_fits (or a snapshot of
            it), with columns tile_id/soln_idx/pol/chi2dof/sigma_resid/etc.
        tiles: DataFrame with tile metadata, including 'id' and 'flavor'.
        nstd: Number of (MAD-derived) standard deviations beyond each
            (pol, flavor) population's robust centre before a tile's fit
            is an outlier on that metric (default: 3.0). See
            reject_outliers.

    Returns:
        phase_fits merged with tiles (on tile_id/id) and with an
        'outlier' column marking population-outlier rows.
    """
    merged = phase_fits.merge(tiles, left_on=COL_TILE_ID, right_on="id", how="left")
    merged = reject_outliers(merged, COL_CHI2DOF, group_cols=(COL_POL, COL_FLAVOR), nstd=nstd)
    merged = reject_outliers(merged, COL_SIGMA_RESID, group_cols=(COL_POL, COL_FLAVOR), nstd=nstd)
    return merged


def pivot_phase_fits(
    phase_fits: pd.DataFrame,
    tiles: pd.DataFrame,
) -> pd.DataFrame:
    """Pivot per-polarization phase fits to per-tile format.

    Args:
        phase_fits: DataFrame with phase fits per tile and polarization.
        tiles: DataFrame with tile metadata.

    Returns:
        Pivoted DataFrame with fits separated into XX and YY columns.
    """
    phase_fits = pd.merge(
        phase_fits[phase_fits[COL_POL] == COL_XX].drop(columns=[COL_POL]),
        phase_fits[phase_fits[COL_POL] == COL_YY].drop(columns=[COL_POL, COL_SOLN_IDX]),
        on=[COL_TILE_ID],
        suffixes=["_xx", "_yy"],
    )
    phase_fits = pd.merge(phase_fits, tiles, left_on=COL_TILE_ID, right_on="id")
    phase_fits.drop("id", axis=1, inplace=True)
    tile_columns = [COL_SOLN_IDX, "name", COL_TILE_ID, "rx", "slot", COL_FLAVOR]
    tile_columns += [*(set(tiles.columns) - set(tile_columns) - {"id"})]
    fit_columns = [column for column in phase_fits.columns if column not in tile_columns]
    fit_columns.sort()
    phase_fits = pd.concat([phase_fits[tile_columns], phase_fits[fit_columns]], axis=1)
    return phase_fits
