"""Per-tile before/after stats table: build_tile_stats_rows and
write_tile_stats_table.

write_before_after_stats() is the HyperfitsSolutionGroup-level entry point
-- split out of the old write_stats_and_debug_plots() so the stats-table
half lives here and the plotting half lives in calvin.plots.phase_fits
(docs/RESTRUCTURE.md Phase 4). Returns group.phase_fits (the final,
annotated phase-fit DataFrame) so callers doing both stats and plots can
pass it straight into write_debug_phase_fit_plots() without recomputing
anything.
"""

import logging

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from mwax_mover.calibration.outliers import annotate_phase_outliers
from mwax_mover.calvin.hyperdrive import ChannelFlagReason, HyperfitsSolutionGroup, TileFlagReason
from mwax_mover.calvin.plots.gains import _channel_reason_counts_text, _format_flavor, _tile_flag_reason_text

logger = logging.getLogger(__name__)


def build_tile_stats_rows(
    group: HyperfitsSolutionGroup,
    jones_snapshot: list[NDArray[np.complex128]],
    tile_bad_mask: NDArray[np.bool_],
    tile_reasons: NDArray[np.object_],
    channel_reasons: list[NDArray[np.object_]],
    phase_fits: pd.DataFrame,
) -> list[dict]:
    """Build one summary row per tile, for the whole observation.

    Used for both the "before" (right after
    HyperfitsSolutionGroup.apply_tile_flags(), before any further flagging)
    and "after" (once the full flagging pipeline has run) snapshots --
    which snapshot it is depends entirely on what's passed in for
    jones_snapshot/tile_bad_mask/tile_reasons/channel_reasons/phase_fits.

    Args:
        group: The solution group (used for tile names/IDs/flavours).
        jones_snapshot: One complex array per file, shape (n_tiles,
            n_chanblocks, 2, 2) -- e.g. group.jones itself (current state)
            or a copy taken at an earlier point.
        tile_bad_mask: Boolean array, shape (n_tiles,). True where the tile
            is fully flagged. Both callers in write_before_after_stats pass
            `<snapshot>_tile_flag_reasons != TileFlagReason.NONE`.
        tile_reasons: A TileFlagReason array, shape (n_tiles,) -- e.g.
            group.tile_flag_reasons at the point of this snapshot (a copy,
            if this isn't the group's current/final state).
        channel_reasons: One array per file, shape (n_tiles, n_chanblocks) --
            e.g. group.channel_flag_reasons at the point of this snapshot.
        phase_fits: Ideally already flavour-merged and outlier-annotated
            phase fits -- i.e. the output of
            calibration.outliers.annotate_phase_outliers, computed against
            the same snapshot (pristine data for "before", final data
            for "after") -- NOT necessarily group.phase_fits, which
            reflects whatever detect_phase_outliers last computed. If
            'outlier' isn't present (e.g. a bare process_phase_fits()
            result), the 'phase_outlier' row field is silently left
            blank -- chi2dof/sigma_resid are read regardless.

    Returns:
        List of one dict per tile, each with keys:
            tile (int): Tile ID.
            name (str): Tile name.
            flavor (str): Formatted receiver flavour (see _format_flavor).
            fully_flagged (bool): Whether the whole tile is flagged.
            flagged_pct (float): Percentage of this tile's channels flagged.
            n_bad_channels (int): Count of flagged channels, across all files.
            n_total_channels (int): Total channel count, across all files.
            gx_min, gx_median, gx_max (float): Gain amplitude stats over
                this tile's still-good gx channels (NaN if fully flagged).
            gy_min, gy_median, gy_max (float): Same, for gy.
            chi2dof_x, chi2dof_y (float): Phase-fit chi2/dof, XX/YY (NaN
                if no phase fit for that pol).
            sigma_resid_x, sigma_resid_y (float): Phase-fit residual std
                dev, XX/YY (NaN if no phase fit for that pol).
            phase_outlier (str): Comma-separated pols ("XX", "YY", or
                "XX,YY") flagged as phase-fit population outliers, or "".
            tile_reason (str): Whole-tile flag reason text, if fully
                flagged (see _tile_flag_reason_text), else "".
            channel_reasons (str): Per-channel flag reason counts, if not
                fully flagged (see _channel_reason_counts_text), else "".
        Matches the columns write_tile_stats_table expects.
    """
    n_tiles = len(group.metafits_tiles_df)
    tile_names = group.metafits_tiles_df["name"].to_numpy()
    tile_ids = group.metafits_tiles_df["id"].to_numpy()
    tile_flavors = group.metafits_tiles_df["flavor"].to_numpy()

    total_channels = np.zeros(n_tiles, dtype=int)
    bad_channels = np.zeros(n_tiles, dtype=int)
    for file_reasons in channel_reasons:
        total_channels += file_reasons.shape[1]
        bad_channels += np.array(
            [np.sum([reason != ChannelFlagReason.NONE for reason in file_reasons[tile]]) for tile in range(n_tiles)]
        )

    phase_indexed = phase_fits.set_index(["tile_id", "pol"]) if len(phase_fits) else None

    rows = []
    for tile in range(n_tiles):
        n_total = int(total_channels[tile])
        if tile_bad_mask[tile]:
            # A whole-tile flag (e.g. METAFITS, MOSTLY_BAD_CHANNELS) NaNs
            # every channel via self.jones directly, without ever
            # touching channel_reasons -- so every channel is bad here
            # regardless of what the per-channel reason count says.
            fully_flagged = True
            n_bad = n_total
        else:
            n_bad = int(bad_channels[tile])
            fully_flagged = n_total > 0 and n_bad == n_total
        flagged_pct = 100 * n_bad / n_total if n_total else 0.0

        row = {
            "tile": int(tile_ids[tile]),
            "name": tile_names[tile],
            "flavor": _format_flavor(tile_flavors[tile]),
            "fully_flagged": fully_flagged,
            "flagged_pct": flagged_pct,
            "n_bad_channels": n_bad,
            "n_total_channels": n_total,
            "gx_min": np.nan,
            "gx_median": np.nan,
            "gx_max": np.nan,
            "gy_min": np.nan,
            "gy_median": np.nan,
            "gy_max": np.nan,
            "chi2dof_x": np.nan,
            "chi2dof_y": np.nan,
            "sigma_resid_x": np.nan,
            "sigma_resid_y": np.nan,
            "phase_outlier": "",
            "tile_reason": _tile_flag_reason_text(tile, tile_reasons) if fully_flagged else "",
            "channel_reasons": "" if fully_flagged else _channel_reason_counts_text(tile, channel_reasons),
        }

        if not fully_flagged:
            # Amplitude stats over this tile's still-good channels only,
            # across every file -- matches the historical
            # build_tile_summary_table's "stats over good channels only"
            # behaviour, needed because a NON_CONVERGED channel's jones
            # value isn't necessarily NaN yet at the "before" snapshot
            # (load() only records the reason, it doesn't NaN the data).
            good_gx, good_gy = [], []
            for file_jones, file_reasons in zip(jones_snapshot, channel_reasons, strict=True):
                good = file_reasons[tile] == ChannelFlagReason.NONE
                good_gx.append(np.abs(file_jones[tile, good, 0, 0]))
                good_gy.append(np.abs(file_jones[tile, good, 1, 1]))
            gx_all = np.concatenate(good_gx) if good_gx else np.array([])
            gy_all = np.concatenate(good_gy) if good_gy else np.array([])
            if gx_all.size and np.any(np.isfinite(gx_all)):
                row["gx_min"] = float(np.nanmin(gx_all))
                row["gx_median"] = float(np.nanmedian(gx_all))
                row["gx_max"] = float(np.nanmax(gx_all))
            if gy_all.size and np.any(np.isfinite(gy_all)):
                row["gy_min"] = float(np.nanmin(gy_all))
                row["gy_median"] = float(np.nanmedian(gy_all))
                row["gy_max"] = float(np.nanmax(gy_all))

        if phase_indexed is not None:
            tile_id = int(tile_ids[tile])
            outlier_pols = []
            try:
                row["chi2dof_x"] = float(phase_indexed.loc[(tile_id, "XX"), "chi2dof"])
                row["sigma_resid_x"] = float(phase_indexed.loc[(tile_id, "XX"), "sigma_resid"])
                if bool(phase_indexed.loc[(tile_id, "XX"), "outlier"]):
                    outlier_pols.append("XX")
            except KeyError:
                pass
            try:
                row["chi2dof_y"] = float(phase_indexed.loc[(tile_id, "YY"), "chi2dof"])
                row["sigma_resid_y"] = float(phase_indexed.loc[(tile_id, "YY"), "sigma_resid"])
                if bool(phase_indexed.loc[(tile_id, "YY"), "outlier"]):
                    outlier_pols.append("YY")
            except KeyError:
                pass
            # Advisory only -- reported here (and in the phase-fit debug
            # plots), never flagged. See HyperfitsSolutionGroup.
            # detect_phase_outliers's docstring for why.
            if outlier_pols:
                row["phase_outlier"] = ",".join(outlier_pols)

        rows.append(row)

    return rows


def write_tile_stats_table(title: str, rows: list[dict], stats_fd) -> None:
    """Write a before/after per-tile stats table to a file-like object.

    Args:
        title: Title for this table (e.g. "BEFORE any changes" or
            "AFTER all flagging").
        rows: Output of build_tile_stats_rows.
        stats_fd: A writable, text-mode file-like object.
    """

    def fmt(value, spec):
        return "--" if value is None or (isinstance(value, float) and np.isnan(value)) else f"{value:{spec}}"

    id_w = 6
    name_w = max(10, max((len(r["name"]) for r in rows), default=10) + 2)
    flavor_w = max(8, max((len(r["flavor"]) for r in rows), default=8) + 2)
    num_w = 8
    phout_w = 9

    header = (
        f"{title}:\n"
        f"{'Tile':<{id_w}} {'Name':<{name_w}} {'Flavor':<{flavor_w}} {'Status':<14} {'Flagged%':>9} "
        f"{'gx_med':>{num_w}} {'gx_min':>{num_w}} {'gx_max':>{num_w}} "
        f"{'gy_med':>{num_w}} {'gy_min':>{num_w}} {'gy_max':>{num_w}} "
        f"{'chi2_x':>{num_w}} {'chi2_y':>{num_w}} {'sres_x':>{num_w}} {'sres_y':>{num_w}} "
        f"{'PhOutlier':>{phout_w}}  Reason(s)"
    )
    stats_fd.write(f"{header}\n")
    stats_fd.write("-" * len(header) + "\n")

    n_bad_total = 0
    n_channels_total = 0

    for r in rows:
        n_bad_total += r["n_bad_channels"]
        n_channels_total += r["n_total_channels"]
        status = "FULLY_FLAGGED" if r["fully_flagged"] else ("PARTIAL" if r["channel_reasons"] else "OK")
        reason = r["tile_reason"] if r["fully_flagged"] else r["channel_reasons"]

        line = (
            f"{r['tile']:<{id_w}} {r['name']:<{name_w}} {r['flavor']:<{flavor_w}} {status:<14} "
            f"{r['flagged_pct']:>8.1f}% "
            f"{fmt(r['gx_median'], f'{num_w}.2f')} {fmt(r['gx_min'], f'{num_w}.2f')} {fmt(r['gx_max'], f'{num_w}.2f')} "
            f"{fmt(r['gy_median'], f'{num_w}.2f')} {fmt(r['gy_min'], f'{num_w}.2f')} {fmt(r['gy_max'], f'{num_w}.2f')} "
            f"{fmt(r['chi2dof_x'], f'{num_w}.3f')} {fmt(r['chi2dof_y'], f'{num_w}.3f')} "
            f"{fmt(r['sigma_resid_x'], f'{num_w}.4f')} {fmt(r['sigma_resid_y'], f'{num_w}.4f')} "
            f"{r['phase_outlier']:>{phout_w}}  {reason}"
        )
        stats_fd.write(f"{line}\n")

    if n_channels_total > 0:
        stats_fd.write(
            f"\nFlagged {n_bad_total}/{n_channels_total} Jones ({100 * n_bad_total / n_channels_total:.2f}%)\n\n"
        )
    else:
        stats_fd.write("\n")


def write_before_after_stats(
    group: HyperfitsSolutionGroup,
    obs_id: int,
    stats_fd,
    phase_outlier_nstd: float = 3.0,
) -> pd.DataFrame:
    """Write the before/after per-tile stats table for the group's final
    flagged state.

    Split out of the old write_stats_and_debug_plots() (see
    calvin.plots.phase_fits.write_debug_phase_fit_plots for the plotting
    half, which needs this function's return value). Must be called after
    HyperfitsSolutionGroup.run_flagging_pipeline() has run -- its
    before_jones/before_tile_flag_reasons/before_channel_flag_reasons/
    before_phase_fits/phase_fits attributes are all required here.

    The "after" phase fit is not recomputed here: group.phase_fits is
    already the final, fully-cleaned, flavour/outlier-annotated state,
    because run_flagging_pipeline() runs detect_phase_outliers() last for
    exactly this reason (see its docstring). This function reuses it
    directly for build_tile_stats_rows's AFTER row (the stats.txt AFTER
    row's Flavor/PhOutlier columns) -- and since phase fitting costs real
    time (roughly 2 minutes for a 256-tile observation in testing),
    doesn't pay for a second fit of the same data. Only the "before" phase
    fit is annotated here, since it's a genuinely different (deliberately
    unflagged) snapshot that nothing else computes.

    Args:
        group: The solution group, after run_flagging_pipeline() (and
            typically commit()) have run.
        obs_id: The observation ID, used for section titles.
        stats_fd: Open file descriptor to write the before/after per-tile
            stats table into. Callers write this as the first section of
            the combined {obs_id}_stats.txt file, with
            calvin.hyperdrive.write_hyperdrive_stats() convergence stats
            appended below.
        phase_outlier_nstd: Number of (MAD-derived) standard-deviation-
            equivalents beyond the population's robust median before a
            tile's phase fit is reported as an outlier -- only affects the
            BEFORE table's annotation now (the AFTER table reuses
            group.phase_fits, already annotated with whatever nstd
            run_flagging_pipeline was given). Pass the same value to both
            this and write_debug_phase_fit_plots, or the BEFORE and AFTER
            sections will silently reflect two different thresholds.
            Purely advisory -- does not affect flagging either way.

    Returns:
        group.phase_fits (the final, annotated phase fit DataFrame), so
        callers that also need it (e.g. for a DB insert, or to pass into
        write_debug_phase_fit_plots) don't have to recompute it.
    """
    assert group.before_jones is not None
    assert group.before_tile_flag_reasons is not None
    assert group.before_channel_flag_reasons is not None
    assert group.before_phase_fits is not None
    assert group.jones is not None
    assert group.tile_flag_reasons is not None
    assert group.channel_flag_reasons is not None
    assert group.phase_fits is not None

    tiles = group.metafits_tiles_df
    annotated_before_phase_fits = annotate_phase_outliers(group.before_phase_fits, tiles, nstd=phase_outlier_nstd)
    final_phase_fits = group.phase_fits

    before_tile_bad_mask = group.before_tile_flag_reasons != TileFlagReason.NONE
    after_tile_bad_mask = group.tile_flag_reasons != TileFlagReason.NONE

    before_rows = build_tile_stats_rows(
        group,
        group.before_jones,
        before_tile_bad_mask,
        group.before_tile_flag_reasons,
        group.before_channel_flag_reasons,
        annotated_before_phase_fits,
    )
    write_tile_stats_table(f"{obs_id}: BEFORE any changes (unchanged hyperdrive solutions file)", before_rows, stats_fd)

    after_rows = build_tile_stats_rows(
        group,
        group.jones,
        after_tile_bad_mask,
        group.tile_flag_reasons,
        group.channel_flag_reasons,
        final_phase_fits,
    )
    write_tile_stats_table(f"{obs_id}: AFTER all Calvin flagging", after_rows, stats_fd)

    return final_phase_fits
