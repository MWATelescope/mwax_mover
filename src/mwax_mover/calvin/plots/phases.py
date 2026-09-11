"""Phase-fit diagnostic plots: intercepts, residuals, and per-tile fits.

plot_debug_phase_fits() is the entry point, producing plots and TSV files
for phase fit intercepts, residuals, and RX lengths from an
already-annotated (see calibration.outliers.annotate_phase_outliers)
phase-fits DataFrame. write_debug_phase_fit_plots() is the
HyperfitsSolutionGroup-level wrapper -- split out of the old
write_stats_and_debug_plots() so the plotting half lives here and the
stats-table half lives in calvin.plots.stats_table (docs/RESTRUCTURE.md
Phase 4).
"""

import logging
import os
import textwrap
import warnings

import matplotlib as mpl

# This is a batch/server-side pipeline that never displays a figure
# interactively -- only ever saves to file. Force the headless Agg
# backend explicitly (must happen before pyplot's first import, which is
# when backend selection is locked in) rather than letting matplotlib
# resolve to whatever interactive backend happens to be available (e.g.
# TkAgg), which wastes real time on GUI-toolkit overhead for every figure.
mpl.use("Agg")

import numpy as np
import pandas as pd
import seaborn as sns
from astropy import units as u
from astropy.constants import c  # ty: ignore[unresolved-import]
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from numpy.typing import NDArray

from mwax_mover.calibration.df_columns import (
    COL_CHI2DOF,
    COL_FLAVOR,
    COL_LENGTH,
    COL_OUTLIER,
    COL_POL,
    COL_QUALITY,
    COL_SIGMA_RESID,
    COL_SOLN_IDX,
    COL_TILE_ID,
    COL_XX,
    COL_YY,
)
from mwax_mover.calibration.fitting import ensure_system_byte_order, poly_str, wrap_angle
from mwax_mover.calibration.outliers import pivot_phase_fits
from mwax_mover.calvin.hyperfits_solution_group import HyperfitsSolutionGroup
from mwax_mover.calvin.plots.layout import resolve_plot_dpi, scale_plot_figsize
from mwax_mover.constants import MAD_TO_STD_SCALE_FACTOR

logger = logging.getLogger(__name__)


def plot_debug_phase_fits(
    phase_fits: pd.DataFrame,
    tiles: pd.DataFrame,
    freqs: NDArray[np.float64],
    soln_xx: NDArray[np.complex128],
    soln_yy: NDArray[np.complex128],
    weights: NDArray[np.float64],
    prefix: str = "./",
    show: bool = False,
    title: str = "",
    plot_residual: bool = False,
    residual_vmax=None,
    phase_outlier_nstd: float = 3.0,
) -> pd.DataFrame | None:
    """Generate debug plots and analysis for phase fits.

    Produces plots and TSV files for phase fit intercepts, residuals, and RX lengths,
    and returns a pivoted dataframe with per-antenna fit information.

    Args:
        phase_fits: Already flavour-merged and outlier-annotated phase
            fits -- i.e. the output of
            calibration.outliers.annotate_phase_outliers, not a bare
            process_phase_fits() result. Must include 'flavor' and
            'outlier' columns (a bare process_phase_fits() result will
            raise a KeyError). Callers needing "the" outlier verdict
            should compute it once via annotate_phase_outliers and reuse
            it here and for the stats table (see
            calvin.plots.stats_table.write_before_after_stats), so the
            two reports can't disagree with each other.
        tiles: DataFrame with tile metadata, passed through unchanged to
            pivot_phase_fits()'s own (separate) merge below.
        freqs: Array of frequency values in Hz.
        soln_xx: XX polarization solutions.
        soln_yy: YY polarization solutions.
        weights: Weight values for each frequency channel.
        prefix: Output directory prefix for saving plots (default: './').
        show: Whether to display plots (default: False).
        title: Title for plots (default: '').
        plot_residual: Whether to plot residuals (default: False).
        residual_vmax: Maximum value for residual plot y-axis (default: None).
        phase_outlier_nstd: Must match the nstd used to produce
            phase_fits's 'outlier' column -- passed through to
            plot_phase_residual so its shaded outlier-range band reflects
            the actual reporting threshold (default: 3.0).

    Returns:
        Pivoted DataFrame with combined fit data, or None if no valid fits.
    """
    n_total = len(phase_fits)
    if n_total == 0:
        return None

    flavor_fits = phase_fits

    n_good = len(flavor_fits[~flavor_fits[COL_OUTLIER]])
    if n_good == 0:
        return None

    bad_fits = flavor_fits[flavor_fits[COL_OUTLIER]]
    if len(bad_fits) > 0:
        logger.debug(f"{len(bad_fits)} of {n_total} fits are phase-outliers (reported only, not flagged):")
        logger.debug(bad_fits[["name", COL_POL]].to_string(index=False))

    # make a new colormap for weighted data
    half_blues = LinearSegmentedColormap.from_list(
        colors=mpl.colormaps["Blues"](np.linspace(0.5, 1, 256)),
        name="HalfBlues",
    )

    if len(flavor_fits):
        _rx_means = plot_rx_lengths(flavor_fits, prefix, show, title)

    freqs = ensure_system_byte_order(freqs)
    weights = ensure_system_byte_order(weights)
    soln_xx = ensure_system_byte_order(soln_xx)
    soln_yy = ensure_system_byte_order(soln_yy)

    if plot_residual:
        plot_phase_residual(
            freqs,
            soln_xx,
            soln_yy,
            weights,
            prefix,
            title,
            residual_vmax,
            flavor_fits,
            nstd=phase_outlier_nstd,
        )
    if len(flavor_fits):
        plot_phase_intercepts(prefix, show, title, flavor_fits)

    # pivot_phase_fits() does its own tiles merge -- pass it only the
    # plain phase-fit columns (not the tile-metadata/outlier columns
    # already merged in above), to avoid duplicate-column collisions
    # with that merge.
    plain_columns = [
        COL_TILE_ID,
        COL_SOLN_IDX,
        COL_POL,
        COL_LENGTH,
        "intercept",
        COL_SIGMA_RESID,
        COL_CHI2DOF,
        COL_QUALITY,
        "stderr",
        COL_OUTLIER,
    ]
    phase_fits_pivot = pivot_phase_fits(phase_fits[plain_columns], tiles)
    weights2 = weights**2

    if prefix:
        phase_fits_pivot.to_csv(f"{prefix}phase_fits.tsv", sep="\t", index=False)

    if len(phase_fits_pivot):
        plot_phase_fits(
            freqs,
            soln_xx,
            soln_yy,
            prefix,
            show,
            title,
            half_blues,
            phase_fits_pivot,
            weights2,
        )

    return phase_fits_pivot


def plot_rx_lengths(flavor_fits, prefix, show, title):
    """Plot and save cable length distribution by receiver.

    Args:
        flavor_fits: DataFrame with fit results per receiver.
        prefix: Output directory prefix for saving plot.
        show: Whether to display the plot.
        title: Title for the plot.

    Returns:
        Series with mean cable lengths per receiver.
    """
    good_fits = flavor_fits[~flavor_fits[COL_OUTLIER]]
    rxs = sorted(good_fits["rx"].unique())
    means = good_fits.groupby(["rx"])[COL_LENGTH].mean()

    plt.clf()
    box_plot = sns.boxplot(data=good_fits, y="rx", x=COL_LENGTH, hue=COL_POL, orient="h", fliersize=0.5)
    box_plot.grid(axis="x")
    x_text = np.max(box_plot.get_xlim())

    for ytick in box_plot.get_yticks():
        rx = rxs[ytick]
        mean = means[rx]
        box_plot.text(
            x_text,
            ytick,
            f"rx{rx:02} = {mean:+6.2f}m",
            horizontalalignment="left",
            weight="semibold",
            fontfamily="monospace",
        )
        box_plot.add_line(plt.Line2D([mean, mean], [ytick - 0.5, ytick + 0.5], color="red", linewidth=1))

    fig = plt.gcf()
    if title:
        fig.suptitle(title)
    if show:
        plt.show()
    if prefix:
        plt.tight_layout()
        fig.savefig(f"{prefix}rx_lengths.png", dpi=resolve_plot_dpi(300), bbox_inches="tight")

    return means


def plot_phase_fits(freqs, soln_xx, soln_yy, prefix, show, title, cmap, phase_fits_pivot, weights2):
    """Plot phase fits for XX and YY polarizations.

    Args:
        freqs: Array of frequency values.
        soln_xx: XX polarization solutions.
        soln_yy: YY polarization solutions.
        prefix: Output directory prefix for saving plots.
        show: Whether to display plots.
        title: Title for plots.
        cmap: Colormap for weighted data.
        phase_fits_pivot: DataFrame with pivoted phase fit results.
        weights2: Squared weight values.
    """
    rxs = np.sort(np.unique(phase_fits_pivot["rx"]))
    slots = np.sort(np.unique(phase_fits_pivot["slot"]))
    figsize = scale_plot_figsize(float(np.clip(len(slots) * 2.5, 5, 20)), float(np.clip(len(rxs) * 3, 5, 30)))

    for pol, soln in zip(["xx", "yy"], [soln_xx, soln_yy], strict=True):
        plt.clf()
        fig, axs = plt.subplots(len(rxs), len(slots), sharex=True, sharey="row", squeeze=True)
        # rest of the code assumes axs is 2D array
        if len(rxs) == 1 and len(slots) == 1:
            axs = np.array([[axs]])
        elif len(rxs) == 1:
            axs = axs[np.newaxis, :]
        elif len(slots) == 1:
            axs = axs[:, np.newaxis]

        for ax in axs.flatten():
            ax.axis("off")
        for _, fit in phase_fits_pivot.iterrows():
            signal = soln[fit[COL_SOLN_IDX]]
            if fit["flag"] or np.isnan(signal).all():
                continue
            mask = np.where(np.logical_and(np.isfinite(signal), weights2 > 0))[0]
            angle = np.angle(signal)
            mask_freq: np.ndarray = freqs[mask]
            model_freqs = np.linspace(mask_freq.min(), mask_freq.max(), len(freqs))
            rx_idx = np.where(rxs == fit["rx"])[0][0]
            slot_idx = np.where(slots == fit["slot"])[0][0]
            ax = axs[rx_idx][slot_idx]
            ax.axis("on")
            gradient = (2 * np.pi * u.rad * (fit[f"length_{pol}"] * u.m) / c).to(u.rad / u.Hz).value
            intercept = fit[f"intercept_{pol}"]
            model = gradient * model_freqs + intercept
            ax.scatter(model_freqs, wrap_angle(model), c="red", s=0.5)
            mask_weights = weights2[mask]
            ax.scatter(mask_freq, wrap_angle(angle[mask]), c=mask_weights, cmap=cmap, s=2)
            outlier = fit[f"outlier_{pol}"]
            color = "red" if outlier else "black"
            ax.set_title(
                f"{fit['name']}|{fit['soln_idx']}",
                color=color,
                weight="semibold",
                fontfamily="monospace",
            )
            x_text = np.mean(ax.get_xlim())
            y_text = np.mean(ax.get_ylim())
            text = "\n".join(
                [
                    f"L{fit[f'length_{pol}']:+6.2f}m",
                    f"X{fit[f'chi2dof_{pol}']:.4f}",
                ]
            )
            ax.text(
                x_text,
                y_text,
                text,
                ha="center",
                va="center",
                zorder=10,
                horizontalalignment="left",
                weight="semibold",
                fontfamily="monospace",
                color=color,
                backgroundcolor=("white", 0.5),
            )

        fig.set_size_inches(*figsize)
        if title:
            fig.suptitle(title)
            fig.subplots_adjust(top=0.88)
        if show:
            plt.show()
        if prefix:
            plt.tight_layout()
            fig.savefig(f"{prefix}phase_fits_{pol}.png", dpi=resolve_plot_dpi(300), bbox_inches="tight")


def plot_phase_intercepts(prefix, show, title, flavor_fits):
    """Plot phase intercepts in polar coordinates.

    Rows are ordered alphabetically by receiver flavour, columns as XX
    then YY, regardless of the order flavours/pols happen to appear in
    flavor_fits.

    Args:
        prefix: Output directory prefix for saving plot.
        show: Whether to display the plot.
        title: Title for the plot.
        flavor_fits: DataFrame with phase fit results.
    """
    plt.clf()
    g = sns.FacetGrid(
        flavor_fits,
        row=COL_FLAVOR,
        col=COL_POL,
        hue=COL_FLAVOR,
        row_order=sorted(flavor_fits[COL_FLAVOR].unique()),
        col_order=[COL_XX, COL_YY],
        subplot_kws={"projection": "polar"},
        sharex=False,
        sharey=False,
        despine=False,
    )
    g.map(
        (lambda theta, r, size, **kwargs: plt.scatter(x=theta, y=r, s=10 / (0.1 + size), **kwargs)),
        "intercept",
        COL_LENGTH,
        COL_SIGMA_RESID,
    )
    fig = plt.gcf()
    if title:
        fig.suptitle(title)
        fig.subplots_adjust(top=0.95)
    if show:
        plt.show()
    if prefix:
        plt.tight_layout()
        fig.savefig(f"{prefix}intercepts.png", dpi=resolve_plot_dpi(300), bbox_inches="tight")


def plot_phase_residual(
    freqs,
    soln_xx,
    soln_yy,
    weights,
    prefix,
    title,
    residual_vmax,
    flavor_fits,
    nstd=3.0,
):
    """Plot and analyze phase residuals across frequencies.

    Args:
        freqs: Array of frequency values in Hz.
        soln_xx: XX polarization solutions.
        soln_yy: YY polarization solutions.
        weights: Weight values for each frequency.
        prefix: Output directory prefix for saving plots and data.
        title: Title for plots.
        residual_vmax: Maximum value for residual plot y-axis.
        flavor_fits: DataFrame with phase fit results per receiver
            flavor, already annotated with an 'outlier' column (see
            calibration.outliers.annotate_phase_outliers).
        nstd: Number of (MAD-derived) standard deviations used for the
            shaded outlier-range band on each facet -- must match the
            nstd that produced flavor_fits's 'outlier' column, or the
            band drawn here won't reflect the actual reporting threshold
            (default: 3.0, matching reject_outliers's own default).

    Rows are ordered alphabetically by receiver flavour, columns as XX
    then YY, matching plot_phase_intercepts. XX and YY share the same
    y-axis scale (and therefore the same tick decimal formatting) within
    each flavour row, so the two columns are directly comparable -- but
    different flavour rows are not forced to share a scale with each
    other, since their typical residual magnitudes can genuinely differ
    (see Step 6's rationale in CALVIN.md).
    """
    plt.clf()
    g = sns.FacetGrid(
        flavor_fits,
        row=COL_FLAVOR,
        col=COL_POL,
        hue=COL_FLAVOR,
        row_order=sorted(flavor_fits[COL_FLAVOR].unique()),
        col_order=[COL_XX, COL_YY],
        sharex=True,
        sharey="row",
    )
    # sharey="row" ties XX/YY's y-limits (and therefore tick values/decimal
    # formatting) together within each flavour row, but seaborn also hides
    # the y-tick labels on the second (YY) column by default (via
    # FacetGrid.__init__'s own `if sharey in [True, 'row']: ... label.set_
    # visible(False)` for every non-leftmost axis) -- appropriate when a
    # row has many columns to save space, but not here, where seeing both
    # columns' matching numbers side by side is the actual point. Undo it
    # with the same mechanism seaborn used to hide them.
    for ax in g.axes.flat:
        for label in ax.get_yticklabels():
            label.set_visible(True)
        ax.yaxis.offsetText.set_visible(True)

    if len(freqs) != len(weights):
        raise RuntimeError(f"({len(freqs)=}) and ({len(weights)=}) must be the same length")

    df = pd.DataFrame(
        {
            "freq": freqs,
            "weights": weights,
        }
    )

    # Per-(flavor, pol) sigma_resid outlier-range band, mirroring
    # reject_outliers's own median + nstd*1.4826*MAD formula computed
    # over that group's surviving (non-outlier) population -- shown as a
    # shaded band on each facet so a tile's residual scatter can be
    # visually compared against the actual threshold that would mark it
    # a population outlier, the same way the amplitude/gain-outlier plots
    # shade an acceptance band (see plot_outlier_gains).
    mad_to_std = MAD_TO_STD_SCALE_FACTOR
    sigma_resid_bands: dict[tuple[str, str], float] = {}
    for (flav, pol), grp in flavor_fits.groupby([COL_FLAVOR, COL_POL]):
        good = grp.loc[~grp[COL_OUTLIER], COL_SIGMA_RESID]
        if len(good) == 0:
            continue
        med = good.median()
        mad = (good - med).abs().median()
        if mad == 0 or np.isnan(mad):
            std = good.std(ddof=1) if len(good) > 1 else 0.0
            if not std:
                continue
            sigma_resid_bands[(flav, pol)] = med + nstd * std
        else:
            sigma_resid_bands[(flav, pol)] = med + nstd * mad_to_std * mad

    def plot_residual(
        soln_idxs: pd.Series,
        pols: pd.Series,
        flavs: pd.Series,
        lengths: pd.Series,
        intercepts: pd.Series,
        **kwargs,
    ):
        gradients = (2 * np.pi * u.rad * (lengths.to_numpy() * u.m) / c).to(u.rad / u.Hz).value
        intercepts_arr = intercepts.to_numpy()
        pol = pols.iloc[0]
        flav = flavs.iloc[0]
        if pol == COL_XX:
            solns = soln_xx[soln_idxs.values]
        elif pol == COL_YY:
            solns = soln_yy[soln_idxs.values]
        else:
            raise RuntimeError(f"wut pol? {pol}")
        models = gradients[:, np.newaxis] * freqs[np.newaxis, :] + intercepts_arr[:, np.newaxis]
        resids = wrap_angle(np.angle(solns) - models)
        # A whole frequency bin can legitimately be all-NaN here (e.g. every
        # tile in this flavor/pol group is flagged at that chanblock) --
        # already handled below via the isfinite `mask`, so the resulting
        # "All-NaN slice encountered" RuntimeWarning is expected noise, not
        # a sign of a problem.
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="All-NaN slice encountered", category=RuntimeWarning)
            medians = np.nanmedian(resids, axis=0)
        min_mse = np.inf
        best_coeffs = None
        best_indep = None
        # NOTE: this was previously
        #   np.logical_and(np.isfinite(medians), np.logical_not(np.isnan(medians)), weights > 0)
        # which does NOT do what it looks like: np.logical_and is a binary
        # ufunc, so the third positional argument is `out=`, not a third
        # condition. The `weights > 0` filter was therefore silently ignored
        # and zero-weight channels were included in the polyfit below.
        # np.isfinite() already excludes NaN (and inf), so the isnan() term
        # was redundant as well.
        mask = np.where(np.isfinite(medians) & (weights > 0))[0]
        df[f"{flav}_{pol}"] = medians

        band = sigma_resid_bands.get((flav, pol))
        if band is not None:
            plt.axhspan(-band, band, color="tab:blue", alpha=0.12, zorder=0)

        for indep_var in ["ν", "λ"]:
            if indep_var == "ν":
                xs = freqs[mask]
            elif indep_var == "λ":
                xs = 1.0 / freqs[mask]

            for order in range(1, 9):
                try:
                    # Orders up to 8 are deliberately tried against
                    # however many points happen to be valid; a
                    # poorly-conditioned high-order fit is expected here
                    # and gets discarded by the MSE comparison below, so
                    # numpy's RankWarning is expected noise, not a sign
                    # of a problem.
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=np.exceptions.RankWarning)
                        coeffs = np.polyfit(xs, medians[mask], order)
                except ValueError:
                    logger.exception(
                        f"plot_residual(): Error in np.polyfit. Skipping polyfit({order=}, {indep_var=}) due to "
                        f"ValueError for {flav=} {pol=}.\n{xs=}\n{medians[mask]=}"
                    )
                    continue

                mse = order * np.nanmean((medians - np.poly1d(coeffs)(freqs)) ** 2)
                if mse < min_mse:
                    min_mse = mse
                    best_coeffs = coeffs
                    best_indep = indep_var

        _ = kwargs.pop("label")
        sns.scatterplot(x=freqs, y=medians, hue=weights, **dict(**kwargs, marker="+"))
        if best_coeffs is not None and best_indep is not None:
            sns.lineplot(x=freqs, y=np.poly1d(best_coeffs)(freqs), **kwargs)
            eqn = poly_str(best_coeffs, independent_var=best_indep)
            poly_wrap = textwrap.fill(f"[{len(best_coeffs)}] {eqn}", width=40)
            plt.text(0.05, 0.1, poly_wrap, transform=plt.gca().transAxes, fontsize=7)
        if band is not None:
            plt.text(
                0.05,
                0.9,
                f"±{nstd:g}·MAD range: {band:.3f} rad",
                transform=plt.gca().transAxes,
                fontsize=7,
                color="tab:blue",
            )
        if residual_vmax is not None:
            ylim = float(residual_vmax)
            plt.ylim(-ylim, ylim)

    g.map(plot_residual, COL_SOLN_IDX, COL_POL, COL_FLAVOR, COL_LENGTH, "intercept")
    g.set_axis_labels("freq", "phase")

    fig = plt.gcf()
    if title:
        fig.suptitle(title)
        fig.subplots_adjust(top=0.95)
    fig.savefig(f"{prefix}residual.png", dpi=resolve_plot_dpi(200), bbox_inches="tight")
    df.to_csv(f"{prefix}residual.tsv", sep="\t", index=False)


def write_debug_phase_fit_plots(
    group: HyperfitsSolutionGroup,
    refant_name: str,
    final_phase_fits: pd.DataFrame,
    output_path: str,
    obs_id: int,
    phase_outlier_nstd: float = 3.0,
) -> None:
    """Write the phase-fit debug plots for the group's final flagged state.

    Split out of the old write_stats_and_debug_plots() (see
    calvin.plots.stats_table.write_before_after_stats for the stats-table
    half, which computes final_phase_fits -- pass its return value
    straight through here rather than recomputing anything, since phase
    fitting costs real time, roughly 2 minutes for a 256-tile observation
    in testing).

    Args:
        group: The solution group, after run_flagging_pipeline() (and
            typically commit()) have run.
        refant_name: Name of the reference antenna.
        final_phase_fits: group.phase_fits (the final, annotated phase fit
            DataFrame) -- the return value of
            calvin.plots.stats_table.write_before_after_stats().
        output_path: Directory to write the {obs_id}_rx_lengths.png,
            _phase_fits_xx.png, _phase_fits_yy.png, _intercepts.png, and
            _residual.png debug plots into.
        obs_id: The observation ID, used for output filenames.
        phase_outlier_nstd: Number of (MAD-derived) standard-deviation-
            equivalents beyond the population's robust median before a
            tile's phase fit is reported as an outlier. Purely advisory
            here -- only affects the plots' shaded outlier-range band, not
            group.phase_fits's own annotation (already fixed by whatever
            nstd run_flagging_pipeline was given). Pass the same value
            given to write_before_after_stats, or the BEFORE table and
            these plots will silently reflect two different thresholds.
    """
    tiles = group.metafits_tiles_df
    all_chanblocks_hz = group.all_chanblocks_hz_concat
    _, _noref_xx, _noref_yy, ref_xx, ref_yy = group.get_solns_both(refant_name)
    weights = group.weights
    plot_debug_phase_fits(
        final_phase_fits,
        tiles,
        all_chanblocks_hz,
        ref_xx,
        ref_yy,
        weights,
        prefix=os.path.join(output_path, f"{obs_id}_"),
        plot_residual=True,
        phase_outlier_nstd=phase_outlier_nstd,
    )
