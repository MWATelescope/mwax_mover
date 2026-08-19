"""Plotting and plot-adjacent reporting for the Calvin pipeline.

Contains everything that touches matplotlib/seaborn, plus hyperdrive's own
binary-generated plots/stats. Split out of mwax_calvin_quality.py (now
retired) and mwax_calvin_utils.py, per the mwax_hyperdrive_solutions.py
migration.

Two families of functions live here:

- Phase-fit diagnostics (plot_debug_phase_fits and its helpers) -- moved
  essentially unchanged, since they already operated on plain
  arrays/DataFrames rather than the old CalSolutionQuality model.
- Amplitude-outlier plots (plot_outlier_gains/plot_combined_gains) --
  rewritten to work from a HyperfitsSolutionGroup's flag-reason state
  (TileFlagReason/ChannelFlagReason) instead of the old bad_mask/band/fit
  tuple returned by mwax_calvin_quality.flag_bad_gains.
"""

import logging
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import matplotlib as mpl

# This is a batch/server-side pipeline that never displays a figure
# interactively -- only ever saves to file. Force the headless Agg
# backend explicitly (must happen before pyplot's first import, which is
# when backend selection is locked in) rather than letting matplotlib
# resolve to whatever interactive backend happens to be available (e.g.
# TkAgg), which wastes real time on GUI-toolkit overhead for every figure.
mpl.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from astropy import units as u
from astropy.constants import c  # ty: ignore[unresolved-import]
from matplotlib.colors import LinearSegmentedColormap
from numpy.typing import NDArray

from mwax_mover.mwax_calvin_utils import (
    annotate_phase_outliers,
    get_convergence_summary,
    pivot_phase_fits,
    poly_str,
    textwrap,
    wrap_angle,
)
from mwax_mover.mwax_command import run_command_ext
from mwax_mover.mwax_hyperdrive_solutions import (
    ChannelFlagReason,
    HyperfitsSolutionGroup,
    TileFlagReason,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase-fit diagnostics (plot_debug_phase_fits and helpers) -- moved from
# mwax_calvin_utils.py essentially unchanged; already worked on plain
# arrays/DataFrames, not CalSolutionQuality.
# ---------------------------------------------------------------------------


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
            mwax_calvin_utils.annotate_phase_outliers, not a bare
            process_phase_fits() result. Must include 'flavor' and
            'outlier' columns (a bare process_phase_fits() result will
            raise a KeyError). Callers needing "the" outlier verdict
            should compute it once via annotate_phase_outliers and reuse
            it here and for the stats table (see
            write_stats_and_debug_plots), so the two reports can't
            disagree with each other.
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

    n_good = len(flavor_fits[~flavor_fits["outlier"]])
    if n_good == 0:
        return None

    bad_fits = flavor_fits[flavor_fits["outlier"]]
    if len(bad_fits) > 0:
        logger.debug(f"{len(bad_fits)} of {n_total} fits are phase-outliers (reported only, not flagged):")
        logger.debug(bad_fits[["name", "pol"]].to_string(index=False))

    # make a new colormap for weighted data
    half_blues = LinearSegmentedColormap.from_list(
        colors=mpl.colormaps["Blues"](np.linspace(0.5, 1, 256)),
        name="HalfBlues",
    )

    if len(flavor_fits):
        _rx_means = plot_rx_lengths(flavor_fits, prefix, show, title)

    def ensure_system_byte_order(arr):
        system_byte_order = ">" if sys.byteorder == "big" else "<"
        if arr.dtype.byteorder != system_byte_order and arr.dtype.byteorder not in "|=":
            return arr.newbyteorder(system_byte_order)
        return arr

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
            plot_residual,
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
        "tile_id",
        "soln_idx",
        "pol",
        "length",
        "intercept",
        "sigma_resid",
        "chi2dof",
        "quality",
        "stderr",
        "outlier",
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
    good_fits = flavor_fits[~flavor_fits["outlier"]]
    rxs = sorted(good_fits["rx"].unique())
    means = good_fits.groupby(["rx"])["length"].mean()

    plt.clf()
    box_plot = sns.boxplot(data=good_fits, y="rx", x="length", hue="pol", orient="h", fliersize=0.5)
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
        fig.savefig(f"{prefix}rx_lengths.png", dpi=300, bbox_inches="tight")

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
    figsize = (np.clip(len(slots) * 2.5, 5, 20), np.clip(len(rxs) * 3, 5, 30))

    for pol, soln in zip(["xx", "yy"], [soln_xx, soln_yy]):
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
            signal = soln[fit["soln_idx"]]
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
            fig.savefig(f"{prefix}phase_fits_{pol}.png", dpi=300, bbox_inches="tight")


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
        row="flavor",
        col="pol",
        hue="flavor",
        row_order=sorted(flavor_fits["flavor"].unique()),
        col_order=["XX", "YY"],
        subplot_kws={"projection": "polar"},
        sharex=False,
        sharey=False,
        despine=False,
    )
    g.map(
        (lambda theta, r, size, **kwargs: plt.scatter(x=theta, y=r, s=10 / (0.1 + size), **kwargs)),
        "intercept",
        "length",
        "sigma_resid",
    )
    fig = plt.gcf()
    if title:
        fig.suptitle(title)
        fig.subplots_adjust(top=0.95)
    if show:
        plt.show()
    if prefix:
        plt.tight_layout()
        fig.savefig(f"{prefix}intercepts.png", dpi=300, bbox_inches="tight")


def plot_phase_residual(
    freqs,
    soln_xx,
    soln_yy,
    weights,
    prefix,
    title,
    plot_res,
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
        plot_res: Whether to plot residuals.
        residual_vmax: Maximum value for residual plot y-axis.
        flavor_fits: DataFrame with phase fit results per receiver
            flavor, already annotated with an 'outlier' column (see
            mwax_calvin_utils.annotate_phase_outliers).
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
    (see Step 3's rationale in CALVIN.md).
    """
    plt.clf()
    g = sns.FacetGrid(
        flavor_fits,
        row="flavor",
        col="pol",
        hue="flavor",
        row_order=sorted(flavor_fits["flavor"].unique()),
        col_order=["XX", "YY"],
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
    mad_to_std = 1.4826
    sigma_resid_bands: dict[tuple[str, str], float] = {}
    for (flav, pol), grp in flavor_fits.groupby(["flavor", "pol"]):
        good = grp.loc[~grp["outlier"], "sigma_resid"]
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
        if pol == "XX":
            solns = soln_xx[soln_idxs.values]
        elif pol == "YY":
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
        mask = np.where(np.logical_and(np.isfinite(medians), np.logical_not(np.isnan(medians)), weights > 0))[0]
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
            poly_wrap = textwrap(f"[{len(best_coeffs)}] {eqn}", width=40)
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

    g.map(plot_residual, "soln_idx", "pol", "flavor", "length", "intercept")
    g.set_axis_labels("freq", "phase")

    fig = plt.gcf()
    if title:
        fig.suptitle(title)
        fig.subplots_adjust(top=0.95)
    fig.savefig(f"{prefix}residual.png", dpi=200, bbox_inches="tight")
    df.to_csv(f"{prefix}residual.tsv", sep="\t", index=False)


def generate_hyperdrive_plots(
    obs_id: int,
    hyperdrive_solution_filename: str,
    hyperdrive_binary_path: str,
    metafits_filename: str,
    output_dir: str,
    before: bool,
) -> tuple[bool, str]:
    """Generate solution plots via the hyperdrive binary itself.

    Args:
        obs_id: Observation ID.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        metafits_filename: Path to the metafits file.
        output_dir: path to where we write the plots
        before: bool specifying if this run is the BEFORE or AFTER calvin flags outliers- only used to generate correct filenames

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(
        f"{obs_id} generating {'original unmodified' if before else 'after flagging'} hyperdrive plots for {hyperdrive_solution_filename}..."
    )

    try:
        hyp_soln_plot_args = f" --output-directory {output_dir}"
        cmd = (
            f"{hyperdrive_binary_path} solutions-plot {hyp_soln_plot_args} "
            f"-m"
            f" {metafits_filename} {hyperdrive_solution_filename}"
        )

        success, output = run_command_ext(cmd, -1, timeout=60, use_shell=False)

        if not success:
            logger.warning(f"{obs_id} hyperdrive solutions-plot failed for {hyperdrive_solution_filename}: {output}")
            return False, output

        if before:
            # Rename files so the AFTER run does not overwrite them.
            # Glob results are materialized into a list first -- renaming
            # a file while a generator is still scanning the same
            # directory for the same pattern is mutating the directory
            # mid-iteration, which isn't guaranteed to behave consistently.
            directory = Path(output_dir)  # change to your target directory

            # rename "*_solutions_amps.png" to "_solutions_amps_original.png"
            for file in list(directory.glob("*_solutions_amps.png")):
                new_name = file.with_name(file.stem + "_original" + file.suffix)
                file.rename(new_name)

            # rename "*_solutions_phases.png" to "_solutions_phases_original.png"
            for file in list(directory.glob("*_solutions_phases.png")):
                new_name = file.with_name(file.stem + "_original" + file.suffix)
                file.rename(new_name)

        logger.info(f"{obs_id} Finished running hyperdrive plots on {hyperdrive_solution_filename}.")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


def write_hyperdrive_stats(
    obs_id: int,
    stats_fd,
    hyperdrive_solution_filename: str,
) -> tuple[bool, str]:
    """Write convergence statistics. (Append to existing stats file if it exists.)

    Args:
        obs_id: Observation ID.
        stats_fd: File descriptor for the statistics file.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(f"{obs_id} Writing convergence stats for {hyperdrive_solution_filename}.")
    try:
        conv_summary_list = get_convergence_summary(hyperdrive_solution_filename)

        stats_fd.writelines(f"{row[0]}: {row[1]}\n" for row in conv_summary_list)
        stats_fd.write("\n")

        logger.info(f"{obs_id} Finished running convergence stats for {hyperdrive_solution_filename}.")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


# ---------------------------------------------------------------------------
# Amplitude-outlier plots -- rewritten from mwax_calvin_quality.plot_combined
# / plot_outlier_gains to work from a HyperfitsSolutionGroup's flag-reason
# state instead of the old bad_mask/band/fit tuple from flag_bad_gains.
# ---------------------------------------------------------------------------


def _grid_shape(n: int) -> tuple[int, int]:
    """Compute a near-square (rows, cols) grid that fits n tile-pairs.

    Chooses cols = ceil(sqrt(n)) and rows = ceil(n / cols). Each "tile"
    occupies two actual subplot columns (amplitude + residual), so the
    caller multiplies the returned cols by 2 when building the actual
    subplot grid.

    Args:
        n: Number of tile-pairs needed.

    Returns:
        A (rows, cols) tuple, where cols is the number of tile-pairs per row.
    """
    cols = int(np.ceil(np.sqrt(n)))
    rows = int(np.ceil(n / cols))
    return rows, cols


def _paged_output_path(output_path: str, first_tile_index: int, last_tile_index: int) -> str:
    """Build a per-page output filename with a tile-range suffix.

    E.g. _paged_output_path("test.png", 0, 63) -> "test_0-63.png"

    Args:
        output_path: Base output path, e.g. "test.png" or "/some/dir/test.png".
        first_tile_index: First tile index included in this page.
        last_tile_index: Last tile index included in this page.

    Returns:
        The output path with "_{first}-{last}" inserted before the extension.
    """
    base, ext = os.path.splitext(output_path)
    return f"{base}_{first_tile_index}-{last_tile_index}{ext}"


def _format_flavor(flavor: str) -> str:
    """Format a receiver flavour for display in the stats table.

    mwalib's ReceiverType enum stringifies as e.g. "ReceiverType.SHAO" --
    strip the class-name prefix so the stats table just shows "SHAO",
    matching the short form used everywhere else (CALVIN.md, log
    messages, etc.). Passes through unchanged if there's no such prefix
    (e.g. an empty string, or a plain str already).

    Args:
        flavor: A tile's flavor value, e.g. from metafits_tiles_df.

    Returns:
        Display-formatted flavour string.
    """
    text = str(flavor)
    return text.rsplit(".", 1)[-1] if "." in text else text


def _tile_flag_reason_text(tile_idx: int, tile_reasons: NDArray[np.object_]) -> str:
    """Build a human-readable reason string for a fully-flagged tile.

    Args:
        tile_idx: Tile index (row position in the group's metafits_tiles_df).
        tile_reasons: A TileFlagReason array, shape (n_tiles,) -- e.g.
            group.tile_flag_reasons, or a copy taken at an earlier point
            (this function doesn't care which snapshot it reflects).

    Returns:
        A semicolon-separated description of every TileFlagReason bit set
        for this tile, or a note that it was fully flagged by per-channel
        reasons alone (no whole-tile reason bit set).
    """
    reason = tile_reasons[tile_idx]
    parts = []
    if reason & TileFlagReason.METAFITS:
        parts.append("flagged in metafits")
    if reason & TileFlagReason.HYPERDRIVE_TILE:
        parts.append("flagged in TILES HDU")
    if reason & TileFlagReason.HYPERDRIVE_BASELINE:
        parts.append("flagged via BASELINES-HDU inference")
    if reason & TileFlagReason.PHASE_OUTLIER:
        parts.append("phase fit is a population outlier")
    if reason & TileFlagReason.MOSTLY_BAD_CHANNELS:
        parts.append("too many bad channels (promoted to fully flagged)")
    if not parts:
        return "Fully flagged by per-channel reasons alone (no whole-tile flag reason)"
    return "; ".join(parts)


# Human-readable label for each ChannelFlagReason bit, used by
# _fully_flagged_channel_summary_text below. AMPLITUDE_OUTLIER isn't
# here -- its label needs the actual mad_residual_threshold value, built
# separately where it's used.
_CHANNEL_REASON_LABELS = {
    ChannelFlagReason.PRE_EXISTING_NAN: "NaN",
    ChannelFlagReason.NON_CONVERGED: "not converged",
    ChannelFlagReason.PARTIAL_JONES: "partial Jones",
    ChannelFlagReason.GAIN_MAX_CUTOFF: "above gain cutoff",
}


def _fully_flagged_channel_summary_text(
    tile_idx: int, file_reasons: NDArray[np.object_], mad_residual_threshold: float
) -> str:
    """Build the 1-2 line top-centre summary for a Calvin-fully-flagged tile.

    Unlike _tile_flag_reason_text (used for a tile flagged structurally,
    before Calvin's own analysis ever ran, where there's no per-channel
    data to break down), this is for a tile Calvin itself fully flagged --
    e.g. promoted via flag_mostly_bad_tiles, or simply 100%
    per-channel-flagged without a specific whole-tile reason -- where
    real per-channel data exists and is worth summarising.

    Line 1 is the fraction of this tile's channels that were never
    individually flagged for their own reason -- i.e. would have been
    usable on their own, before a whole-tile promotion (if any) swept
    them into NaN regardless -- as "{pct}% Good (n_good/n_total)". A
    tile promoted by flag_mostly_bad_tiles always has n_good > 0 for at
    least some fraction below the promotion threshold; a tile that
    reached 100% individually-flagged channels without ever needing
    promotion has n_good = 0.

    Line 2 lists every distinct per-channel reason actually present on
    this tile as "{count} {label}", comma-separated, in ascending
    reason-bit order (matching ChannelFlagReason's declaration order,
    which happens to roughly match pipeline order too) -- omitted
    entirely if line 1 already covers everything (n_good == n_total,
    which can't actually happen here since this function is only
    called for a fully-flagged tile, but handled defensively anyway).

    Args:
        tile_idx: Tile index (row position in metafits_tiles_df).
        file_reasons: This file's ChannelFlagReason array, shape
            (n_tiles, n_chanblocks) -- e.g. bundle["file_reasons"].
        mad_residual_threshold: The actual MAD-residual threshold used
            by flag_amplitude_outliers (see HyperfitsSolutionGroup.
            mad_residual_threshold), for an accurate "outside N MAD"
            label rather than a hardcoded guess.

    Returns:
        A 1-2 line string, ready to hand straight to ax.text().
    """
    reasons_here = file_reasons[tile_idx, :]
    n_total = len(reasons_here)
    n_good = int(np.sum(reasons_here == ChannelFlagReason.NONE))
    pct_good = 100 * n_good / n_total if n_total else 0.0
    line1 = f"{pct_good:.0f}% Good ({n_good}/{n_total})"

    parts = []
    for flag in ChannelFlagReason:
        if flag == ChannelFlagReason.NONE:
            continue
        label = f"outside {mad_residual_threshold:g} MAD" if flag == ChannelFlagReason.AMPLITUDE_OUTLIER else _CHANNEL_REASON_LABELS[flag]
        n = int(np.sum([bool(r & flag) for r in reasons_here]))
        if n:
            parts.append(f"{n} {label}")

    if not parts:
        return line1
    return line1 + "\n" + ", ".join(parts)


def _extract_combined_gains_bundle(group: HyperfitsSolutionGroup, file_idx: int) -> dict:
    """Extract the plain, picklable data _render_combined_gains_figure
    needs from a HyperfitsSolutionGroup.

    Exists so plot_outlier_gains can dispatch page rendering to worker
    processes: a HyperfitsSolutionGroup itself isn't picklable (it holds
    mwalib's Rust-backed MetafitsContext), but everything actually needed
    to render a page is plain numpy arrays/dicts/primitives, which are.
    Called once per file (not per page) since the same bundle is reused
    for every page of that file.

    Args:
        group: The solution group, after apply_tile_flags,
            enforce_whole_jones_nan, detect_phase_outliers,
            flag_amplitude_outliers, and flag_mostly_bad_tiles have run.
        file_idx: Which solution file (index into group.jones/solns).

    Returns:
        A dict of everything _render_combined_gains_figure needs.
    """
    assert group.jones is not None
    assert group.channel_flag_reasons is not None
    assert group.tile_flag_reasons is not None
    assert group.amplitude_fit is not None
    assert group.amplitude_band is not None
    assert group.mad_residual_threshold is not None

    return {
        "file_idx": file_idx,
        "file_jones": group.jones[file_idx],
        "tile_names": group.metafits_tiles_df["name"].to_numpy(),
        "fit": group.amplitude_fit[file_idx],
        "band": group.amplitude_band[file_idx],
        "file_reasons": group.channel_flag_reasons[file_idx],
        "tile_reasons": group.tile_flag_reasons,
        "obsid": group.metafits.obsid,
        "mad_residual_threshold": group.mad_residual_threshold,
    }


def plot_combined_gains(
    group: HyperfitsSolutionGroup,
    file_idx: int,
    first_tile_index: int = 0,
    n_tiles: int = 16,
    pristine_jones: NDArray[np.complex128] | None = None,
    solution_file_will_be_modified: bool = True,
) -> plt.Figure:
    """Plot gx and gy gain amplitude in separate subplots per tile, for one
    file in a solution group.

    Equivalent to the retired mwax_calvin_quality.plot_combined, adapted to
    work from HyperfitsSolutionGroup's flag-reason state (after all
    flagging methods have run) instead of the old bad_mask/band/fit tuple
    from flag_bad_gains.

    Thin wrapper around _render_combined_gains_figure: extracts the plain
    data that function needs from group, then delegates to it. Kept
    separate so plot_outlier_gains can extract the bundle once per file
    and reuse it across every page (and across worker processes, when
    saving to disk), rather than needing group itself in each page's
    rendering call.

    Args:
        group: The solution group, after apply_tile_flags,
            enforce_whole_jones_nan, detect_phase_outliers,
            flag_amplitude_outliers, and flag_mostly_bad_tiles have run.
        file_idx: Which solution file (index into group.jones/solns) to plot.
        first_tile_index: Index of the first tile to include in this page.
        n_tiles: Number of tiles to plot starting from first_tile_index.
            Also determines the subplot grid shape (see _grid_shape).
        pristine_jones: Jones matrices to plot amplitudes from, shape
            (n_tiles, n_chanblocks, 2, 2) -- e.g. a copy of
            group.jones[file_idx] taken before any flagging ran, so
            flagged-but-not-yet-NaN'd values are still visible on the
            plot. Defaults to group.jones[file_idx] (its current state)
            if not given -- if flagging has already run, flagged entries
            will show as gaps rather than visible outlier points.
        solution_file_will_be_modified: If True, a note is added to the
            figure title.

    Returns:
        The matplotlib Figure containing the grid of per-tile subplot pairs.
    """
    bundle = _extract_combined_gains_bundle(group, file_idx)
    return _render_combined_gains_figure(bundle, first_tile_index, n_tiles, pristine_jones, solution_file_will_be_modified)


def _render_combined_gains_figure(
    bundle: dict,
    first_tile_index: int,
    n_tiles: int,
    pristine_jones: NDArray[np.complex128] | None,
    solution_file_will_be_modified: bool,
) -> plt.Figure:
    """Render one page of the combined gx/gy amplitude plot from an
    extracted data bundle (see _extract_combined_gains_bundle).

    This is the actual rendering logic behind plot_combined_gains, kept
    as a standalone function (touching only plain data, never a
    HyperfitsSolutionGroup) so it can run directly inside a
    ProcessPoolExecutor worker.

    Fully-flagged tiles (every channel bad, from any combination of
    per-channel reasons and/or a whole-tile reason) are handled one of
    two ways, depending on *why* they're fully flagged:

    - Flagged structurally, before Calvin's own analysis ever ran
      (metafits / TILES-HDU / BASELINES-HDU-inferred -- apply_tile_flags()
      NaNs these immediately): there's no real data to show even in the
      pristine snapshot, so both subplots show only the flag reason as
      red text, top-centre, with a red border. Titles still say "gx
      amplitude"/"gy amplitude" (plus "FULLY FLAGGED"), even with no
      data plotted, so the two subplots remain distinguishable.
    - Flagged by Calvin itself (e.g. promoted via flag_mostly_bad_tiles,
      or simply 100% per-channel-flagged without a specific whole-tile
      reason): real pristine data still exists, so both subplots get the
      normal plot (below) *plus* a red top-centre summary and red border
      overlaid on top -- the point being flagged, not the absence of
      data. Unlike the structural case's single-line reason, this
      summary is built by _fully_flagged_channel_summary_text: a "{pct}%
      Good (n_good/n_total)" line (the fraction of channels that were
      never individually flagged for their own reason, before whatever
      whole-tile promotion swept them into NaN regardless), then a
      second line breaking down every distinct per-channel reason
      actually present as "{count} {label}", comma-separated (e.g. "100
      NaN, 200 above gain cutoff, 22 outside 10 MAD"). Titles say "gx
      amplitude - FULLY FLAGGED"/"gy amplitude - FULLY FLAGGED", matching
      the structural case's "FULLY FLAGGED" suffix. If the underlying
      polynomial fit has no valid
      channels left to fit against (e.g. a tile where every channel was
      already excluded by something else before flag_amplitude_outliers
      ran), the band/fit legitimately have nothing to show and are left
      blank -- this is an inherent data limitation, not a bug to work
      around with a fabricated fallback.

    For all other (non-fully-flagged) tiles, each gets two adjacent
    subplots (gx, then gy), showing the raw gain amplitude, the
    polynomial fit line, and a shaded band showing the acceptable range
    around the fit (see HyperfitsSolutionGroup.flag_amplitude_outliers).
    Colour tracks severity, not which check caught a channel: orange for
    a partial (some-channels) flag, red reserved for a fully flagged
    tile. Channels caught by amplitude-outlier detection are shaded
    orange and marked with a black 'x'; channels caught by the absolute
    gain-magnitude sanity cutoff instead (see HyperfitsSolutionGroup.
    flag_gain_max_cutoff) are also shaded orange, but marked with a
    black '+' -- the two reasons are told apart by marker shape, not
    colour. Shading (and the 'x'/'+' markers) is restricted to channels
    with their own genuine per-channel reason, not every channel of a
    fully-flagged tile (flag_mostly_bad_tiles NaNs every channel of a
    promoted tile regardless of whether that specific channel ever
    earned its own reason, so blindly using the tile-wide bad mask here
    would mark innocent channels as if they had been individually
    caught). Border colour: red if the tile is fully flagged (regardless
    of why -- see above), else orange if it has any new gain-cutoff or
    amplitude-outlier flags, else the default axes colour.

    Args:
        bundle: Extracted data from _extract_combined_gains_bundle.
        first_tile_index: Index of the first tile to include in this page.
        n_tiles: Number of tiles to plot starting from first_tile_index.
            Also determines the subplot grid shape (see _grid_shape).
        pristine_jones: Jones matrices to plot amplitudes from, shape
            (n_tiles, n_chanblocks, 2, 2) -- e.g. a copy of the file's
            jones taken before any flagging ran, so flagged-but-not-yet-
            NaN'd values are still visible on the plot. Defaults to
            bundle["file_jones"] (its current state) if not given -- if
            flagging has already run, flagged entries will show as gaps
            rather than visible outlier points.
        solution_file_will_be_modified: If True, a note is added to the
            figure title.

    Returns:
        The matplotlib Figure containing the grid of per-tile subplot pairs.
    """
    file_idx = bundle["file_idx"]
    file_jones = bundle["file_jones"]
    n_tiles_total, n_chanblocks = file_jones.shape[:2]
    tile_names = bundle["tile_names"]

    gains_for_plot = pristine_jones if pristine_jones is not None else file_jones
    before_gx = np.abs(gains_for_plot[:, :, 0, 0])
    before_gy = np.abs(gains_for_plot[:, :, 1, 1])

    fit = bundle["fit"]
    band_lower_gx, band_upper_gx = bundle["band"]["gx"]
    band_lower_gy, band_upper_gy = bundle["band"]["gy"]

    file_reasons = bundle["file_reasons"]
    tile_reasons = bundle["tile_reasons"]
    mad_residual_threshold = bundle["mad_residual_threshold"]

    last_tile_index = min(first_tile_index + n_tiles, n_tiles_total)
    tile_range = range(first_tile_index, last_tile_index)
    chan_idx = np.arange(n_chanblocks)

    # Per-channel "bad" mask, combining this file's own channel reasons with
    # any whole-tile flag (broadcast across every chanblock).
    bad_mask = (file_reasons != ChannelFlagReason.NONE) | (tile_reasons[:, np.newaxis] != TileFlagReason.NONE)
    # Channels caught specifically by amplitude-outlier detection.
    new_amplitude_bad_mask = np.array(
        [[bool(reason & ChannelFlagReason.AMPLITUDE_OUTLIER) for reason in row] for row in file_reasons]
    )
    # Channels caught specifically by the absolute gain-magnitude sanity
    # cutoff (see HyperfitsSolutionGroup.flag_gain_max_cutoff) -- shown
    # distinctly from ordinary amplitude outliers, since a value large
    # enough to trip this is a numerical divergence, not a borderline
    # statistical call.
    new_gain_cutoff_bad_mask = np.array(
        [[bool(reason & ChannelFlagReason.GAIN_MAX_CUTOFF) for reason in row] for row in file_reasons]
    )

    n_plotted = len(tile_range)
    n_rows, n_tile_cols = _grid_shape(n_tiles)
    n_cols = n_tile_cols * 2
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(6 * n_cols, 4 * n_rows), dpi=150, squeeze=False)

    for i, tile in enumerate(tile_range):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        ax_gx = axes[row, col_pair]
        ax_gy = axes[row, col_pair + 1]

        flagged = bad_mask[tile, :]
        n_flagged_here = int(flagged.sum())
        has_new_amplitude_bad_mask = bool(new_amplitude_bad_mask[tile, :].any())
        has_new_gain_cutoff_bad_mask = bool(new_gain_cutoff_bad_mask[tile, :].any())
        tile_name = tile_names[tile]
        tile_fully_flagged = bool(flagged.all())

        if tile_fully_flagged:
            reason = _tile_flag_reason_text(tile, tile_reasons)
            # Tiles flagged before Calvin's own analysis ever ran
            # (apply_tile_flags() NaNs them immediately) have no real
            # data to show even in the pristine snapshot -- text only,
            # same as before. A tile Calvin itself fully flagged (e.g.
            # promoted via flag_mostly_bad_tiles) still has real pristine
            # data, so it falls through to the normal plotting path below
            # instead, with the reason text and border added on top.
            structural_reason = bool(
                tile_reasons[tile]
                & (TileFlagReason.METAFITS | TileFlagReason.HYPERDRIVE_TILE | TileFlagReason.HYPERDRIVE_BASELINE)
            )
            if structural_reason:
                for ax in (ax_gx, ax_gy):
                    ax.axis("on")
                    ax.set_xticks([])
                    ax.set_yticks([])
                    ax.text(
                        0.5,
                        0.92,
                        reason,
                        ha="center",
                        va="top",
                        wrap=True,
                        fontsize=8,
                        color="red",
                        transform=ax.transAxes,
                    )
                    for spine in ax.spines.values():
                        spine.set_edgecolor("red")
                        spine.set_linewidth(2.5)

                ax_gx.set_title(f"Tile {tile} ({tile_name}) - gx amplitude - FULLY FLAGGED", fontsize=9)
                ax_gy.set_title(f"Tile {tile} ({tile_name}) - gy amplitude - FULLY FLAGGED", fontsize=9)
                continue

        # Channels with a genuine per-channel reason of their own, as
        # opposed to `flagged` (bad_mask), which also broadcasts a
        # whole-tile reason (e.g. MOSTLY_BAD_CHANNELS) across every
        # chanblock regardless of that channel's own history. Used below
        # to keep the black 'x' marker restricted to channels that were
        # actually individually flagged -- a channel only NaN'd because
        # flag_mostly_bad_tiles promoted the whole tile never earned its
        # own reason and shouldn't look like it did.
        channel_level_flagged = file_reasons[tile, :] != ChannelFlagReason.NONE

        # -- shade amplitude-outlier-flagged channels with a translucent
        # orange band -- orange indicates partial (some-channels)
        # flagging regardless of reason; red is reserved for a fully
        # flagged tile (see the border-colour logic below), not for
        # which specific check caught the channel.
        new_flagged_chans = np.where(new_amplitude_bad_mask[tile, :])[0]
        for cb in new_flagged_chans:
            ax_gx.axvspan(cb - 0.5, cb + 0.5, color="orange", alpha=0.15, zorder=0)
            ax_gy.axvspan(cb - 0.5, cb + 0.5, color="orange", alpha=0.15, zorder=0)

        # -- shade gain-max-cutoff-flagged channels the same translucent
        # orange -- the two reasons are still told apart by marker shape
        # ('x' vs '+' below), not colour, since colour here tracks
        # severity (partial vs fully flagged) rather than which specific
        # check caught the channel --
        new_gain_cutoff_chans = np.where(new_gain_cutoff_bad_mask[tile, :])[0]
        for cb in new_gain_cutoff_chans:
            ax_gx.axvspan(cb - 0.5, cb + 0.5, color="orange", alpha=0.15, zorder=0)
            ax_gy.axvspan(cb - 0.5, cb + 0.5, color="orange", alpha=0.15, zorder=0)

        # -- gx subplot: data, fit line, shaded acceptance band --
        ax_gx.fill_between(
            chan_idx,
            band_lower_gx[tile],
            band_upper_gx[tile],
            color="tab:blue",
            alpha=0.15,
            zorder=0,
            label="gx band",
        )
        ax_gx.plot(chan_idx, before_gx[tile], color="tab:blue", alpha=0.7, linewidth=0.8, label="gx")
        ax_gx.plot(chan_idx, fit["gx"][tile], color="black", linestyle="--", alpha=0.8, linewidth=0.8, label="gx fit")
        flagged_other_gx = channel_level_flagged & ~new_gain_cutoff_bad_mask[tile, :]
        if flagged_other_gx.any():
            ax_gx.scatter(
                chan_idx[flagged_other_gx],
                before_gx[tile][flagged_other_gx],
                color="black",
                marker="x",
                s=15,
                zorder=3,
                label="flagged",
            )
        if new_gain_cutoff_bad_mask[tile, :].any():
            ax_gx.scatter(
                chan_idx[new_gain_cutoff_bad_mask[tile, :]],
                before_gx[tile][new_gain_cutoff_bad_mask[tile, :]],
                color="black",
                marker="+",
                s=30,
                zorder=3,
                label="gain cutoff",
            )

        gx_title = f"Tile {tile} ({tile_name}) - gx amplitude"
        if n_flagged_here == 0:
            gx_title += " (no flags)"
        ax_gx.set_title(gx_title, fontsize=9)
        ax_gx.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False, useMathText=True))
        ax_gx.tick_params(labelsize=7)

        # -- gy subplot: data, fit line, shaded acceptance band --
        ax_gy.fill_between(
            chan_idx,
            band_lower_gy[tile],
            band_upper_gy[tile],
            color="tab:green",
            alpha=0.15,
            zorder=0,
            label="gy band",
        )
        ax_gy.plot(chan_idx, before_gy[tile], color="tab:green", alpha=0.7, linewidth=0.8, label="gy")
        ax_gy.plot(chan_idx, fit["gy"][tile], color="gray", linestyle="--", alpha=0.8, linewidth=0.8, label="gy fit")
        flagged_other_gy = channel_level_flagged & ~new_gain_cutoff_bad_mask[tile, :]
        if flagged_other_gy.any():
            ax_gy.scatter(
                chan_idx[flagged_other_gy],
                before_gy[tile][flagged_other_gy],
                color="black",
                marker="x",
                s=15,
                zorder=3,
                label="flagged",
            )
        if new_gain_cutoff_bad_mask[tile, :].any():
            ax_gy.scatter(
                chan_idx[new_gain_cutoff_bad_mask[tile, :]],
                before_gy[tile][new_gain_cutoff_bad_mask[tile, :]],
                color="black",
                marker="+",
                s=30,
                zorder=3,
                label="gain cutoff",
            )

        gy_title = f"Tile {tile} ({tile_name}) - gy amplitude"
        if n_flagged_here == 0:
            gy_title += " (no flags)"
        ax_gy.set_title(gy_title, fontsize=9)
        ax_gy.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False, useMathText=True))
        ax_gy.tick_params(labelsize=7)

        if tile_fully_flagged:
            # Calvin fully flagged this tile itself (e.g. promoted via
            # flag_mostly_bad_tiles) -- real pristine data has still been
            # plotted above with the normal styling, but a red border and
            # a summary of why (also red, top-centre so it doesn't sit
            # over the middle of the data) make the fully-flagged status
            # visually unambiguous, matching the structural-flag case's
            # colour even though that one has no data to plot alongside it.
            # Unlike the structural case, real per-channel data exists
            # here, so the summary is the actual channel breakdown
            # (_fully_flagged_channel_summary_text), not the simpler
            # single-line _tile_flag_reason_text used above.
            calvin_fully_flagged_summary = _fully_flagged_channel_summary_text(
                tile, file_reasons, mad_residual_threshold
            )
            for ax in (ax_gx, ax_gy):
                for spine in ax.spines.values():
                    spine.set_edgecolor("red")
                    spine.set_linewidth(2.5)
                ax.text(
                    0.5,
                    0.92,
                    calvin_fully_flagged_summary,
                    ha="center",
                    va="top",
                    wrap=True,
                    fontsize=8,
                    color="red",
                    transform=ax.transAxes,
                    zorder=10,
                    bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.75, "edgecolor": "none"},
                )
            gx_title += " - FULLY FLAGGED"
            gy_title += " - FULLY FLAGGED"
            ax_gx.set_title(gx_title, fontsize=9)
            ax_gy.set_title(gy_title, fontsize=9)
        elif has_new_gain_cutoff_bad_mask or has_new_amplitude_bad_mask:
            # Orange for a partially-flagged tile, regardless of which
            # reason -- red is reserved for fully flagged (above), not
            # for a specific check.
            for ax in (ax_gx, ax_gy):
                for spine in ax.spines.values():
                    spine.set_edgecolor("orange")
                    spine.set_linewidth(2.5)

    for i in range(n_plotted, n_rows * n_tile_cols):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        axes[row, col_pair].axis("off")
        axes[row, col_pair + 1].axis("off")

    handles, labels = [], []
    for ax in axes.flat:
        for handle, label in zip(*ax.get_legend_handles_labels()):
            if label not in labels:
                handles.append(handle)
                labels.append(label)
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.97), ncol=4)

    obsid = bundle["obsid"]
    obsid_str = f"obsid {obsid}" if obsid is not None else "obsid unknown"
    modification_str = (
        "Jones matrices of outlier gains will be NaNed"
        if solution_file_will_be_modified
        else "Outlier gains are reported only, solutions file was not modified"
    )
    fig.suptitle(
        f"Gain amplitude with fit & MAD band for {obsid_str} "
        f"(file {file_idx}; tiles {first_tile_index}-{last_tile_index - 1}; NO REF TILE). {modification_str}",
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.90))

    return fig


def _render_and_save_combined_gains_page(
    bundle: dict,
    first_tile_index: int,
    n_tiles: int,
    pristine_jones: NDArray[np.complex128] | None,
    solution_file_will_be_modified: bool,
    page_path: str,
) -> tuple[bool, str]:
    """Render one page and save it to disk -- a ProcessPoolExecutor worker
    entry point for plot_outlier_gains.

    Returns (success, error) rather than the Figure itself: a matplotlib
    Figure doesn't survive a process boundary usefully (and every current
    caller of plot_outlier_gains only wants the on-disk file anyway).

    Args:
        bundle: Extracted data from _extract_combined_gains_bundle.
        first_tile_index: See _render_combined_gains_figure.
        n_tiles: See _render_combined_gains_figure.
        pristine_jones: See _render_combined_gains_figure.
        solution_file_will_be_modified: See _render_combined_gains_figure.
        page_path: Where to save this page.

    Returns:
        (True, "") on success, or (False, error message) if rendering or
        saving raised -- logged by the caller rather than propagated, so
        one bad page doesn't take down the rest of the batch.
    """
    try:
        fig = _render_combined_gains_figure(
            bundle, first_tile_index, n_tiles, pristine_jones, solution_file_will_be_modified
        )
        fig.savefig(page_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return True, ""
    except Exception as exc:  # noqa: BLE001 -- reported to the caller, not raised in the worker
        return False, str(exc)


def plot_outlier_gains(
    group: HyperfitsSolutionGroup,
    file_idx: int,
    n_tiles: int = 16,
    output_path: str | None = None,
    pristine_jones: NDArray[np.complex128] | None = None,
    solution_file_will_be_modified: bool = True,
) -> list[plt.Figure]:
    """Plot flagged hyperdrive calibration gains, paged by tile, for one file.

    Args:
        group: The solution group (see plot_combined_gains).
        file_idx: Which solution file (index into group.jones/solns) to plot.
        n_tiles: Number of tiles per page/figure.
        output_path: If given, each page is saved using this as the base
            filename, with "_{first}-{last}" inserted before the extension.
        pristine_jones: See plot_combined_gains.
        solution_file_will_be_modified: See plot_combined_gains.

    Returns:
        When output_path is None: a list of matplotlib Figures, one per
        page, in tile order (rendered sequentially in-process). When
        output_path is given (every current caller): pages are instead
        rendered and saved in parallel worker processes (see
        _render_and_save_combined_gains_page), and this returns an empty
        list -- no Figure objects survive the process boundary, and
        nothing currently uses the return value in that case anyway.
    """
    assert group.jones is not None
    n_tiles_total = group.jones[file_idx].shape[0]
    n_pages = int(np.ceil(n_tiles_total / n_tiles))

    if output_path is None:
        # Rare/unused in practice -- no current caller relies on getting
        # live Figure objects back. Keep this path simple and sequential
        # rather than adding process-pool complexity for a case nothing
        # exercises.
        figures: list[plt.Figure] = []
        for page in range(n_pages):
            first_tile_index = page * n_tiles
            figures.append(
                plot_combined_gains(
                    group,
                    file_idx,
                    first_tile_index=first_tile_index,
                    n_tiles=n_tiles,
                    pristine_jones=pristine_jones,
                    solution_file_will_be_modified=solution_file_will_be_modified,
                )
            )
        return figures

    bundle = _extract_combined_gains_bundle(group, file_idx)

    with ProcessPoolExecutor() as executor:
        futures = {}
        for page in range(n_pages):
            first_tile_index = page * n_tiles
            last_tile_index = min(first_tile_index + n_tiles, n_tiles_total) - 1
            page_path = _paged_output_path(output_path, first_tile_index, last_tile_index)
            future = executor.submit(
                _render_and_save_combined_gains_page,
                bundle,
                first_tile_index,
                n_tiles,
                pristine_jones,
                solution_file_will_be_modified,
                page_path,
            )
            futures[future] = page_path

        for future in as_completed(futures):
            success, error = future.result()
            if not success:
                logger.warning(f"Failed to render/save {futures[future]}: {error}")

    return []


# ---------------------------------------------------------------------------
# Before/after per-tile stats table -- the redesigned replacement for
# mwax_calvin_quality.build_tile_summary_table/write_tile_summary_table,
# built for the whole observation (all files in a group) rather than one
# file, and driven by TileFlagReason/ChannelFlagReason instead of a single
# bad_array mask.
# ---------------------------------------------------------------------------


def _channel_reason_counts_text(tile_idx: int, channel_reasons: list[NDArray[np.object_]]) -> str:
    """Summarise a tile's per-channel flag reasons as counts, across all files.

    E.g. "AMPLITUDE_OUTLIER(20ch), NON_CONVERGED(4ch)" -- deliberately does
    NOT list every individual flagged channel, per the "counts per reason,
    not a full channel list" requirement.

    Args:
        tile_idx: Tile index (row position in the group's metafits_tiles_df).
        channel_reasons: One array per file, shape (n_tiles, n_chanblocks).

    Returns:
        A comma-separated "REASON(Nch)" string, empty if no channel-level
        reason is set anywhere for this tile.
    """
    counts: dict[str, int] = {}
    for file_reasons in channel_reasons:
        for reason in file_reasons[tile_idx]:
            if reason == ChannelFlagReason.NONE:
                continue
            for flag in ChannelFlagReason:
                if flag != ChannelFlagReason.NONE and reason & flag:
                    counts[flag.name] = counts.get(flag.name, 0) + 1
    return ", ".join(f"{name}({n}ch)" for name, n in counts.items())


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
            is (or will be) fully flagged -- e.g. combined_tile_flags for
            a "before" snapshot, or tile_flag_reasons != NONE for "after".
        tile_reasons: A TileFlagReason array, shape (n_tiles,) -- e.g.
            group.tile_flag_reasons at the point of this snapshot (a copy,
            if this isn't the group's current/final state).
        channel_reasons: One array per file, shape (n_tiles, n_chanblocks) --
            e.g. group.channel_flag_reasons at the point of this snapshot.
        phase_fits: Ideally already flavour-merged and outlier-annotated
            phase fits -- i.e. the output of
            mwax_calvin_utils.annotate_phase_outliers, computed against
            the same snapshot (pristine data for "before", final data
            for "after") -- NOT necessarily group.phase_fits, which
            reflects whatever detect_phase_outliers last computed. If
            'outlier' isn't present (e.g. a bare process_phase_fits()
            result), the 'phase_outlier' row field is silently left
            blank -- chi2dof/sigma_resid are read regardless.

    Returns:
        List of one dict per tile, matching the columns in
        write_tile_stats_table's expected row format.
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
            for file_jones, file_reasons in zip(jones_snapshot, channel_reasons):
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


def write_stats_and_debug_plots(
    group: HyperfitsSolutionGroup,
    refant_name: str,
    phase_fit_niter: int,
    output_path: str,
    obs_id: int,
    stats_fd,
    phase_outlier_nstd: float = 3.0,
) -> pd.DataFrame:
    """Write the before/after per-tile stats table and the
    phase-fit debug plots, for the group's final flagged state.

    Consolidates reporting previously duplicated between
    mwax_calvin_processor and cal_utils. Must be called after
    HyperfitsSolutionGroup.run_flagging_pipeline() has run -- its
    before_jones/before_tile_flag_reasons/before_channel_flag_reasons/
    before_phase_fits/phase_fits attributes are all required here.
    Typically called after commit() too, so the debug plots reflect data
    that's actually been written to disk (in-memory state is otherwise
    identical either side of commit()).

    The "after" phase fit is not recomputed here: group.phase_fits is
    already the final, fully-cleaned, flavour/outlier-annotated state,
    because run_flagging_pipeline() runs detect_phase_outliers() last
    for exactly this reason (see its docstring). This function reuses it
    directly for both build_tile_stats_rows (the stats.txt AFTER row's
    Flavor/PhOutlier columns) and plot_debug_phase_fits (the phase-fit
    debug plots), so the two reports can't disagree with each other --
    and, since phase fitting costs real time (roughly 2 minutes for a
    256-tile observation in testing), doesn't pay for a second fit of
    the same data. Only the "before" phase fit is annotated here, since
    it's a genuinely different (deliberately unflagged) snapshot that
    nothing else computes.

    Args:
        group: The solution group, after run_flagging_pipeline() (and
            typically commit()) have run.
        refant_name: Name of the reference antenna.
        phase_fit_niter: No longer used internally -- the "after" phase
            fit is reused from group.phase_fits rather than recomputed,
            so this has nothing left to control. Kept as a parameter
            only for call-site compatibility; existing callers can leave
            it as-is.
        output_path: Directory to write the {obs_id}_rx_lengths.png,
            _phase_fits_xx.png, _phase_fits_yy.png, _intercepts.png, and
            _residual.png debug plots into.
        obs_id: The observation ID, used for output filenames.
        stats_fd: Open file descriptor to write the before/after
            per-tile stats table into. Callers write this as the first
            section of the combined {obs_id}_stats.txt file, with
            write_hyperdrive_stats() convergence stats appended below.
        phase_outlier_nstd: Number of standard deviations beyond the
            population mean before a tile's phase fit is reported as an
            outlier -- only affects the BEFORE table's annotation now
            (the AFTER table/plots reuse group.phase_fits, already
            annotated with whatever nstd run_flagging_pipeline was given).
            Pass the same value to both calls, or the BEFORE and AFTER
            sections will silently reflect two different thresholds.
            Purely advisory -- does not affect flagging either way.

    Returns:
        group.phase_fits (the final, annotated phase fit DataFrame), so
        callers that also need it (e.g. for a DB insert) don't have to
        recompute it. Note this now includes the flavour/outlier
        annotation columns, unlike the bare process_phase_fits() result
        this used to return -- harmless for the DB-insert caller, which
        only reads specific known columns by name.
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
    annotated_after_phase_fits = group.phase_fits
    final_phase_fits = annotated_after_phase_fits

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
    write_tile_stats_table(
        f"{obs_id}: BEFORE any changes (unchanged hyperdrive solutions file)", before_rows, stats_fd
    )

    after_rows = build_tile_stats_rows(
        group,
        group.jones,
        after_tile_bad_mask,
        group.tile_flag_reasons,
        group.channel_flag_reasons,
        annotated_after_phase_fits,
    )
    write_tile_stats_table(f"{obs_id}: AFTER all Calvin flagging", after_rows, stats_fd)

    all_chanblocks_hz = group.all_chanblocks_hz_concat
    _, _noref_xx, _noref_yy, ref_xx, ref_yy = group.get_solns_both(refant_name)
    weights = group.weights
    plot_debug_phase_fits(
        annotated_after_phase_fits,
        tiles,
        all_chanblocks_hz,
        ref_xx,
        ref_yy,
        weights,
        prefix=os.path.join(output_path, f"{obs_id}_"),
        plot_residual=True,
        phase_outlier_nstd=phase_outlier_nstd,
    )

    return final_phase_fits
