"""Generate the illustrative plots used in CALVIN.md.

Uses synthetic, clearly-not-real data, but runs it through the actual
pipeline functions (fit_phase_line, iterative_poly_clip_batch, reject_outliers)
from mwax_calvin_utils so the plots reflect real algorithm behaviour, not
just a hand-drawn approximation of it.

NOTE: uses iterative_poly_clip_batch, not the per-tile iterative_poly_clip,
because the batch version is the one the production pipeline actually calls
(see HyperfitsSolutionGroup.flag_amplitude_outliers). The two differ slightly
in their zero-MAD handling, so illustrating with the per-tile version could
show behaviour the pipeline does not have.

Run from the repo root: python3 docs/img/make_illustrations.py
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, "src")

from mwax_mover.mwax_calvin_utils import (
    fit_phase_line,
    iterative_poly_clip_batch,
    reject_outliers,
)

rng = np.random.default_rng(42)

OUT = "docs/img"
plt.rcParams.update({"font.size": 11, "figure.dpi": 130})


# ---------------------------------------------------------------------------
# Step 3a: phase-vs-frequency, good tile vs. outlier (wrong-delay) tile
# ---------------------------------------------------------------------------
def step3a_phase_fits():
    n_chan = 200
    freqs_hz = np.linspace(150e6, 180e6, n_chan)
    weights = np.ones(n_chan)

    # Small, realistic delays chosen so phase does NOT wrap across this
    # band -- a real, well-calibrated tile's phase should vary smoothly
    # with frequency without wrapping; visible wrapping is itself usually
    # a sign something is wrong, not a normal feature to illustrate here.
    true_delay = 8e-9  # seconds -> a plausible short cable-length offset

    def make_solution(delay_s, noise):
        phase = 2 * np.pi * freqs_hz * delay_s
        noise_rad = rng.normal(0, noise, n_chan)
        return np.exp(1j * (phase + noise_rad))

    sol_good = make_solution(true_delay, noise=0.06)
    # A faulty connector/receiver adds far more scatter around the same
    # nominal delay line, which is what actually drives a bad
    # chi2dof/sigma_resid -- not a different slope or wrapping.
    sol_bad = make_solution(true_delay, noise=1.1)

    fit_good = fit_phase_line(freqs_hz, sol_good, weights, niter=3)
    fit_bad = fit_phase_line(freqs_hz, sol_bad, weights, niter=3)

    fig, axs = plt.subplots(1, 2, figsize=(11, 4), sharey=True)
    for ax, sol, fit, title, colour in [
        (axs[0], sol_good, fit_good, "Tile 104 (good)", "tab:blue"),
        (axs[1], sol_bad, fit_bad, "Tile 057 (phase outlier)", "tab:red"),
    ]:
        phase_unwrapped = np.angle(sol)
        ax.scatter(freqs_hz / 1e6, phase_unwrapped, s=6, alpha=0.5, color=colour, label="Solution (per channel)")
        c_light = 299792458.0  # m/s
        slope_rad_per_hz = 2 * np.pi * fit.length / c_light
        order = np.argsort(freqs_hz)
        freqs_sorted = freqs_hz[order]
        # Reconstruct the fitted model the same way fit_phase_line's own
        # objective does (as a complex exponential, then take its angle)
        # so it wraps to (-pi, pi] exactly like the real data -- these
        # delays are small enough that this doesn't introduce a
        # within-band wrap/discontinuity, unlike a naive
        # slope*freq + intercept line evaluated at large absolute
        # frequencies (which picks up a large, physically-meaningless
        # multiple of 2*pi).
        model_complex = np.exp(1j * (slope_rad_per_hz * freqs_sorted + fit.intercept))
        model_phase = np.angle(model_complex)
        ax.plot(
            freqs_sorted / 1e6,
            model_phase,
            color="black",
            lw=1.5,
            label=f"Fitted delay line\n(χ²/dof={fit.chi2dof:.2f}, σ={fit.sigma_resid:.2f} rad)",
        )
        ax.set_title(title)
        ax.set_xlabel("Frequency (MHz)")
        ax.set_ylabel("Phase (radians)")
        ax.legend(loc="lower right", fontsize=8)

    fig.suptitle("Step 3 — phase-vs-frequency delay fit: good tile vs. outlier tile (illustrative data)")
    fig.tight_layout()
    fig.savefig(f"{OUT}/step3a_phase_fit_example.png", bbox_inches="tight")
    plt.close(fig)
    return fit_good, fit_bad


# ---------------------------------------------------------------------------
# Step 3b: population scatter of chi2dof with median/MAD threshold, outlier tile marked
# ---------------------------------------------------------------------------
def step3b_population():
    n_tiles = 128
    # Most tiles cluster near chi2dof ~ 1 with normal-ish scatter.
    chi2dof = np.abs(rng.normal(1.0, 0.25, n_tiles))
    tile_ids = np.arange(1, n_tiles + 1)

    # Two illustrative bad tiles, well above the robust threshold. These
    # are independent, hand-picked illustrative values -- not required to
    # numerically match step3a_phase_fits's "Tile 057" example, which is
    # deliberately a lower-noise case chosen so its fitted delay line
    # stays visually clean (no within-band wrap).
    outlier_idx = [56, 91]
    outlier_labels = ["Tile 057", "Tile 092"]
    chi2dof[outlier_idx[0]] = 3.1
    chi2dof[outlier_idx[1]] = 4.2

    df = pd.DataFrame({"tile_id": tile_ids, "pol": "XX", "chi2dof": chi2dof})
    df = reject_outliers(df, "chi2dof", nstd=3.0)

    median = np.median(chi2dof)
    mad = np.median(np.abs(chi2dof - median))
    threshold = median + 3.0 * 1.4826 * mad

    fig, ax = plt.subplots(figsize=(9, 4.5))
    good = ~df["outlier"].to_numpy()
    bad = df["outlier"].to_numpy()
    ax.scatter(tile_ids[good], chi2dof[good], s=22, color="tab:blue", label="Tiles (not flagged)")
    ax.scatter(tile_ids[bad], chi2dof[bad], s=45, color="tab:red", zorder=3, label="Flagged (phase outlier)")
    ax.axhline(threshold, color="black", ls="--", lw=1.2, label=f"median + 3×MAD threshold ({threshold:.2f})")
    ax.axhline(median, color="grey", ls=":", lw=1, label=f"population median ({median:.2f})")
    for idx, label in zip(outlier_idx, outlier_labels):
        ax.annotate(
            label,
            (tile_ids[idx], chi2dof[idx]),
            textcoords="offset points",
            xytext=(8, 6),
        )
    ax.set_xlabel("Tile ID")
    ax.set_ylabel("χ²/dof (phase fit)")
    ax.set_title("Step 3 — population outlier test across all tiles (illustrative data)")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(f"{OUT}/step3b_population_outlier_test.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Step 4: gain amplitude vs channel, fitted curve, acceptance band, flagged RFI spikes
# ---------------------------------------------------------------------------
def step4_amplitude_outliers():
    n_chan = 300
    chan_idx = np.arange(n_chan, dtype=np.float64)

    # A smooth, physically-realistic bandpass shape (mild parabola) plus noise.
    true_curve = 1.0 + 0.15 * ((chan_idx - n_chan / 2) / (n_chan / 2)) ** 2
    noise = rng.normal(0, 0.02, n_chan)
    gx_amp = true_curve + noise

    # Inject a few narrowband RFI-like spikes.
    spike_channels = [40, 41, 140, 141, 142, 250]
    gx_amp[spike_channels] += rng.uniform(0.35, 0.55, len(spike_channels))

    # iterative_poly_clip_batch works on a batch of tiles, so present this
    # single illustrative trace as a batch of one and unwrap the results.
    initial_valid = np.ones((1, n_chan), dtype=bool)
    valid_b, _residual_b, fit_b, mad_b, med_b = iterative_poly_clip_batch(
        chan_idx, gx_amp[np.newaxis, :], degree=2, residual_threshold=10.0, initial_valid=initial_valid
    )
    valid, fit, mad, med = valid_b[0], fit_b[0], mad_b[0], med_b[0]

    band_lower = fit + med - 10.0 * mad
    band_upper = fit + med + 10.0 * mad

    fig, ax = plt.subplots(figsize=(10, 4.5))
    # Shade the full width of each flagged fine channel, so it's clear
    # a whole channel is excluded, not just a single point value.
    flagged_channels = chan_idx[~valid]
    for i, ch in enumerate(flagged_channels):
        ax.axvspan(
            ch - 0.5,
            ch + 0.5,
            color="tab:red",
            alpha=0.18,
            label="Flagged channel (amplitude outlier)" if i == 0 else None,
        )
    ax.scatter(chan_idx[valid], gx_amp[valid], s=10, color="tab:blue", label="Accepted channels")
    ax.scatter(chan_idx[~valid], gx_amp[~valid], s=40, color="tab:red", zorder=3, label="Flagged value")
    ax.plot(chan_idx, fit, color="black", lw=1.3, label="Fitted curve (degree-2 polynomial)")
    ax.fill_between(chan_idx, band_lower, band_upper, color="black", alpha=0.08, label="±10 MAD acceptance band")
    ax.set_xlabel("Channel number")
    ax.set_ylabel("Gain amplitude |gx|")
    ax.set_title("Step 4 — per-channel amplitude outlier flagging, one tile (illustrative data)")
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    fig.savefig(f"{OUT}/step4_amplitude_outliers.png", bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    step3a_phase_fits()
    step3b_population()
    step4_amplitude_outliers()
    print("Wrote illustrations to", OUT)
