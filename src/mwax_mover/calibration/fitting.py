"""Phase-ramp and gain fitting, and the numeric helpers they depend on.

fit_phase_line() fits a linear phase ramp to one tile/polarisation's
solution (using an exact analytic Hessian -- see _phase_fit_hess_inv --
rather than relying on scipy.optimize.minimize's own approximation).
fit_gain() fits gain amplitude vs. frequency. poly_str() formats fit
results for display (used directly by calvin.plots.phases too).
"""

import sys
import warnings

import numpy as np
from astropy import units as u
from astropy.constants import c  # ty: ignore[unresolved-import]
from numpy.typing import NDArray
from scipy.optimize import minimize

from mwax_mover.calibration.models import GainFitInfo, PhaseFitInfo


def pad_gains_to_full_coarse(
    values: list[float],
    actual_chans: list[int],
    expected_chans: NDArray[np.int_],
) -> list[float]:
    """Pad a per-coarse-channel list to match all expected metafits channels.

    Creates a list of length ``len(expected_chans)`` initialised to NaN, then
    places each value from *values* at the position of its corresponding coarse
    channel index in *expected_chans*.  Channels present in *expected_chans*
    but absent from *actual_chans* remain NaN.

    Args:
        values: Per-coarse-channel values, in the same order as *actual_chans*.
            Length must equal ``len(actual_chans)``.
        actual_chans: Coarse channel indices present in the calibration
            solutions, in the same order as *values*.
        expected_chans: All coarse channel indices from the metafits, sorted
            ascending.  Defines the length and ordering of the output.

    Returns:
        List of length ``len(expected_chans)`` with each value placed at the
        position of its channel in *expected_chans*, and NaN at positions for
        missing channels.
    """
    n_expected = len(expected_chans)
    padded: list[float] = [np.nan] * n_expected
    for i, chan_idx in enumerate(actual_chans):
        positions = np.where(expected_chans == chan_idx)[0]
        if len(positions) == 1:
            padded[positions[0]] = values[i]
    return padded


def pad_gain_fit_info(
    gain_fit: GainFitInfo,
    actual_coarse_chans: list[int],
    expected_coarse_chans: NDArray[np.int_],
) -> GainFitInfo:
    """Return a new GainFitInfo with all per-channel arrays padded to the full metafits channel set.

    Applies :func:`pad_gains_to_full_coarse` to the *gains*, *pol0*, *pol1*,
    and *sigma_resid* arrays of *gain_fit*, producing a new ``GainFitInfo``
    whose per-channel arrays have ``len(expected_coarse_chans)`` elements.
    The *quality* scalar is preserved unchanged.

    Args:
        gain_fit: Source ``GainFitInfo`` (or a pandas Series with the same
            named fields) whose per-channel arrays may have fewer elements
            than ``len(expected_coarse_chans)``.
        actual_coarse_chans: Sorted coarse channel indices present in the
            calibration solutions (same length as ``gain_fit.gains``).
        expected_coarse_chans: All coarse channel indices from the metafits,
            sorted ascending.

    Returns:
        New ``GainFitInfo`` with per-channel arrays of length
        ``len(expected_coarse_chans)``, NaN-padded at missing channels.
    """
    return GainFitInfo(
        quality=gain_fit.quality,
        gains=pad_gains_to_full_coarse(gain_fit.gains, actual_coarse_chans, expected_coarse_chans),
        pol0=pad_gains_to_full_coarse(gain_fit.pol0, actual_coarse_chans, expected_coarse_chans),
        pol1=pad_gains_to_full_coarse(gain_fit.pol1, actual_coarse_chans, expected_coarse_chans),
        sigma_resid=pad_gains_to_full_coarse(gain_fit.sigma_resid, actual_coarse_chans, expected_coarse_chans),
    )


def ensure_system_byte_order(arr):
    """Convert array to system byte order if needed.

    Args:
        arr: Input numpy array.

    Returns:
        Array converted to system byte order, or original if already correct.
    """
    system_byte_order = ">" if sys.byteorder == "big" else "<"
    if arr.dtype.byteorder not in f"{system_byte_order}|=":
        return arr.astype(arr.dtype.newbyteorder("="))
    return arr


def parse_csv_header(value: str, dtype: type) -> np.ndarray:
    """Parse comma-separated values from FITS header.

    Args:
        value: Comma-separated string values.
        dtype: Data type for the output array.

    Returns:
        Parsed array with the specified data type.
    """
    return np.array(value.split(","), dtype=dtype)


def wrap_angle(angle):
    """Wrap angle to the range [-π, π].

    Args:
        angle: Input angle(s) in radians.

    Returns:
        Wrapped angle(s) in the range [-π, π].
    """
    return np.mod(angle + np.pi, 2 * np.pi) - np.pi


# Floor for fit_phase_line's sigma-clip scale: the threshold is
# 2 * max(1.4826 * MAD, this), in radians. See its use in fit_phase_line for
# why the floor is needed.
_MIN_CLIP_THRESHOLD_RAD = 1e-6


def _phase_fit_hess_inv(freqs_hz: NDArray[np.float64]) -> NDArray[np.float64]:
    """Exact inverse Hessian of the phase-ramp fit objective w.r.t. (m, c).

    residual_i(m, c) = wrap(θ_i - m·ν_i - c) is piecewise-linear in (m, c)
    almost everywhere (wrap's derivative is exactly 1 a.e.; see
    wrap_angle), so for cost = Σ residual_i², the Gauss-Newton Hessian
    approximation (2·JᵗJ, where J is the residual Jacobian) is not an
    approximation here -- it's the exact Hessian, independent of (m, c)
    and of how many optimizer iterations were taken to get there.

    This replaces relying on scipy.optimize.minimize's own internal BFGS
    hess_inv, which is only a running approximation built up from
    gradient differences across iterations -- accurate after enough
    iterations, but meaningless if the optimizer (now given an exact
    analytic gradient, so converging in far fewer steps) terminates
    before that approximation has accumulated real curvature information.

    Args:
        freqs_hz: Frequencies (Hz) of the currently-valid points.

    Returns:
        The 2x2 inverse Hessian, ordered (m, c) to match `params`.
    """
    n = len(freqs_hz)
    sum_freqs = np.sum(freqs_hz)
    sum_freqs_sq = np.sum(freqs_hz**2)
    hessian = 2.0 * np.array([[sum_freqs_sq, sum_freqs], [sum_freqs, n]])
    return np.linalg.inv(hessian)


def fit_phase_line(
    freqs_hz: NDArray[np.float64],
    solution: NDArray[np.complex128],
    weights: NDArray[np.float64],
    niter: int = 1,
) -> PhaseFitInfo:
    """Fit a linear phase ramp to calibration solutions.

    Credit: Dr. Sammy McSweeny

    Args:
        freqs_hz: Array of frequencies in Hz.
        solution: Complex array of calibration solutions.
        weights: Array of weights for each solution.
        niter: Number of fitting iterations. Each iteration refits after
            rejecting outliers more than 2 robust scale units (median + MAD, see
            the sigma-clip comment in the loop below) from the median residual.
            Must be >= 1.

    Returns:
        PhaseFitInfo object containing fitted parameters and quality metrics.

    Raises:
        RuntimeError: If not enough valid phases are available to fit.
        ValueError: If niter is less than 1.
    """
    # Quality metrics for the phase fit:
    #
    # sigma_resid: Standard deviation of phase residuals (radians) after subtracting
    #              the best-fit model. Lower is better.
    #
    # chi2dof:     Chi-squared per degree of freedom = sum(residuals²) / (N - 2).
    #              Values near 1.0 indicate a good fit; much larger suggests poor fit
    #              or RFI; much smaller suggests over-fitting or too few points.
    #
    # stderr:      Standard error of the fitted slope m (rad/Hz), from the
    #              objective's exact analytic Hessian (see
    #              _phase_fit_hess_inv) scaled by residual variance. Not
    #              used elsewhere in the pipeline -- purely informational.
    #
    # quality:     Fraction of original frequency channels surviving the
    #              sigma-clip (|residual - median| < 2 * 1.4826 * MAD; see the
    #              detailed comment at the clip itself) (len(mask) / nfreqs).
    #              Ranges 0-1; 1.0 means all channels were used.

    # original number of frequencies
    nfreqs = len(freqs_hz)

    # sort by frequency
    ind = np.argsort(freqs_hz)
    freqs_hz = freqs_hz[ind]
    solution = solution[ind]
    weights = weights[ind]

    # Choose a suitable frequency bin width:
    # - Assume the frequencies are "quantised" (i.e. all integer multiples of some constant)
    # - Assume there is at least one example of a pair of consecutive bins present
    # - Do not assume the arrays are ordered in increasing frequency
    # Get the minimum difference between two (now-ordered) consecutive bins, and
    # declare this to be the bin width
    d_freq = np.min(np.diff(freqs_hz)) * u.Hz

    # remove nans and zero weights
    mask = np.where(np.logical_and(np.isfinite(solution), weights > 0))[0]

    if len(mask) < 2:
        raise RuntimeError(f"Not enough valid phases to fit ({len(mask)})")

    solution = solution[mask]
    freqs_hz = freqs_hz[mask]
    weights = weights[mask]

    # normalise
    solution /= np.abs(solution)
    solution *= weights

    # print(f"{np.angle(solution)[:4]=}, ")

    # Now we want to "adjust" the solution data so that it
    # - is roughly centered on the DC bin
    # - has a large amount of zero padding on either side
    freqs_hz = freqs_hz * u.Hz

    bins = np.round((freqs_hz / d_freq).decompose().value).astype(int)
    ctr_bin = (np.min(bins) + np.max(bins)) // 2
    shifted_bins = bins - ctr_bin  # Now "bins" represents where I want to put the solution values

    # ...except that ~1/2 of them are negative, so I'll have to add a certain amount
    # once I decide how much zero padding to include.
    # This is set by the resolution I want in delay space (Nyquist rate)
    dm = 0.01 * u.m
    dt = dm / c  # The target time resolution
    nyquist_freq_hz = 0.5 / dt  # The Nyquist rate
    N = 2 * int(np.round(nyquist_freq_hz / d_freq))  # The number of bins to use during the FFTs

    shifted_bins[shifted_bins < 0] += (
        N  # Now the "negative" frequencies are put at the end, which is where FFT wants them
    )

    # Create a zero-padded, shifted version of the spectrum, which I'll call sol0
    # sol0: This shifts the non-zero data down to a set of frequencies straddling the DC bin.
    # This makes the peak in delay space broad, and lets us hone in near the optimal solution by
    # finding the peak in delay space
    sol0 = np.zeros((N,)).astype(complex)
    sol0[shifted_bins] = solution

    # IFFT of sol0 to get the approximate solution as the peak in delay space
    isol0 = np.fft.ifft(sol0)
    t = -np.fft.fftfreq(len(sol0), d=d_freq.to(u.Hz).value) * u.s  # (Not sure why this negative is needed)
    d = np.fft.fftshift(c * t)
    isol0 = np.fft.fftshift(isol0)

    # Find max peak, and the equivalent slope
    imax = np.argmax(np.abs(isol0))
    dmax = d[imax]

    # print(f"{dmax=:.02f}")

    slope = (2 * np.pi * u.rad * dmax / c).to(u.rad / u.Hz)

    # print(f"{slope=:.10f}")

    # Now that we're near a local minimum, get a better one by doing a standard minimisation
    # To get the y-intercept, divide the original data by the constructed data
    # and find the average phase of the result

    def model(freqs_hz, m, c):
        return np.exp(1j * (m * freqs_hz + c))

    y_int = np.angle(np.mean(solution / model(freqs_hz.to(u.Hz).value, slope.value, 0)))
    params = (slope.value, y_int)

    def objective_and_grad(params, freqs_hz, data):
        # Combines cost and its exact gradient into one call (jac=True
        # below) so minimize() never falls back to finite-difference
        # gradient estimation -- which was re-evaluating this same
        # objective ~500+ times per fit (once per finite-difference step,
        # repeated by BFGS's line search) and dominating overall runtime.
        #
        # wrap_angle(x) = mod(x + pi, 2*pi) - pi has derivative exactly 1
        # almost everywhere (it's flat with slope 1 between discontinuous
        # -2*pi jumps at the wrap points, a measure-zero set the
        # optimizer won't land on), so d(residual_i)/dm = -ν_i and
        # d(residual_i)/dc = -1, giving:
        #   d(cost)/dm = -2 * sum(residual_i * ν_i)
        #   d(cost)/dc = -2 * sum(residual_i)
        constructed = model(freqs_hz, *params)
        residuals = wrap_angle(np.angle(data) - np.angle(constructed))
        cost = np.sum(np.abs(residuals) ** 2)
        grad = np.array([-2.0 * np.sum(residuals * freqs_hz), -2.0 * np.sum(residuals)])
        return cost, grad

    if niter < 1:
        raise ValueError(f"niter must be >= 1, got {niter}")

    # Initialised to NaN so the type is always float/ndarray at the return site
    # regardless of early-exit paths.  The loop is guaranteed to run at least
    # once (niter >= 1), so these will always be overwritten before use.
    resid_std: float = np.nan
    chi2dof: float = np.nan
    stderr: NDArray[np.float64] = np.array([np.nan])

    while niter > 0:
        niter -= 1
        # A line-search failure inside minimize() doesn't stop it from
        # returning a result -- just possibly a worse one, which the
        # chi2dof/sigma_resid quality metrics below already reflect. The
        # resulting LineSearchWarning is expected noise for a difficult
        # fit, not a sign minimize() failed outright.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="The line search algorithm did not converge", category=RuntimeWarning
            )
            res = minimize(objective_and_grad, params, args=(freqs_hz.to(u.Hz).value, solution), jac=True)
        params = res.x

        constructed = model(freqs_hz.to(u.Hz).value, *params)
        residuals = wrap_angle(np.angle(solution) - np.angle(constructed))
        chi2dof = np.sum(np.abs(residuals) ** 2) / (len(residuals) - len(params))
        resid_std = residuals.std()
        resid_var = residuals.var(ddof=len(params))
        stderr = np.sqrt(np.diag(_phase_fit_hess_inv(freqs_hz.to(u.Hz).value)) * resid_var)

        # Sigma-clip using a robust median+MAD scale of residuals
        # (radians), not stderr[0] (rad/Hz). stderr[0] is the standard
        # error of the fitted SLOPE m, not a residual-scale quantity --
        # comparing it against |residuals| (also radians) is a units
        # mismatch (rad/Hz vs rad). It used to "work" only by a numerical
        # coincidence specific to scipy.optimize.minimize's default
        # (numerical-gradient) BFGS: on this badly-conditioned problem
        # (m ~1e-9, c ~O(1)), BFGS's own hess_inv approximation never
        # moves far from its near-identity starting point, which happens
        # to make stderr[0] land close to resid_std anyway -- not because
        # it reflects m's true uncertainty, but because BFGS's
        # bookkeeping stays close to its own initial scale. That
        # coincidence breaks once an exact analytic gradient lets the
        # optimizer converge in far fewer iterations: stderr[0] (now
        # computed exactly via _phase_fit_hess_inv, decoupled from BFGS's
        # convergence path) correctly reflects m's real, tiny rad/Hz-
        # scale uncertainty, which is meaningless as a radians threshold.
        #
        # resid_std itself can collapse towards machine noise for a
        # (near-)exact fit -- e.g. clean synthetic data, or any tile
        # whose residuals converge extremely tightly -- which would
        # otherwise make the clipping threshold degenerate (almost no
        # residual satisfies it, tripping the "too few points" early
        # exit and reporting near-zero quality for an excellent fit).
        # _MIN_CLIP_THRESHOLD_RAD guards against that; it's far below any
        # physically meaningful phase noise, so it has no effect on
        # realistic (noisy) data where resid_std is orders of magnitude
        # larger than this.
        #
        # The clip itself uses a robust median + MAD scale, not resid_std
        # and not zero as the comparison centre. Two related reasons:
        #
        # 1. std is not robust to the very outliers it's meant to catch
        #    (the same masking/swamping issue fixed in reject_outliers):
        #    a large contaminated fraction inflates std in proportion to
        #    its own presence, so the "2*std" threshold grows permissive
        #    exactly when it should tighten. E.g. with 25% of channels
        #    flipped by pi, the outlier residuals (~2.1-2.6 rad) and the
        #    clean residuals (~-0.55 to -1.0 rad) separate cleanly, but
        #    2*resid_std (~2.7 rad) ends up just barely above the
        #    outliers' own max -- letting them all survive by
        #    coincidence, not because they're not outliers.
        # 2. A single non-robust least-squares fit over contaminated data
        #    is itself biased towards the outliers, so even the CLEAN
        #    residuals end up centred away from zero (e.g. ~-0.7 rad
        #    above, not 0). Comparing |residuals| against a threshold
        #    (implicitly centred at zero) would then wrongly reject the
        #    clean majority too; centring on the residuals' own median
        #    instead correctly separates the two groups regardless of
        #    where the (biased) fit put them.
        resid_median = np.median(residuals)
        resid_mad = np.median(np.abs(residuals - resid_median))
        clip_scale = max(1.4826 * resid_mad, _MIN_CLIP_THRESHOLD_RAD)
        mask = np.where(np.abs(residuals - resid_median) < 2 * clip_scale)[0]
        if len(mask) < 2:
            break
        solution = solution[mask]
        freqs_hz = freqs_hz[mask]

    period = ((params[0] * u.rad / u.Hz) / (2 * np.pi * u.rad)).to(u.s)
    quality = len(mask) / nfreqs

    return PhaseFitInfo(
        length=(c * period).to(u.m).value,
        intercept=wrap_angle(params[1]),
        sigma_resid=resid_std,
        chi2dof=chi2dof,
        quality=quality,
        stderr=stderr[0],
        # median_thickness=median_thickness,
    )


def fit_gain(chanblocks_hz, solns, weights, chanblocks_per_coarse: int) -> GainFitInfo:
    """Fit gain solutions across frequency channels.

    Args:
        chanblocks_hz: Frequency of each channel block in Hz.
        solns: Gain solutions (amplitudes).
        weights: Weights for each solution.
        chanblocks_per_coarse: Number of channel blocks per coarse channel.

    Returns:
        GainFitInfo object containing fitted gains and quality metrics.
        See GainFitInfo's docstring -- in particular, its pol0/pol1
        fields are polynomial-fit coefficients, not polarisation labels.
    """
    # length check- should be the number of fine channels
    n_freqs = len(chanblocks_hz)
    assert n_freqs == len(solns) == len(weights)
    # This is our output number of channels
    n_coarse = n_freqs // chanblocks_per_coarse

    # Take the absolute value of the amplitudes
    amps = np.abs(solns)

    # Initialize output arrays
    gains = np.full(n_coarse, np.nan)
    pol0 = np.full(n_coarse, np.nan)
    pol1 = np.full(n_coarse, np.nan)
    sigma_resid = np.full(n_coarse, np.nan)

    # Initialise quality accumulator
    n_within: int = 0
    quality: float = np.nan

    # split chans, solns, weights into chunks of chanblocks_per_coarse
    for coarse_idx, (
        coarse_hz,
        coarse_amps,
        coarse_weights,
    ) in enumerate(
        zip(
            np.split(chanblocks_hz, n_coarse),
            np.split(amps, n_coarse),
            np.split(weights, n_coarse),
            strict=True,
        )
    ):
        # remove nans and zero weights
        coarse_mask = np.where(np.logical_and(np.isfinite(coarse_amps), coarse_weights > 0))[0]
        if len(coarse_mask) < 2:
            continue

        # Apply mask to arrays to remove nans and zero weights
        # Remember these arrays are as big as the number of fine channels per coarse
        coarse_amps = coarse_amps[coarse_mask]
        # Invert the gains since we already negate the phase
        coarse_amps = 1 / coarse_amps

        coarse_hz = coarse_hz[coarse_mask]
        coarse_weights = coarse_weights[coarse_mask]

        # Calculate the weighted mean of the amplitudes for this coarse channel
        gains[coarse_idx] = np.sum(coarse_amps * coarse_weights) / np.sum(coarse_weights)

        # Fit 1st order polynomial to get pol0, pol1, sigma_resid
        coeffs = np.polyfit(coarse_hz, coarse_amps, deg=1, w=coarse_weights)
        pol1[coarse_idx] = coeffs[0]  # slope
        pol0[coarse_idx] = coeffs[1]  # intercept

        # Compute residuals from the polynomial fit
        fitted = np.polyval(coeffs, coarse_hz)
        residuals = coarse_amps - fitted
        sigma_resid[coarse_idx] = residuals.std()

        if sigma_resid[coarse_idx] < 1e-10:
            # If sigma_resid is very small then we can say all are within 2 sigma
            n_within += len(residuals)
        else:
            # Accumulate chanblocks within 2*sigma_resid of the fit for quality
            n_within += int(np.sum(np.abs(residuals) < 2 * sigma_resid[coarse_idx]))

    # Quality is the fraction of all chanblocks (including flagged) within 2*sigma_resid
    quality = n_within / n_freqs

    return GainFitInfo(
        quality=quality,
        gains=gains.tolist(),
        pol0=pol0.tolist(),
        pol1=pol1.tolist(),
        sigma_resid=sigma_resid.tolist(),
    )


def poly_str(coeffs, independent_var="x"):
    """Format polynomial coefficients as a string expression.

    Args:
        coeffs: Polynomial coefficients (highest order first).
        independent_var: Name of the independent variable (default: 'x').

    Returns:
        Formatted polynomial expression string.
    """

    def xpow(i):
        if i == 0:
            return ""
        elif i == 1:
            return f"×{independent_var}"
        else:
            return f"×{independent_var}" + "⁰¹²³⁴⁵⁶⁷⁸⁹"[i]

    return " ".join(
        filter(None, [f"{coeff:+.3}{xpow(i)}" for i, coeff in enumerate(coeffs[::-1])])
        # if abs(coeff) > 1e-20 else ""
    )
