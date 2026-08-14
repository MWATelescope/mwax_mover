"""Calibration support utilities for the Calvin pipeline.

Provides data structures (Tile, Metafits, HyperfitsSolution, HyperfitsSolutionGroup,
GainFitInfo, PhaseFitInfo), the CalvinJobType enum (realtime / mwa_asvo), AOCAL
binary format constants and read/write helpers, SBATCH script generation
(create_sbatch_script) and submission (submit_sbatch), phase and gain fitting
functions, and estimate_birli_output_bytes() for storage pre-checks.
"""

import datetime
import glob
import json
import logging
import mimetypes
import os
import re
import shutil
import sys
import time
from enum import Enum
from pathlib import Path
from typing import NamedTuple

import matplotlib as mpl
import numpy as np
import numpy.typing as npt  # noqa: F401
import pandas as pd
import seaborn as sns
from astropy import units as u
from astropy.constants import c  # ty: ignore[unresolved-import]
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from mwalib import MetafitsContext
from numpy.typing import ArrayLike, NDArray
from scipy.optimize import minimize

from mwax_mover.mwax_command import (
    check_popen_finished,
    run_command_ext,
    run_command_popen,
)
from mwax_mover.utils import (
    delete_files_older_than,
    extract_channels_from_filename,
    get_png_dimensions,
    is_int,
)

logger = logging.getLogger(__name__)

V_LIGHT_M_S = 299792458.0

AOCAL_INTRO = b"MWAOCAL\0"
AOCAL_FILE_TYPE = 0
AOCAL_STRUCTURE_TYPE = 0
AOCAL_INTERVAL_COUNT = 1
AOCAL_POLS = 4  # XX,XY,YX,YY
AOCAL_VALUES = 2  # real and imaginary
DOUBLE_SIZE = 8  # 8 bytes for a float64
AOCAL_HEADER_FORMAT = "<8s6I2d"

# Standard number of MWA coarse channels.
MWA_NUM_COARSE_CHANS = 24


def read_solutions_hdu_complex(solutions_data: NDArray[np.float64]) -> NDArray[np.complex128]:
    """Convert a raw hyperdrive SOLUTIONS HDU array into complex Jones terms.

    The SOLUTIONS HDU stores each of the four polarisation terms (XX, XY, YX,
    YY -- equivalently gx, Dx, Dy, gy in hyperdrive's own Jones-matrix
    notation) as a consecutive (real, imag) float64 pair, giving 8 floats per
    (timeblock, tile, chanblock) entry. This collapses those 8 floats into 4
    complex values without altering any leading axes, so it works whether
    the caller wants a single timeblock or all of them.

    Args:
        solutions_data: Raw SOLUTIONS HDU data, shape (..., 8) -- typically
            (timeblock, tile, chanblock, 8).

    Returns:
        Complex128 array, shape (..., 4), trailing axis ordered
        [XX, XY, YX, YY].
    """
    data = np.asarray(solutions_data, dtype=np.float64)
    return data[..., 0::2] + 1j * data[..., 1::2]


def read_results_hdu(results_data: NDArray[np.float64], timeblock: int = 0) -> NDArray[np.float64]:
    """Get one timeblock's convergence precision values from a RESULTS HDU.

    RESULTS is a plain FITS ImageHDU (not a binary table), shape
    (n_timeblocks, n_chanblocks). This selects a single timeblock's row.
    Solution files with more than one timeblock are not currently supported
    by this pipeline (see the single-timeblock assumption enforced
    elsewhere, e.g. HyperfitsSolutionGroup.get_solns); this helper always
    returns exactly one timeblock's row rather than merging several.

    Args:
        results_data: Raw RESULTS HDU data, shape (n_timeblocks, n_chanblocks).
        timeblock: Which timeblock's row to return. Defaults to 0.

    Returns:
        1-D float64 array of convergence precision per chanblock, for the
        requested timeblock. NaN entries mean that chanblock failed to
        converge or was pre-flagged.
    """
    return np.asarray(results_data, dtype=np.float64)[timeblock]


def read_tiles_hdu(tiles_data) -> tuple[NDArray[np.int_], list[str], NDArray[np.bool_]]:
    """Read tile antenna indices, names, and flags from a TILES HDU.

    Antenna indices should already be ascending in practice, but this sorts
    explicitly to guarantee alignment with the SOLUTIONS HDU's tile axis
    regardless, in case a file ever has them out of order.

    Args:
        tiles_data: Raw TILES HDU data (a FITS binary table with Antenna,
            TileName, and Flag columns).

    Returns:
        A tuple (antennas, tile_names, flags), each ordered by ascending
        antenna index:
        - antennas: int array of antenna indices.
        - tile_names: list of tile name strings.
        - flags: bool array of tile flags (True = flagged).
    """
    antennas = np.asarray(tiles_data["Antenna"])
    order = np.argsort(antennas)
    tile_names = [str(tiles_data["TileName"][i]) for i in order]
    flags = np.asarray(tiles_data["Flag"])[order].astype(bool)
    return antennas[order], tile_names, flags


def read_baseline_tile_flags(baseline_weights: NDArray[np.float64], n_tiles: int) -> NDArray[np.bool_]:
    """Infer per-tile flagging from a BASELINES HDU's NaN pattern.

    Baselines are ordered ascending: (0,1), (0,2), ..., (0,N-1), (1,2), ...
    (autocorrelations are not included in this HDU). A NaN weight on any
    baseline involving a tile means that tile was flagged for the solve.

    This is a third, independent source of tile flagging alongside the
    TILES HDU's own Flag column and the metafits flag column: all three
    come from the same hyperdrive run, but this one is derived rather than
    stored directly, so it's worth keeping as a separate check in case it
    and the TILES HDU flag column ever disagree within one file.

    Args:
        baseline_weights: 1D array of baseline weights (NaN = flagged).
        n_tiles: Number of tiles/antennas in the observation.

    Returns:
        Boolean array of shape (n_tiles,), True where the tile is flagged.
    """
    tile_flagged = np.zeros(n_tiles, dtype=bool)
    idx = 0
    for i in range(n_tiles):
        for j in range(i + 1, n_tiles):
            if np.isnan(baseline_weights[idx]):
                tile_flagged[i] = True
                tile_flagged[j] = True
            idx += 1
    return tile_flagged


class CalvinJobType(Enum):
    """Calvin Job Type"""

    realtime = "realtime"
    mwa_asvo = "mwa_asvo"


class Tile(NamedTuple):
    """Info about an MWA tile"""

    name: str
    id: int
    flag: bool
    # index: int
    rx: int
    slot: int
    flavor: str = ""


class Input(NamedTuple):
    """Info about an MWA tile"""

    name: str
    id: int
    flag: bool
    # index: int
    pol: str
    rx: int
    slot: int
    length: float
    flavor: str = ""


class ChanInfo(NamedTuple):
    """channel selection info"""

    coarse_chan_ranges: list[NDArray[np.int_]]  # List[Tuple(int, int)]
    fine_chans_per_coarse: int
    fine_chan_width_hz: float


class TimeInfo(NamedTuple):
    """timestep info"""

    num_times: int
    int_time_s: float


class Metafits:
    """MWA Metadata reader backed by mwalib MetafitsContext.

    Replaces the former astropy FITS implementation.  All properties now
    delegate to ``mwalib.MetafitsContext`` instead of opening the FITS file
    directly, which removes several manual parsing steps and CHANSEL sanity
    checks that mwalib already validates internally.
    """

    def __init__(self, metafits: str | MetafitsContext):
        """Initialise a Metafits reader backed by mwalib.

        Args:
            metafits: Path to the metafits FITS file, or an already-opened
                MetafitsContext.  Passing an existing context avoids re-opening
                the file when the caller already holds one.
        """
        if isinstance(metafits, str):
            self.filename = metafits
            self._mc: MetafitsContext = MetafitsContext(metafits, None)
        else:
            self.filename = metafits.metafits_filename
            self._mc = metafits

    @property
    def mwalib_context(self) -> MetafitsContext:
        """Get the underlying mwalib MetafitsContext.

        For callers that need the raw context directly -- e.g.
        add_digital_gains_column, which reads rf_inputs digital gains --
        rather than one of this class's own derived properties.
        """
        return self._mc

    @property
    def tiles(self) -> list[Tile]:
        """Get tile information from metafits, sorted by tile ID.

        mwalib exposes one Antenna per tile (not duplicated per pol), so
        no set-based deduplication is needed.  Flag, rx, and slot come from
        rfinput_x (identical to rfinput_y for those fields).
        """
        return sorted(
            [
                Tile(
                    name=ant.tile_name,
                    id=ant.tile_id,
                    flag=bool(ant.rfinput_x.flagged),
                    rx=ant.rfinput_x.rec_number,
                    slot=ant.rfinput_x.rec_slot_number,
                    flavor=str(ant.rfinput_x.rec_type),
                )
                for ant in self._mc.antennas
            ],
            key=lambda tile: tile.id,
        )

    @property
    def inputs(self) -> list[Input]:
        """Get input (rf_input) information from metafits, sorted by input index.

        mwalib exposes one Rfinput per polarisation per tile, so no
        set-based deduplication is needed.  The electrical length is already
        a float (metres) — the ``"EL_"`` prefix stripping from the old FITS
        read is not required.
        """
        return sorted(
            [
                Input(
                    id=rfi.input,
                    name=rfi.tile_name + str(rfi.pol),
                    flag=bool(rfi.flagged),
                    pol=str(rfi.pol),
                    rx=rfi.rec_number,
                    slot=rfi.rec_slot_number,
                    length=rfi.electrical_length_m,
                    flavor=str(rfi.rec_type),
                )
                for rfi in self._mc.rf_inputs
            ],
            key=lambda inp: inp.id,
        )

    @property
    def tiles_df(self) -> pd.DataFrame:
        """Get tiles as a pandas DataFrame."""
        return pd.DataFrame(self.tiles, columns=Tile._fields)

    @property
    def inputs_df(self) -> pd.DataFrame:
        """Get inputs as a pandas DataFrame."""
        return pd.DataFrame(self.inputs, columns=Input._fields)

    @property
    def chan_info(self) -> ChanInfo:
        """Get coarse channel information from metafits.

        mwalib validates CHANNELS, CHANSEL, FINECHAN and their mutual
        consistency internally, so the former sanity checks and the CHANSEL
        length comparison are not reproduced here.
        """
        coarse_chans = np.sort([c.rec_chan_number for c in self._mc.metafits_coarse_chans])
        fine_chan_width_hz = self._mc.corr_fine_chan_width_hz
        fine_chans_per_coarse = self._mc.num_corr_fine_chans_per_coarse

        coarse_chan_ranges = [g for g in np.split(coarse_chans, np.where(np.diff(coarse_chans) != 1)[0] + 1)]

        return ChanInfo(
            coarse_chan_ranges=coarse_chan_ranges,
            fine_chan_width_hz=fine_chan_width_hz,
            fine_chans_per_coarse=fine_chans_per_coarse,
        )

    @property
    def time_info(self) -> TimeInfo:
        """Get time information from metafits."""
        return TimeInfo(
            num_times=self._mc.num_metafits_timesteps,
            int_time_s=self._mc.corr_int_time_ms / 1000.0,
        )

    @property
    def calibrator(self) -> str | None:
        """Get calibrator source name from metafits.

        Returns None when the metafits carries an empty CALIBSRC string.
        """
        return self._mc.calibrator_source or None

    @property
    def obsid(self) -> int:
        """Get observation ID (GPS time) from metafits."""
        return self._mc.obs_id


class PhaseFitInfo(NamedTuple):
    length: float
    # Intercept is in radians
    intercept: float
    sigma_resid: float
    chi2dof: float
    quality: float
    stderr: float

    # median_thickness: float

    # def get_length(self) -> float:
    #     """The equivalent cable length of the phase ramp"""
    #     return v_light_m_s / self.slope

    @staticmethod
    def nan():
        return PhaseFitInfo(
            length=np.nan,
            intercept=np.nan,
            sigma_resid=np.nan,
            chi2dof=np.nan,
            quality=np.nan,
            stderr=np.nan,
            # median_thickness=np.nan,
        )


class GainFitInfo(NamedTuple):
    quality: float
    gains: list[float]
    pol0: list[float]
    pol1: list[float]
    sigma_resid: list[float]

    @staticmethod
    def default(n_coarse: int = MWA_NUM_COARSE_CHANS) -> "GainFitInfo":
        """Return a GainFitInfo with unit gains and zero offsets.

        Args:
            n_coarse: Number of coarse channels. Defaults to MWA_NUM_COARSE_CHANS (24).
        """
        return GainFitInfo(
            quality=1.0,
            gains=[1.0] * n_coarse,
            pol0=[0.0] * n_coarse,
            pol1=[0.0] * n_coarse,
            sigma_resid=[0.0] * n_coarse,
        )

    @staticmethod
    def nan(n_coarse: int = MWA_NUM_COARSE_CHANS) -> "GainFitInfo":
        """Return a GainFitInfo with all-NaN values.

        Args:
            n_coarse: Number of coarse channels. Defaults to MWA_NUM_COARSE_CHANS (24).
        """
        return GainFitInfo(
            quality=np.nan,
            gains=[np.nan] * n_coarse,
            pol0=[np.nan] * n_coarse,
            pol1=[np.nan] * n_coarse,
            sigma_resid=[np.nan] * n_coarse,
        )


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
        return np.frombuffer(arr.tobytes(), dtype=arr.dtype.newbyteorder("="))
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


def fit_phase_line(
    freqs_hz: NDArray[np.float64],
    solution: NDArray[np.complex128],
    weights: NDArray[np.float64],
    niter: int = 1,
    fit_iono: bool = False,
    # chanblocks_per_coarse: int,
    # bin_size: int = 10,
    # typical_thickness: float = 3.9,
) -> PhaseFitInfo:
    """Fit a linear phase ramp to calibration solutions.

    Credit: Dr. Sammy McSweeny

    Args:
        freqs_hz: Array of frequencies in Hz.
        solution: Complex array of calibration solutions.
        weights: Array of weights for each solution.
        niter: Number of fitting iterations. Each iteration refits after
            rejecting outliers beyond 2*stderr. Must be >= 1.
        fit_iono: Whether to fit ionospheric dispersion (currently unused).

    Returns:
        PhaseFitInfo object containing fitted parameters and quality metrics.

    Raises:
        RuntimeError: If not enough valid phases are available to fit.
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
    # stderr:      Standard error on the fitted slope (rad/Hz), estimated from the
    #              optimiser's inverse Hessian scaled by residual variance. Used above
    #              to sigma-clip outlier channels (|residual| < 2 * stderr[0]).
    #
    # quality:     Fraction of original frequency channels surviving the sigma-clip
    #              (len(mask) / nfreqs). Ranges 0–1; 1.0 means all channels were used.

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
    dν = np.min(np.diff(freqs_hz)) * u.Hz

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
    ν = freqs_hz * u.Hz

    bins = np.round((ν / dν).decompose().value).astype(int)
    ctr_bin = (np.min(bins) + np.max(bins)) // 2
    shifted_bins = bins - ctr_bin  # Now "bins" represents where I want to put the solution values

    # ...except that ~1/2 of them are negative, so I'll have to add a certain amount
    # once I decide how much zero padding to include.
    # This is set by the resolution I want in delay space (Nyquist rate)
    dm = 0.01 * u.m
    dt = dm / c  # The target time resolution
    νmax = 0.5 / dt  # The Nyquist rate
    N = 2 * int(np.round(νmax / dν))  # The number of bins to use during the FFTs

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
    t = -np.fft.fftfreq(len(sol0), d=dν.to(u.Hz).value) * u.s  # (Not sure why this negative is needed)
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

    # if fit_iono:
    #     model = lambda ν, m, c, α: np.exp(1j * (m * ν + c + α / ν**2))
    #     y_int = np.angle(np.mean(solution / model(ν.to(u.Hz).value, slope.value, 0, 0)))
    #     params = (slope.value, y_int, 0)

    def model(ν, m, c):
        return np.exp(1j * (m * ν + c))

    y_int = np.angle(np.mean(solution / model(ν.to(u.Hz).value, slope.value, 0)))
    params = (slope.value, y_int)

    def objective(params, ν, data):
        constructed = model(ν, *params)
        residuals = wrap_angle(np.angle(data) - np.angle(constructed))
        cost = np.sum(np.abs(residuals) ** 2)
        return cost

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
        res = minimize(objective, params, args=(ν.to(u.Hz).value, solution))
        params = res.x

        constructed = model(ν.to(u.Hz).value, *params)
        residuals = wrap_angle(np.angle(solution) - np.angle(constructed))
        chi2dof = np.sum(np.abs(residuals) ** 2) / (len(residuals) - len(params))
        resid_std = residuals.std()
        resid_var = residuals.var(ddof=len(params))
        stderr = np.sqrt(np.diag(res.hess_inv * resid_var))

        mask = np.where(np.abs(residuals) < 2 * stderr[0])[0]
        if len(mask) < 2:
            break
        solution = solution[mask]
        ν = ν[mask]

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


def iterative_poly_clip(
    x: np.ndarray,
    y: np.ndarray,
    degree: int,
    residual_threshold: float,
    initial_valid: np.ndarray,
    max_iter: int = 10,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float]:
    """Fit a robust, sigma-clipped polynomial to y(x) and flag outliers.

    Iteratively fits a degree-N polynomial on the currently-valid points,
    computes residuals against that fit, rejects points whose residual
    exceeds residual_threshold MADs (median absolute deviations) from the
    median residual, and refits -- repeating until the valid set stops
    changing (or max_iter is reached). This guards against a single
    extreme outlier dragging a one-shot least-squares fit far enough off
    course that it masks the very outlier it should catch.

    Ported from the now-deleted mwax_calvin_quality._iterative_poly_clip as
    a standalone pure function, for reuse by
    HyperfitsSolutionGroup.flag_amplitude_outliers.

    Args:
        x: 1D array of independent variable values (e.g. chanblock index).
        y: 1D array of dependent variable values (e.g. gain amplitude).
        degree: Polynomial degree to fit.
        residual_threshold: Number of residual-MADs beyond which a point
            is considered an outlier. Dimensionless -- e.g. 5.0 means "5x
            the typical residual scatter for this tile/pol", not an
            absolute gain value.
        initial_valid: Boolean mask of points eligible to be fit at all
            (e.g. already excludes points flagged for unrelated reasons
            like non-convergence). Outliers found here are only ever a
            subset of this mask.
        max_iter: Maximum number of fit/clip iterations.

    Returns:
        A tuple (valid, residual, fit, mad, med):
        - valid: Boolean array, True for points considered good (within
          initial_valid and not rejected as an outlier).
        - residual: Float array, |y - fit - median_residual| / mad at
          every point (including points outside initial_valid, computed
          against the final fit). NaN everywhere if no fit could ever be
          computed (too few valid points).
        - fit: Float array, the final polynomial fit evaluated at every x.
          NaN everywhere if no fit could be computed at all.
        - mad: The median absolute deviation of residuals from the final
          fit iteration (scalar, same units as y). NaN if no fit could be
          computed.
        - med: The median residual from the final fit iteration (scalar,
          same units as y). NaN if no fit could be computed.
    """
    n = len(y)
    valid = initial_valid.copy()
    residual = np.full(n, np.nan, dtype=np.float64)
    fit = np.full(n, np.nan, dtype=np.float64)
    mad = np.nan
    med = np.nan

    if valid.sum() < degree + 2:
        return valid, residual, fit, mad, med

    for _ in range(max_iter):
        coeffs = np.polyfit(x[valid], y[valid], degree)
        fit = np.polyval(coeffs, x)
        resid_all = y - fit

        med = np.median(resid_all[valid])
        mad = np.median(np.abs(resid_all[valid] - med))
        if mad == 0:
            residual[:] = 0.0
            break

        residual = np.abs(resid_all - med) / mad
        new_valid = initial_valid & (residual <= residual_threshold)

        if new_valid.sum() < degree + 2:
            break
        if np.array_equal(new_valid, valid):
            valid = new_valid
            break
        valid = new_valid

    return valid, residual, fit, mad, med


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


def textwrap(s, width=70):
    """Wrap text to a specified width.

    Args:
        s: Input string to wrap.
        width: Maximum line width in characters (default: 70).

    Returns:
        Wrapped text with lines joined by newlines.
    """
    words = s.split()
    lines = []
    current_line = []
    current_length = 0

    for word in words:
        if current_length + len(word) <= width:
            current_line.append(word)
            current_length += len(word) + 1  # +1 for the space
        else:
            lines.append(" ".join(current_line))
            current_line = [word]
            current_length = len(word)

    lines.append(" ".join(current_line))
    return "\n".join(lines)


def debug_phase_fits(
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
) -> pd.DataFrame | None:
    """Generate debug plots and analysis for phase fits.

    Produces plots and TSV files for phase fit intercepts, residuals, and RX lengths,
    and returns a pivoted dataframe with per-antenna fit information.

    Args:
        phase_fits: DataFrame with phase fit results per tile and polarization.
        tiles: DataFrame with tile metadata.
        freqs: Array of frequency values in Hz.
        soln_xx: XX polarization solutions.
        soln_yy: YY polarization solutions.
        weights: Weight values for each frequency channel.
        prefix: Output directory prefix for saving plots (default: './').
        show: Whether to display plots (default: False).
        title: Title for plots (default: '').
        plot_residual: Whether to plot residuals (default: False).
        residual_vmax: Maximum value for residual plot y-axis (default: None).

    Returns:
        Pivoted DataFrame with combined fit data, or None if no valid fits.
    """
    n_total = len(phase_fits)
    if n_total == 0:
        return

    phase_fits = reject_outliers(phase_fits, "chi2dof")
    phase_fits = reject_outliers(phase_fits, "sigma_resid")

    n_good = len(phase_fits[~phase_fits["outlier"]])
    if n_good == 0:
        return

    flavor_fits = pd.merge(phase_fits, tiles, left_on="tile_id", right_on="id")
    bad_fits = flavor_fits[flavor_fits["outlier"]]
    if len(bad_fits) > 0:
        logger.debug(f"flagged {len(bad_fits)} of {n_total} fits as outliers:")
        logger.debug(bad_fits[["name", "pol"]].to_string(index=False))

    # make a new colormap for weighted data
    half_blues = LinearSegmentedColormap.from_list(
        colors=mpl.colormaps["Blues"](np.linspace(0.5, 1, 256)),
        name="HalfBlues",
    )

    if len(flavor_fits):
        _rx_means = plot_rx_lengths(flavor_fits, prefix, show, title)
        # print(f"{rx_means=}")

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
        )
    if len(flavor_fits):
        plot_phase_intercepts(prefix, show, title, flavor_fits)

    phase_fits_pivot = pivot_phase_fits(phase_fits, tiles)
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


def reject_outliers(data, quality_key, nstd=3.0, max_iter=10):
    """Mark outliers in a DataFrame based on a quality metric.

    Uses a robust, iteratively-refined threshold per polarisation:
    threshold = median + nstd * 1.4826 * MAD, computed from that
    polarisation's not-yet-flagged rows, with newly-flagged rows removed
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
    iterative-clip approach already used by iterative_poly_clip for
    amplitude-outlier detection.

    Also fixes a pre-existing bug: the previous implementation computed
    quality_thresh from a `pol`-specific population but then applied it
    via a mask with no `pol` filter, so a threshold derived from one
    polarisation's population could incorrectly flag rows of the other.
    Flagging is now scoped to the current `pol` throughout.

    Args:
        data: Input DataFrame with a 'pol' column and quality column.
        quality_key: Name of the column to use for outlier detection.
        nstd: Number of (MAD-derived, approximately Gaussian-equivalent)
            standard deviations beyond the population median before a
            row is an outlier (default: 3.0). Negative flags low
            outliers instead of high ones.
        max_iter: Maximum number of threshold/clip iterations per
            polarisation.

    Returns:
        DataFrame with an 'outlier' column added/updated marking outliers.
    """
    if nstd == 0:
        return data
    if "outlier" not in data.columns:
        data["outlier"] = False

    # Scales a normal-distribution MAD to be comparable to a standard
    # deviation, so nstd keeps roughly the same meaning as the previous
    # mean+nstd*std threshold for a population with few/no outliers.
    mad_to_std = 1.4826

    quality_values = data[quality_key].to_numpy()
    outlier_values = data["outlier"].to_numpy().copy()

    for pol in data["pol"].unique():
        pol_mask = (data["pol"] == pol).to_numpy()

        for _ in range(max_iter):
            idx_pol_good = np.where(pol_mask & ~outlier_values)[0]
            if len(idx_pol_good) == 0:
                break

            pol_values = quality_values[idx_pol_good]
            pol_median = np.median(pol_values)
            pol_mad = np.median(np.abs(pol_values - pol_median))

            if pol_mad == 0 or np.isnan(pol_mad):
                if np.ptp(pol_values) == 0:
                    # Truly zero spread across this pol's currently-good
                    # population (or too few points to compute a
                    # meaningful spread, e.g. a single row) -- nothing
                    # stands out, so stop iterating for this pol. Without
                    # this guard, zero spread gives threshold == median,
                    # and ">=" trivially flags every remaining row via
                    # equality -- the opposite of correct behaviour.
                    break
                # MAD's ~50% breakdown point means a minority of extreme
                # values can collapse it to zero even though real spread
                # exists (e.g. 9 identical values + 1 extreme one: the
                # median residual is 0 for the majority, so is the MAD).
                # Fall back to a mean+std threshold for this round only,
                # as a safety net -- std isn't fooled by a minority
                # outlier the way MAD's breakdown point is here.
                pol_mean = np.mean(pol_values)
                pol_std = np.std(pol_values, ddof=1)
                if pol_std == 0:
                    break
                quality_thresh = pol_mean + nstd * pol_std
            else:
                quality_thresh = pol_median + nstd * mad_to_std * pol_mad

            if nstd >= 0:
                newly_bad = pol_mask & ~outlier_values & (quality_values >= quality_thresh)
            else:
                newly_bad = pol_mask & ~outlier_values & (quality_values <= quality_thresh)

            if not newly_bad.any():
                break
            outlier_values[newly_bad] = True

    data["outlier"] = outlier_values
    return data


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
    # offset = good_fits['length'].median() * 0.05 # offset from median for display
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
        # fig.subplots_adjust(top=0.88)
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
            mask_weights: ArrayLike = weights2[mask]
            ax.scatter(mask_freq, wrap_angle(angle[mask]), c=mask_weights, cmap=cmap, s=2)
            outlier = fit[f"outlier_{pol}"]
            color = "red" if outlier else "black"
            ax.set_title(
                f"{fit['name']}|{fit['soln_idx']}",
                color=color,
                weight="semibold",
                fontfamily="monospace",
            )  # |{fit['id']}
            x_text = np.mean(ax.get_xlim())
            y_text = np.mean(ax.get_ylim())
            text = "\n".join(
                [
                    f"L{fit[f'length_{pol}']:+6.2f}m",
                    f"X{fit[f'chi2dof_{pol}']:.4f}",
                    # f"S{fit[f'sigma_resid_{pol}']:.4f}",
                    # f"Q{fit[f'quality_{pol}']:.2f}",
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
        flavor_fits: DataFrame with phase fit results per receiver flavor.
    """
    plt.clf()
    g = sns.FacetGrid(flavor_fits, row="flavor", col="pol", hue="flavor", sharex=True, sharey=False)

    if len(freqs) != len(weights):
        raise RuntimeError(f"({len(freqs)=}) and ({len(weights)=}) must be the same length")

    df = pd.DataFrame(
        {
            "freq": freqs,
            "weights": weights,
        }
    )

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
        medians = np.nanmedian(resids, axis=0)
        min_mse = np.inf
        best_coeffs = None
        best_indep = None
        mask = np.where(np.logical_and(np.isfinite(medians), np.logical_not(np.isnan(medians)), weights > 0))[0]
        df[f"{flav}_{pol}"] = medians
        for indep_var in ["ν", "λ"]:
            if indep_var == "ν":
                xs = freqs[mask]
            elif indep_var == "λ":
                xs = 1.0 / freqs[mask]

            for order in range(1, 9):
                try:
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
        if residual_vmax is not None:
            ylim = float(residual_vmax)
            plt.ylim(-ylim, ylim)

        # logger.debug(f"{flav=} {pol=} {eqn=}")

    g.map(plot_residual, "soln_idx", "pol", "flavor", "length", "intercept")
    g.set_axis_labels("freq", "phase")

    fig = plt.gcf()
    if title:
        fig.suptitle(title)
        fig.subplots_adjust(top=0.95)
    fig.savefig(f"{prefix}residual.png", dpi=200, bbox_inches="tight")
    # save df to csv
    df.to_csv(f"{prefix}residual.tsv", sep="\t", index=False)


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
        phase_fits[phase_fits["pol"] == "XX"].drop(columns=["pol"]),
        phase_fits[phase_fits["pol"] == "YY"].drop(columns=["pol", "soln_idx"]),
        on=["tile_id"],
        suffixes=["_xx", "_yy"],
    )
    phase_fits = pd.merge(phase_fits, tiles, left_on="tile_id", right_on="id")
    phase_fits.drop("id", axis=1, inplace=True)
    tile_columns = ["soln_idx", "name", "tile_id", "rx", "slot", "flavor"]
    tile_columns += [*(set(tiles.columns) - set(tile_columns) - {"id"})]
    fit_columns = [column for column in phase_fits.columns if column not in tile_columns]
    fit_columns.sort()
    phase_fits = pd.concat([phase_fits[tile_columns], phase_fits[fit_columns]], axis=1)
    return phase_fits


def get_convergence_summary(solutions_fits_file: str):
    """Get a convergence summary from a solution file.

    Args:
        solutions_fits_file: Path to the solutions FITS file.

    Returns:
        List of tuples with convergence statistics.
    """
    # Local import to avoid a module-level circular dependency: HyperfitsSolution
    # now lives in mwax_hyperdrive_solutions.py, which itself imports several
    # symbols from this module (ChanInfo, Metafits, ensure_system_byte_order,
    # etc.). This is the only remaining usage of HyperfitsSolution here.
    from mwax_mover.mwax_hyperdrive_solutions import HyperfitsSolution

    soln = HyperfitsSolution(solutions_fits_file)
    results = soln.results
    converged_channel_indices = np.where(~np.isnan(results))
    summary = []
    summary.append(("Converged channel indices", converged_channel_indices))
    summary.append(("Total number of channels", len(results)))
    summary.append(
        (
            "Number of converged channels",
            f"{len(converged_channel_indices[0])}",
        )
    )
    summary.append(
        (
            "Fraction of converged channels",
            (f" {len(converged_channel_indices[0]) / len(results) * 100}%"),
        )
    )
    summary.append(
        (
            "Average channel convergence",
            f" {np.mean(results[converged_channel_indices])}",
        )
    )
    return summary


def generate_hyperdrive_plots(
    obs_id: int, hyperdrive_solution_filename: str, hyperdrive_binary_path: str, metafits_filename: str, output_dir: str
) -> tuple[bool, str]:
    """Generate solution plots.

    Args:
        obs_id: Observation ID.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        metafits_filename: Path to the metafits file.
        output_dir: path to where we write the plots

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(f"{obs_id} generating plots for {hyperdrive_solution_filename}...")

    try:
        # Now run hyperdrive again to do some plots
        hyp_soln_plot_args = f" --output-directory {output_dir}"
        cmd = (
            f"{hyperdrive_binary_path} solutions-plot {hyp_soln_plot_args} "
            f"-m"
            f" {metafits_filename} {hyperdrive_solution_filename}"
        )

        return_value, _ = run_command_ext(cmd, -1, timeout=60, use_shell=False)

        logger.info(
            f"{obs_id} Finished running hyperdrive plots on {hyperdrive_solution_filename}. Return={return_value}"
        )
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


def write_hyperdrive_stats(
    obs_id: int,
    stats_fd,
    hyperdrive_solution_filename: str,
) -> tuple[bool, str]:
    """Write convergence statistics. (Append to existiing stats file if it exists.)

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


def write_readme_file(filename, cmd, exit_code, output, error):
    """Write a readme file documenting the result of a command or operation.

    Used both for subprocess results (birli, hyperdrive) and for recording
    Python exception details on failure.

    Args:
        filename: Path to write the readme file to.
        cmd: The command or operation that was executed.
        exit_code: The exit code or error code (0 = success).
        output: Standard output from the command, or empty string.
        error: Standard error from the command, or exception traceback text.
    """
    try:
        with open(filename, "w", encoding="UTF-8") as readme:
            if exit_code == 0:
                readme.write(f"This run succeeded at: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
            else:
                readme.write(f"This run failed at: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
            readme.write(f"Command: {cmd}\n")
            readme.write(f"Exit code: {exit_code}\n")
            readme.write(f"output: {output}\n")
            readme.write(f"error: {error}\n")

    except Exception:
        logger.warning(
            (f"Could not write text file {filename} describing the problem observation."),
            exc_info=True,
        )


def run_birli(
    input_data_path: str,
    metafits_filename: str,
    uvfits_filename: str,
    job_output_path: str,
    obs_id: int,
    oversampled: bool,
    birli_binary_path: str,
    birli_max_mem_gib: int,
    birli_timeout: int,
    birli_freq_res_hz: int,
    birli_int_time_res_sec: float,
    birli_edge_width_hz: int,
) -> bool:
    """Execute Birli to process visibility data.

    Args:
        input_data_path: Path to input visibility FITS files.
        metafits_filename: Path to the metafits file.
        uvfits_filename: Output path for UV FITS file.
        job_output_path: Output directory for Birli.
        obs_id: Observation ID.
        oversampled: Whether the observation is oversampled.
        birli_binary_path: Path to the Birli executable.
        birli_max_mem_gib: Maximum memory in GiB for Birli.
        birli_timeout: Timeout in seconds for Birli execution.
        birli_freq_res_hz: Frequency resolution in Hz.
        birli_int_time_res_sec: Integration time resolution in seconds.
        birli_edge_width_hz: Edge width in Hz to flag.

    Returns:
        True if execution succeeded, False otherwise.
    """
    birli_success: bool = False
    start_time = time.time()
    stderr = ""

    cmdline = None
    exit_code = None
    stdout = None
    try:
        # Get only data files
        data_files = glob.glob(os.path.join(input_data_path, f"{obs_id}_*_*_*.fits"))

        data_file_arg = ""
        for data_file in data_files:
            if data_file.endswith("solutions.fits"):
                continue
            if data_file.endswith("metafits_ppds.fits"):
                continue
            data_file_arg += f"{data_file} "

        metafits = Metafits(metafits_filename)
        fine_chan_width_hz = metafits.chan_info.fine_chan_width_hz
        time_time_s = metafits.time_info.int_time_s

        # set default edge_width res from config
        if oversampled:
            # For oversampled obs we don't flag edges and we don't correct passband
            edge_width_hz = 0
        else:
            edge_width_hz = birli_edge_width_hz  # default
            edge_width_hz = np.max([fine_chan_width_hz, edge_width_hz])
            assert edge_width_hz >= fine_chan_width_hz, f"{edge_width_hz=} must be >= {fine_chan_width_hz=}"
            assert edge_width_hz % fine_chan_width_hz == 0, f"{edge_width_hz=} must multiple of {fine_chan_width_hz=}"

        # set minimum freq res from config
        min_freq_res = birli_freq_res_hz
        avg_arg = ""
        if fine_chan_width_hz < min_freq_res:
            avg_arg += f" --avg-freq-res={int(min_freq_res / 1e3)}"

        # set minimum time res from config
        min_time_res = birli_int_time_res_sec
        if time_time_s < min_time_res:
            avg_arg += f" --avg-time-res={min_time_res}"

        # Run birli
        cmdline = (
            f"{birli_binary_path}"
            f" --metafits {metafits_filename}"
            " --no-draw-progress"
            f" --uvfits-out={uvfits_filename}"
            f" --flag-edge-width={int(edge_width_hz / 1e3)}"
            f" --max-memory={birli_max_mem_gib}"
            f" {avg_arg} {data_file_arg}"
        )

        birli_popen_process = run_command_popen(cmdline, -1, False, False)

        exit_code, stdout, stderr = check_popen_finished(
            birli_popen_process,
            birli_timeout,
        )

        elapsed = time.time() - start_time

        if exit_code == 0:
            # Success!
            logger.info(f"{obs_id}: Birli run successful in {elapsed:.3f} seconds")
            birli_success = True

            # Success!
            # Write out a useful file of command line info
            readme_filename = os.path.join(job_output_path, f"{obs_id}_birli_readme.txt")
            write_readme_file(
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
        else:
            logger.error(f"{obs_id}: Birli run FAILED: Exit code of {exit_code} in {elapsed:.3f} seconds: {stderr}")
    except Exception as birli_run_exception:
        elapsed = time.time() - start_time
        logger.error(
            f"{obs_id}: birli run FAILED: Unhandled exception {birli_run_exception} in {elapsed:.3f} seconds: {stderr}"
        )

    if not birli_success:
        # If we are not shutting down,
        # Move the files to an error dir
        logger.info(
            f"{obs_id}: moving failed files to {job_output_path} for manual analysis and writing readme_error.txt"
        )

        # Move the processing dir
        shutil.move(input_data_path, job_output_path)

        # Write out a useful file of error and command line info
        readme_filename = os.path.join(job_output_path, "readme_error.txt")
        write_readme_file(
            readme_filename,
            cmdline,
            exit_code,
            stdout,
            stderr,
        )

    return birli_success


def run_hyperdrive(
    input_uvfits_files: list[str],
    metafits_filename: str,
    job_output_path: str,
    obs_id: int,
    hyperdrive_binary_path: str,
    source_list_filename: str,
    source_list_type: str,
    num_sources: int,
    hyperdrive_timeout: int,
    hyperdrive_extra_args: str,
) -> tuple[bool, str]:
    """Run hyperdrive calibration on UV FITS files.

    Args:
        input_uvfits_files: List of input UV FITS files (one per coarse channel).
        metafits_filename: Path to the metafits file.
        job_output_path: Output directory for hyperdrive.
        obs_id: Observation ID.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        source_list_filename: Path to the source list file.
        source_list_type: Type of source list (e.g., 'gleam').
        num_sources: Number of sources in the list.
        hyperdrive_timeout: Timeout in seconds for hyperdrive execution.
        hyperdrive_extra_args: Any additional command line args provided from the calvin_processor config file.

    Returns:
        tuple[True, calibration_command] if all runs succeeded, [False, calibration_command] if any failed.
    """
    logger.info(
        f"{obs_id}: {len(input_uvfits_files)} contiguous bands detected."
        f" Running hyperdrive {len(input_uvfits_files)} times...."
    )

    hyperdrive_runs_success: int = 0
    stdout = ""
    stderr = ""
    elapsed = -1
    cmdline = ""
    exit_code = 0
    # FIX 1: initialise calibration_command so it is always bound, even if
    # input_uvfits_files is empty, preventing UnboundLocalError at the return sites.
    calibration_command = ""

    for hyperdrive_run, uvfits_file in enumerate(input_uvfits_files):
        obsid_and_band = os.path.basename(uvfits_file.replace(".uvfits", ""))

        # FIX 2: move start_time above the try block so it is always bound
        # before the exception handler references elapsed.
        start_time = time.time()

        try:
            hyperdrive_solution_full_filename = os.path.join(job_output_path, f"{obsid_and_band}_solutions.fits")
            bin_solution_filename = f"{obsid_and_band}_solutions.bin"
            bin_solution_full_filename = os.path.join(job_output_path, bin_solution_filename)

            calibration_command = (
                f"--num-sources {num_sources}"
                f" --source-list {source_list_filename}"
                f" --source-list-type {source_list_type}"
                f" {hyperdrive_extra_args}"
            )
            cmdline = (
                f"{hyperdrive_binary_path} di-calibrate"
                f" --no-progress-bars {calibration_command}"
                f" --data {uvfits_file} {metafits_filename} "
                f" --outputs {hyperdrive_solution_full_filename} {bin_solution_full_filename}"
            )

            logger.info(f"{obs_id}: Running hyperdrive on {uvfits_file}...")
            hyperdrive_popen_process = run_command_popen(cmdline, -1, False, False)

            exit_code, stdout, stderr = check_popen_finished(
                hyperdrive_popen_process,
                hyperdrive_timeout,
            )

            elapsed = time.time() - start_time

            if exit_code == 0:
                logger.info(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(input_uvfits_files)} successful"
                    f" in {elapsed:.3f} seconds"
                )

                # FIX 3: include job_output_path so the readme is written to
                # the correct output directory, not the current working directory.
                readme_filename = os.path.join(job_output_path, f"{obsid_and_band}_hyperdrive_readme.txt")
                write_readme_file(
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )

                hyperdrive_runs_success += 1
            else:
                logger.error(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(input_uvfits_files)} FAILED:"
                    f" Exit code of {exit_code} in"
                    f" {elapsed:.3f} seconds. StdErr: {stderr}"
                )
                break

        except Exception as hyperdrive_run_exception:
            elapsed = time.time() - start_time
            logger.error(
                f"{obs_id}: hyperdrive run"
                f" {hyperdrive_run + 1}/{len(input_uvfits_files)} FAILED:"
                " Unhandled exception"
                f" {hyperdrive_run_exception} in"
                f" {elapsed:.3f} seconds. StdErr: {stderr}"
            )
            break

    if hyperdrive_runs_success != len(input_uvfits_files):
        logger.info(
            f"{obs_id}: moving failed files to {job_output_path} for manual analysis and writing readme_error.txt"
        )

        for uvfits_file in input_uvfits_files:
            shutil.move(uvfits_file, job_output_path)

        readme_filename = os.path.join(job_output_path, "readme_error.txt")
        write_readme_file(
            readme_filename,
            cmdline,
            exit_code,
            stdout,
            stderr,
        )
        return False, calibration_command

    return True, calibration_command


def create_sbatch_script(
    config_file_path: str,
    obs_id: int,
    jobtype: CalvinJobType,
    log_path: str,
    request_ids: list[str],
    bulk_request: bool,
    processor_args: str,
) -> str:
    """Create a Slurm batch script for Calvin processing.

    Args:
        config_file_path: Path to the Calvin configuration file.
        obs_id: Observation ID.
        jobtype: Type of Calvin job (realtime or mwa_asvo).
        log_path: Global log directory path.
        request_ids: List of request IDs.
        bulk_request: Is this a bulk request? If so lower priority.
        processor_args: Extra command-line arguments for the processor.

    Returns:
        The generated batch script as a string.
    """
    # log_path is the global log path e.g. /home/mwa/logs
    # processor_args is to allow the caller to add extra processor cmd line args.
    # E.g. MWA ASVO requires --mwa-asvo-download-url=URL
    #
    if jobtype == CalvinJobType.realtime:
        job_name = f"real{obs_id}"
        partition = "priority,gpu"
        nice = "0"  # highest priority
        wall_time = "04:00:00"
    else:
        job_name = f"asvo{obs_id}"
        partition = "gpu"
        if bulk_request:
            nice = "10000"  # lowest priority
        else:
            nice = "1000"  # lower priority than realtime jobs
        wall_time = "10:00:00"  # allow extra time for downloading from ASVO (8 hours + 2 for processing)

    job_script = f"""#!/bin/bash
#SBATCH --partition={partition}
#SBATCH --nodes=1
#SBATCH --cpus-per-task=90
#SBATCH --ntasks=1
#SBATCH --gpus-per-task=1
#SBATCH --exclusive # use all cpus
#SBATCH --mem=900G
#SBATCH --time={wall_time}
#SBATCH --account=mwa
#SBATCH --job-name={job_name}
#SBATCH --signal=USR1@360
#SBATCH --output={log_path}/%J.out
#SBATCH --error={log_path}/%J.out
#SBATCH --open-mode=append
#SBATCH --parsable
#SBATCH --nice={nice}

echo "Starting Calvin {jobtype.value} Job: $SLURM_JOBID";

# Source the python environment
cd /home/mwa/mwax_mover
source .venv/bin/activate

# Explicitly specifying these as they dont seem to be passed from the mwa env
export MWA_BEAM_FILE=/software/hyperdrive/mwa_full_embedded_element_pattern.h5
export HYPERDRIVE_CUDA_COMPUTE=86

# Process
srun --nodes=1 --ntasks=1 --cpus-per-task=90 \\
mwax_calvin_processor \\
--cfg={config_file_path} \\
--job-type={jobtype.value} \\
--obs-id={obs_id} \\
--request-ids={",".join(request_ids)} \\
--slurm-job-id=$SLURM_JOBID {processor_args}

exit $?
"""

    return job_script


def submit_sbatch(script_path: str, script: str, obs_id: int, request_ids: list[int]) -> tuple[bool, int | None]:
    """Submit an sbatch script to Slurm.

    Args:
        script_path: Directory to write the script to.
        script: The batch script content.
        obs_id: Observation ID (for naming).
        request_id: Request ID (for naming-prevents duplicates- yes it can happen. In fact it just did. Hence this change!)

    Returns:
        A tuple of (success: bool, slurm_job_id: int or None).
    """
    try:
        script_filename: str = os.path.join(
            script_path,
            datetime.datetime.now().strftime(f"%Y%m%d-%H%M%S-{obs_id}-{'-'.join(str(i) for i in request_ids)}.sh"),
        )
        cmdline = f"sbatch {script_filename}"

        # Create an sbatch file
        with open(script_filename, "w") as job_script:
            job_script.write(script)
    except Exception:
        logger.exception(f"{obs_id!s} failure creating temp sbatch script.")
        return (False, None)

    # Submit the job
    return_val: bool = False
    stdout = ""
    try:
        return_val, stdout = run_command_ext(cmdline, None, 60, True)

        # remove crlf from stdout
        stdout = stdout.replace("\n", " ")

        # Success- get the new job id
        # sbatch should send this to std out:
        # "Submitted batch job 34987"
        if return_val:
            logger.info(f"{script_filename} successfully submitted to Slurm. Stdout: {stdout}")
            slurm_job_id_string = stdout.replace("Submitted batch job ", "")
            if is_int(slurm_job_id_string):
                return (True, int(slurm_job_id_string))
            else:
                # This deserves to be a massive failure, as if SBATCH returned true it should always give
                # us the SLURM job id!
                logger.error(f"Slurm job submitted OK, but could not get slurm_job_id from: {stdout}. Aborting")
                sys.exit(-10)
        else:
            logger.error(f"{script_filename} failed to be submitted to SLURM. Error {stdout}")

    except Exception:
        logger.exception(f"{script_filename} failure running sbatch.")
        return_val = False

    if not return_val:
        return (False, None)


def count_slurm_asvo_jobs() -> int:
    """
    Count all SLURM jobs in the queue with names starting with 'asvo'.

    Returns:
        The number of matching jobs, or -1 if the command failed.
    """
    try:
        success, output = run_command_ext(
            command="squeue --format=%j --noheader",
            numa_node=None,
        )
    except Exception:
        logger.exception("count_slurm_asvo_jobs() failed")
        return -1

    if not success:
        logger.error("count_slurm_asvo_jobs() retured -1")
        return -1

    return sum(1 for line in output.splitlines() if line.startswith("asvo"))


def estimate_birli_output_bytes(
    metafits_context: MetafitsContext,
    birli_freq_res_khz: int,
    birli_int_time_res_sec: float,
    bytes_per_r_and_i: int = 13,
) -> int:
    """Estimate the output file size from Birli processing.

    Args:
        metafits_context: Metafits context with observation parameters.
        birli_freq_res_khz: Frequency resolution in kHz.
        birli_int_time_res_sec: Integration time resolution in seconds.
        bytes_per_r_and_i: Bytes per visibility (default: 13).

    Returns:
        Estimated output size in bytes.
    """
    #
    # bytes_per_visibility comes from Birli
    #
    # baselines = (tiles * tiles + 1)
    # timesteps = duration / birli_int_time_res_sec
    # coarse_chans = 24
    # fine_channels = 30.72 MHz / birli_freq_res_khz
    # pols = 4 (XX,XY,YX,YY)
    # bytes_per_visibility = 8+4+1
    # Total bytes = (timesteps * coarse_chans * fine_channels * baselines * pols * bytes_per_visibility )
    #
    # (Normally you would use values * bytes_per_value but Birli has more outputs than this)
    #
    # Total GB = bytes / 1000.^3
    baselines: int = metafits_context.num_baselines  # 144T (10440)
    timesteps: int = int(metafits_context.sched_duration_ms / (birli_int_time_res_sec * 1000.0))  # 60
    coarse_channels: int = metafits_context.num_metafits_coarse_chans
    fine_channels: int = int(
        metafits_context.coarse_chan_width_hz / (birli_freq_res_khz * 1000.0)
    )  # 1280000 / 80000 == 16
    pols: int = metafits_context.num_visibility_pols  # (XX,XY,YX,YY) # 4

    # Uncomment for debug
    # print(f"{timesteps}ts * {coarse_channels * fine_channels}ch * {baselines}bl * {pols}pol * {bytes_per_visibility} bytes")

    return timesteps * coarse_channels * fine_channels * baselines * pols * bytes_per_r_and_i


def get_solution_fits_filename(solutions_dir: str, obs_id: int, rec_chan: int) -> str | None:
    """Find a hyperdrive solution FITS file for a specific channel.

    Searches for solution files in multiple formats:
    1. obsid_solutions.fits (all 24 channels)
    2. obsid_chNNN_solutions.fits (single channel)
    3. obsid_chNNN-MMM_solutions.fits (channel range)

    Args:
        solutions_dir: Directory containing solution files.
        obs_id: Observation ID.
        rec_chan: Receiver channel number to find.

    Returns:
        Full path to matching solution file, or None if not found.
    """
    candidates = get_sorted_solution_files(solutions_dir, obs_id, "fits")

    for filepath in candidates:
        channels = parse_solution_channels(filepath)

        if channels is None:
            return filepath

        chan_start, chan_end = channels
        if chan_start <= rec_chan <= chan_end:
            return filepath

    return None


def parse_solution_channels(filename: str) -> tuple[int, int] | None:
    """Parse channel range from a hyperdrive solution filename.

    Recognises these filename flavours (with .fits or .bin extension):
    1. obsid_solutions.{ext}           -> None (all 24 channels)
    2. obsid_chNNN_solutions.{ext}     -> (NNN, NNN)
    3. obsid_chNNN-MMM_solutions.{ext} -> (NNN, MMM)

    Channel numbers are not zero-padded.

    Args:
        filename: Filename or full path to a solution file.

    Returns:
        (start_channel, end_channel) tuple, or None if the file
        covers all channels (flavour 1).

    Raises:
        ValueError: If the filename does not match any known flavour.
    """
    basename = os.path.basename(filename)

    # Flavour 1: obsid_solutions.{fits,bin} — all 24 channels
    if re.match(r"^\d+_solutions\.(?:fits|bin)$", basename):
        return None

    # Flavour 2: obsid_chNNN_solutions.{fits,bin} — single channel
    match = re.match(r"^\d+_ch(\d+)_solutions\.(?:fits|bin)$", basename)
    if match:
        chan = int(match.group(1))
        return (chan, chan)

    # Flavour 3: obsid_chNNN-MMM_solutions.{fits,bin} — channel range
    match = re.match(r"^\d+_ch(\d+)-(\d+)_solutions\.(?:fits|bin)$", basename)
    if match:
        return (int(match.group(1)), int(match.group(2)))

    raise ValueError(f"The channels for {basename} could not be determined")


def get_sorted_solution_files(directory: str, obs_id: int, extension: str = "fits") -> list[str]:
    """Return solution files sorted numerically by channel number.

    Sorting order:
      obsid_solutions.{ext}             -> channel 0 (sorts first)
      obsid_ch95_solutions.{ext}        -> channel 95
      obsid_ch100-112_solutions.{ext}   -> channel 100 (uses range start)

    Unrecognised filenames are sorted with channel 0.

    Args:
        directory: Directory to search for solution files.
        obs_id: Observation ID to filter by.
        extension: File extension to match ("fits" or "bin").

    Returns:
        List of full paths, sorted by channel number then path.
        Or raises ValueError exception if extension doesn't match ("fits" or "bin")
    """
    # Check that the extension doesn't include a "."
    if extension != "fits" and extension != "bin":
        raise ValueError("get_sorted_solution_files() extension should be 'fits' or 'bin'")

    def _sort_key(path: str) -> tuple[int, str]:
        try:
            channels = parse_solution_channels(path)
        except ValueError:
            return (0, path)

        if channels is None:
            return (0, path)

        return (channels[0], path)

    return sorted(
        glob.glob(os.path.join(directory, f"{obs_id}_*solutions.{extension}")),
        key=_sort_key,
    )


def get_file_description(filename: str) -> str:
    """Given a filename, attempt to generate a description of it
    Args:
        filename: The filename to be described
    """

    # If it has a "chNNN" then this is a single coarse channel output
    chans = extract_channels_from_filename(filename)
    if chans is not None:
        chan_no = chans["start"]

        if "end" not in chans:
            channel_suffix = f" for receiver channel {chan_no} ({chan_no * 1.28:.3f} MHz)"
        else:
            chan_no_end = chans["end"]
            channel_suffix = (
                f" for receiver channels {chan_no}-{chan_no_end} ({chan_no * 1.28:.3f} - {chan_no_end * 1.28:.3f} MHz)"
            )
    else:
        channel_suffix = " for all coarse channels"

    desc = ""
    if "birli_readme.txt" in filename:
        desc = "Full log output of the Birli run"
    elif "hyperdrive_readme.txt" in filename:
        desc = "Full log output of the Hyperdrive run"
    elif "intercepts.png" in filename:
        desc = "Plots showing, for each reciever type and polarisation, a plot of the phase intercepts in polar coordinates vs cable length"
    elif "phase_fits_xx.png" in filename:
        desc = "Plot of the phase fit for each tile (phase vs frequency) for XX"
    elif "phase_fits_yy.png" in filename:
        desc = "Plot of the phase fit for each tile (phase vs frequency) for YY"
    elif "phase_fits.tsv" in filename:
        desc = "Tab separated values (TSV) file containing all of the phase fit statistics per tile"
    elif "rx_lengths.png" in filename:
        desc = "Cable length offsets in metres per receiver"
    elif "solutions_amps.png" in filename:
        desc = "Calibration solution amplitudes vs fine channel per tile"
    elif "solutions_phases.png" in filename:
        desc = "Calibration solution phase vs fine channel per tile"
    elif "stats.txt" in filename:
        desc = "Hyperdrive fine channel convergence statistics"
    elif "residual.tsv" in filename:
        desc = "Tab separated value (TSV) file of phase residuals vs frequency by receiver type and polarisation"
    elif "residual.png" in filename:
        desc = "Plot of phase residuals vs frequency by receiver type and polarisation"
    elif "gain_outliers_tiles" in filename:
        desc = "Plot of outlier gains that were removed from the calibration solutions"
    elif "_solutions.fits" in filename:
        desc = "Hyperdrive calibration solutions in FITS format. If a '*_solutions.original.fits' also exists, then this file has had outlier gains flagged (entire Jones matrix is NaNed)"
    elif "_solutions.original.fits" in filename:
        desc = "Original unmodified Hyperdrive calibration solutions out of Hyperdrive in FITS format (gain outliers not flagged)"

    if desc == "":
        return "Miscellaneous file"
    else:
        return f"{desc}{channel_suffix}"


def generate_plot_index_file(
    fit_id: int, plot_front_end_url: str, fit_dir: str, output_filename: str
) -> tuple[bool, dict]:
    """Scans the specified directory (non-recursively) and produces a JSON manifest
    describing each file, suitable for upload to S3 alongside the files themselves.
    The manifest includes a CloudFront URL and MIME type for each file.

    Args:
        fit_id: the fit_id of this calibration. We use this to create a dir in the plot_upload_path
        plot_front_end_url: URL base to retrieve the file. E.g. https://s3blah
        fit_dir: Directory containing the fit to index
        output_filename: full path and name of the JSON file to write

    Returns:
        bool: Success / failure
        dict: The JSON generated (if successful)
    """
    try:
        if not os.path.isdir(fit_dir):
            raise NotADirectoryError(f"Not a valid directory: {fit_dir}")

        files = []
        for filename in sorted(os.scandir(fit_dir), key=lambda e: e.name):
            if not filename.is_file():
                continue
            if filename.name == "index.json":
                continue

            new_entry = populate_index_json_entry(Path(filename), fit_id, plot_front_end_url)

            # None means it found a file we don't want to upload so skip it
            if new_entry is not None:
                files.append(new_entry)

        index = {
            "version": 2,
            "generated_at": datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "base_url": plot_front_end_url,
            "path": str(fit_id),
            "files": files,
        }

        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2)

        return True, index
    except Exception:
        # log it and return
        logger.exception(f"Problem generating the {output_filename} file for fit {fit_id}")
        return False, {}


def populate_index_json_entry(filename: str | Path, fit_id: int, plot_front_end_url: str) -> dict | None:
    """Builds an index.json file entry dict for a given directory entry.

    Inspects the file at ``filename``, extracts metadata (size, modification
    time, MIME type, and PNG dimensions where applicable), and returns a dict
    suitable for inclusion in the ``files`` list of an index.json file.

    Only ``.png``, ``.tsv``, and ``.txt`` files are supported; all other
    extensions return ``None``.

    Args:
        filename: A str or Path representing the file to describe.
            Must refer to an existing, stat-able file.
        fit_id: The integer fit ID, used to construct the S3 path component
            of the entry's ``url``.
        plot_front_end_url: Base URL of the calibration plot front end
            (e.g. ``"https://cal.mwatelescope.org"``). Combined with
            ``fit_id`` and the filename to form the full entry URL.

    Returns:
        A dict containing the index.json entry fields (``filename``, ``url``,
        ``size_bytes``, ``last_modified``, ``content_type``, ``description``,
        and, for PNG files, ``image_width`` and ``image_height``), or ``None``
        if the file extension is not one of ``.png``, ``.tsv``, or ``.txt``.

    Raises:
        OSError: If the file cannot be stat'd.
        Exception: Any exception raised by :func:`mwax_mover.utils.get_png_dimensions`
            for PNG files is propagated to the caller.
    """
    path = Path(filename)
    _, ext = os.path.splitext(path.name)

    if ext not in (".png", ".tsv", ".txt", ".fits"):
        return None

    stat = path.stat()
    last_modified = datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.UTC)
    mime_type, _ = mimetypes.guess_type(path.name)

    is_png = ext == ".png"
    width, height = None, None

    if is_png:
        width, height = get_png_dimensions(str(path))

    return {
        "filename": path.name,
        "url": f"{plot_front_end_url}/{fit_id}/{path.name}",
        "size_bytes": stat.st_size,
        "last_modified": last_modified.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "content_type": mime_type or "application/octet-stream",
        "description": get_file_description(str(path)),
        **({"image_width": width, "image_height": height} if is_png else {}),
    }


def export_calibration_solutions(solution_files: list[str], cal_export_path: str, cal_export_max_age_hours: int):
    """Export calibration solution FITS files to the configured export directory and delete stale files.

    Args:
        solution_files: List of hyperdrive solution filenames
        cal_export_path: Path to copy solution files to
        cal_export_max_age_hours: Files older than this many hours will be deleted from the cal_export_path

    Returns:
        Nothing
    """
    # if cal_export_path is set then:
    # 1. copy the solution FITS files to the export dir
    # 2. try to clean up old files

    #
    # copy the solution.fits file(s) to the export directory
    logger.info(f"Found {len(solution_files)} solution FITS files to upload.")

    for f in solution_files:
        # Copy solution fits files to the cal_export directory
        cal_dest = os.path.join(cal_export_path, os.path.basename(f))
        logger.info(f"Copying solution FITS file {f} to {cal_dest}")
        shutil.copy(f, cal_dest)

    # Clean up old files
    ext_list = ["fits", "bin"]
    files_removed = delete_files_older_than(cal_export_path, cal_export_max_age_hours * 3600, ext_list)
    if len(files_removed) > 0:
        logger.debug(
            f"Removed the following files from {cal_export_path} as they were older than {cal_export_max_age_hours} hours: {files_removed}"
        )
    else:
        logger.debug(f"No files older than {cal_export_max_age_hours} hours found in {cal_export_path} to remove.")


def upload_plot_files(job_output_path: str, upload_path: str):
    """Move all the plots and stats to the plots dir and include the fit_id as a folder.
    It's a nice to have, so if we fail, log it and move on

    Args:
        job_output_path: The location of all the plots,txt,tsv files for this fit
        upload_path: Intended destination directory for the plots and stats files to go

    Returns:
        Nothing
    """
    try:
        # Create the dest dir
        os.mkdir(upload_path)

        exts = [
            "*.png",
            "*.txt",
            "*.tsv",
            "*.json",
            "*_solutions.fits",
            "*_solutions.original.fits",
        ]
        for ext in exts:
            plot_files = glob.glob(os.path.join(job_output_path, ext))
            for file_no, pfile in enumerate(plot_files, start=1):
                try:
                    dest_filename = os.path.join(upload_path, os.path.basename(pfile))

                    # We want to keep the solutions on calvin servers so copy them, don't move them!
                    if ext in ["*_solutions.fits", "*_solutions.original.fits"]:
                        logger.debug(f"Copying {pfile} to {dest_filename} [{file_no}/{len(plot_files)}]")
                        shutil.copy(pfile, dest_filename)
                    else:
                        logger.debug(f"Moving {pfile} to {dest_filename} [{file_no}/{len(plot_files)}]")
                        shutil.move(pfile, dest_filename)

                except Exception as e:
                    logger.warning(f"Failed to move {pfile} to the {upload_path}. Error: {e!s}. Ignoring")
                    # keep going and try the next file

    except Exception as ee:
        # Something went wrong- log it and keep going
        logger.warning(f"Failed to move files to the {upload_path}. Error: {ee!s}. Ignoring")

