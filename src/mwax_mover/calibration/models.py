"""Calibration domain data structures: tiles, inputs, channel/time info,
metafits, and per-tile/per-channel fit results.

Metafits wraps mwalib.MetafitsContext and is the source of Tile/Input/
ChanInfo/TimeInfo instances. PhaseFitInfo and GainFitInfo are the results
of calibration.fitting's fit_phase_line()/fit_gain() respectively.
"""

from typing import NamedTuple

import numpy as np
import pandas as pd
from mwalib import MetafitsContext
from numpy.typing import NDArray


# Standard number of MWA coarse channels.
MWA_NUM_COARSE_CHANS = 24


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
    """Info about a single MWA rf_input (one polarisation's signal chain for a tile)."""

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

    coarse_chan_ranges: list[
        NDArray[np.int_]
    ]  # each element is a contiguous run of coarse channel numbers (from np.split)
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
    """Result of fitting a linear phase ramp to one tile/polarisation's calibration solution.

    See fit_phase_line for how these are computed.

    Fields:
        length: Fitted equivalent cable length, in metres (derived from the
            fitted phase slope via delay = length / c).
        intercept: Fitted phase intercept, in radians, wrapped to [-pi, pi].
        sigma_resid: Standard deviation of phase residuals (radians) after
            subtracting the best-fit model. Lower is better.
        chi2dof: Chi-squared per degree of freedom = sum(residuals**2) /
            (N - 2). Values near 1.0 indicate a good fit; much larger
            suggests a poor fit or RFI; much smaller suggests over-fitting
            or too few points.
        quality: Fraction of original frequency channels surviving the
            sigma-clip, in [0, 1]. 1.0 means every channel was used.
        stderr: Standard error of the fitted slope (rad/Hz), from the exact
            analytic Hessian (see _phase_fit_hess_inv) scaled by residual
            variance. Not used elsewhere in the pipeline -- purely
            informational.
    """

    length: float
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
    """Result of fitting gain amplitude vs. frequency for one tile/polarisation.

    One instance covers all coarse channels: every field except `quality`
    is a per-coarse-channel list. See fit_gain for how these are computed.

    Note on naming: despite the names, `pol0`/`pol1` are NOT related to
    polarisation (XX/YY) -- a separate GainFitInfo is already computed per
    polarisation (see e.g. x_gains/y_gains in calvin.pipeline.process_solutions).
    Within a single GainFitInfo, `pol0`/`pol1` are the order-0 (intercept)
    and order-1 (slope) coefficients of a small linear polynomial fit to
    gain amplitude vs. chanblock index, done *within* each coarse channel
    solely to compute `sigma_resid`.

    Fields:
        quality: Fraction of all chanblocks (including flagged ones)
            within 2*sigma_resid of their coarse channel's linear fit,
            across all coarse channels. Range [0, 1]; higher is better.
        gains: Per-coarse-channel weighted-mean inverse amplitude
            (1/amp), i.e. the actual gain value used downstream.
        pol0: Per-coarse-channel intercept of the within-coarse-channel
            linear amplitude fit (see note above). Diagnostic only.
        pol1: Per-coarse-channel slope of the within-coarse-channel
            linear amplitude fit (see note above). Diagnostic only.
        sigma_resid: Per-coarse-channel residual standard deviation of
            that linear fit.
    """

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
