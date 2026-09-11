"""A group of hyperdrive FITS calibration solutions for one observation.

Provides HyperfitsSolutionGroup (a set of HyperfitsSolution files -- one per
contiguous coarse-channel band, covering anywhere from 1 to 24 bands for MWA
"picket fence" calibrators -- combined with the observation's metafits): the
single source of truth for flagging and (eventually) writing hyperdrive
solutions across a whole observation. TileFlagReason/ChannelFlagReason and
the four private helpers below (_ref_normalise_xx_yy, _phase_fit_one,
_gain_fit_one, add_digital_gains_column) are used only by
HyperfitsSolutionGroup's methods. See calvin.hyperfits_solution for
HyperfitsSolution itself, and calvin.hyperdrive for running hyperdrive. See
calibration/ for the shared data structures and pure numeric fitting/outlier
functions these use, and calvin.plots for plotting. Split out of
calvin.hyperdrive -- see docs/HYPERDRIVE_PARALLELISM.md Phase 1.
"""

import itertools
import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import IntFlag, auto

import mwalib
import numpy as np
from astropy.io import fits
from numpy.typing import NDArray
from pandas import DataFrame

from mwax_mover.calibration.df_columns import COL_GX, COL_GY, COL_POL, COL_SOLN_IDX, COL_TILE_ID, COL_XX, COL_YY
from mwax_mover.calibration.fitting import fit_gain, fit_phase_line
from mwax_mover.calibration.models import ChanInfo, GainFitInfo, Metafits, PhaseFitInfo
from mwax_mover.calibration.outliers import annotate_phase_outliers, iterative_poly_clip_batch
from mwax_mover.calvin.hyperfits_solution import HyperfitsSolution

logger = logging.getLogger(__name__)


class TileFlagReason(IntFlag):
    """Reasons a whole tile (all chanblocks, all files) is flagged bad.

    Multiple bits may be set for the same tile (e.g. flagged in both the
    metafits and the TILES HDU).

    PHASE_OUTLIER is defined but never set by the automatic pipeline:
    HyperfitsSolutionGroup.detect_phase_outliers reports population-outlier
    phase fits (stats.txt's Flavor/PhOutlier columns, the phase-fit debug
    plots) without flagging or modifying the tile -- a deliberate,
    permanent policy decision, not a config toggle. The bit is kept
    defined (rather than removed) since removing an IntFlag member would
    silently renumber every later member's value.
    """

    NONE = 0
    METAFITS = auto()
    HYPERDRIVE_TILE = auto()
    HYPERDRIVE_BASELINE = auto()
    PHASE_OUTLIER = auto()
    MOSTLY_BAD_CHANNELS = auto()


class ChannelFlagReason(IntFlag):
    """Reasons a single (tile, chanblock) entry is flagged bad.

    Each stage only evaluates entries not already bad from an earlier
    stage, so in practice exactly one bit ends up set per entry -- whoever
    caught it first "owns" the reason.
    """

    NONE = 0
    PRE_EXISTING_NAN = auto()
    NON_CONVERGED = auto()
    PARTIAL_JONES = auto()
    GAIN_MAX_CUTOFF = auto()
    AMPLITUDE_OUTLIER = auto()


def _ref_normalise_xx_yy(
    jones: NDArray[np.complex128], ref_tile_idx: int
) -> tuple[NDArray[np.complex128], NDArray[np.complex128]]:
    """Reference-normalise a file's XX/YY (gx/gy) terms against one tile.

    Replicates the same standard 2x2 Jones-matrix determinant-inverse
    division that HyperfitsSolution.get_ref_solutions() performs on raw
    FITS data, but operating on an already-in-memory jones array instead.

    Args:
        jones: Complex array, shape (n_tiles, n_chanblocks, 2, 2).
        ref_tile_idx: Index of the reference tile (row in the tile axis).

    Returns:
        (ref_xx, ref_yy), each shape (n_tiles, n_chanblocks).
    """
    ref = jones[ref_tile_idx]  # shape (n_chanblocks, 2, 2)
    ref_gx, ref_dx, ref_dy, ref_gy = ref[..., 0, 0], ref[..., 0, 1], ref[..., 1, 0], ref[..., 1, 1]
    # The reference tile can legitimately be NaN (or have a zero
    # determinant) at some chanblocks -- e.g. a non-converged or
    # pre-existing-NaN entry -- in which case this division correctly
    # propagates NaN for that chanblock. The resulting "invalid value
    # encountered in divide" RuntimeWarning is expected noise, not a
    # sign of a problem.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="invalid value encountered in divide", category=RuntimeWarning)
        warnings.filterwarnings("ignore", message="divide by zero encountered in divide", category=RuntimeWarning)
        ref_inv_det = np.divide(1 + 0j, ref_gx * ref_gy - ref_dx * ref_dy)  # shape (n_chanblocks,)

    gx, dx, dy, gy = jones[..., 0, 0], jones[..., 0, 1], jones[..., 1, 0], jones[..., 1, 1]
    ref_xx = (gx * ref_gy - dx * ref_dy) * ref_inv_det
    ref_yy = (gy * ref_gx - dy * ref_dx) * ref_inv_det
    return ref_xx, ref_yy


def _phase_fit_one(
    soln_idx: int,
    tile_id: int,
    pol: str,
    solns: NDArray[np.complex128],
    chanblocks_hz: NDArray[np.float64],
    weights: NDArray[np.float64],
    phase_fit_niter: int,
    tiles: DataFrame,
) -> list | None:
    """Fit a phase ramp for a single tile and polarization.

    Looks up the tile in the tiles DataFrame, skips flagged or missing tiles,
    and calls fit_phase_line to perform the fit. Intended to be called
    concurrently via ThreadPoolExecutor.

    Args:
        soln_idx: Index of this tile in the solutions array.
        tile_id: The tile ID to look up in the tiles DataFrame.
        pol: Polarization label, either "XX" or "YY".
        solns: Complex calibration solutions for this tile and polarization.
        chanblocks_hz: Array of channel block frequencies in Hz.
        weights: Weight values for each solution.
        phase_fit_niter: Number of iterations for phase fitting.
        tiles: DataFrame containing tile metadata including flags and names.

    Returns:
        A list of [tile_id, soln_idx, pol, *PhaseFitInfo fields] if the fit
        succeeded, or None if the tile was skipped or the fit failed.
    """
    id_matches = tiles[tiles.id == tile_id]
    if len(id_matches) != 1:
        return None
    tile = id_matches.iloc[0]
    if tile.flag:
        return None
    name = tile["name"]
    try:
        fit = fit_phase_line(chanblocks_hz, solns, weights, niter=phase_fit_niter)
    except Exception as exc:
        logger.warning(f"Skipping phase fit for {tile_id=:4} {pol} ({name}): {exc}")
        return None
    return [tile_id, soln_idx, pol, *fit]


def _gain_fit_one(
    soln_idx: int,
    tile_id: int,
    pol: str,
    solns: NDArray[np.complex128],
    chanblocks_hz: NDArray[np.float64],
    weights: NDArray[np.float64],
    chanblocks_per_coarse: int,
    tiles: DataFrame,
) -> list | None:
    """Fit gain solutions for a single tile and polarization.

    Looks up the tile in the tiles DataFrame, skips flagged or missing tiles,
    and calls fit_gain to perform the fit. Intended to be called
    concurrently via ThreadPoolExecutor.

    Args:
        soln_idx: Index of this tile in the solutions array.
        tile_id: The tile ID to look up in the tiles DataFrame.
        pol: Polarization label, either "XX" or "YY".
        solns: Complex calibration solutions for this tile and polarization.
        chanblocks_hz: Array of channel block frequencies in Hz.
        weights: Weight values for each solution.
        chanblocks_per_coarse: Number of channel blocks per coarse channel.
        tiles: DataFrame containing tile metadata including flags and names.

    Returns:
        A list of [tile_id, soln_idx, pol, *GainFitInfo fields] if the fit
        succeeded, or None if the tile was skipped or the fit failed.
    """
    id_matches = tiles[tiles.id == tile_id]
    if len(id_matches) != 1:
        return None
    tile = id_matches.iloc[0]
    if tile.flag:
        return None
    name = tile["name"]
    try:
        fit = fit_gain(chanblocks_hz, solns, weights, chanblocks_per_coarse)
    except Exception as exc:
        logger.warning(f"Skipping gain fit for {tile_id=:4} {pol} ({name}): {exc}")
        return None
    return [tile_id, soln_idx, pol, *fit]


def add_digital_gains_column(
    hyperdrive_solution_filename: str,
    metafits_context: mwalib.MetafitsContext,
) -> bool:
    """Add a float32[24] digital-gains column to a FITS binary table HDU.

    Builds the 24-element (one per coarse channel) digital gain array for
    each tile/row in the given HDU, sourced from a populated mwalib
    MetafitsContext, and appends it as a new column. Rows are matched to
    metafits rf_inputs by tile ID so gains line up correctly even if HDU
    row order differs from metafits order. Only the X polarisation's
    digital gains are used, since X and Y always carry the same values.

    Moved here from mwax_calvin_quality.add_digital_gains_column as part
    of consolidating solution-file writes into HyperfitsSolutionGroup.commit().

    Args:
        hyperdrive_solution_filename: Path to the FITS file to modify.
        metafits_context: A populated mwalib.MetafitsContext instance;
            the caller is responsible for constructing/populating it.

    Returns:
        True if the column was added, False if it was already present and
        nothing was changed.

        NOTE: this previously returned the fits.BinTableHDU itself, which
        escaped the `with fits.open(...)` block. astropy loads HDU data
        lazily, so touching .data on the returned object after the file had
        been closed was unsafe. No caller used the return value, so it now
        reports only whether a change was made.

    Raises:
        KeyError: If a tile ID present in the HDU cannot be found among
            metafits_context.rf_inputs for the X polarisation.
    """
    hdu_name = "TILES"
    col_name = "DigitalGains"
    id_col = "Antenna"

    # Lookup: tile_id -> digital_gains, X pol only (X and Y are identical).
    gains_by_tile = {rf.ant: rf.digital_gains for rf in metafits_context.rf_inputs if rf.pol == mwalib.Pol.X}

    with fits.open(hyperdrive_solution_filename, mode="update") as hdul:
        tile_hdu = hdul[hdu_name]
        tile_ids = tile_hdu.data[id_col]

        # Does this column already exist? If so don't do anything but log a warning.
        if col_name in hdul[hdu_name].columns.names:
            logger.warning(f"Column '{col_name}' already exists in HDU '{hdu_name}'; skipping addition.")
            return False

        try:
            gains_array = np.array([gains_by_tile[int(tid)] for tid in tile_ids], dtype=np.float32)
        except KeyError as e:
            raise KeyError(f"Tile ID {e} in HDU '{hdu_name}' not found in metafits rf_inputs for pol='X'") from e

        n_chans = gains_array.shape[1]
        new_col = fits.Column(
            name=col_name,
            format=f"{n_chans}E",
            array=gains_array,
        )

        new_hdu = fits.BinTableHDU.from_columns(tile_hdu.columns + new_col, header=tile_hdu.header)
        new_hdu.name = hdu_name

        hdul[hdul.index_of(hdu_name)] = new_hdu
        hdul.flush()

    return True


class HyperfitsSolutionGroup:
    """A group of Hyperdrive FITS calibration solutions and corresponding metafits files."""

    def __init__(self, metafits: Metafits, solns: list[HyperfitsSolution]):
        """Initialize a solution group with metafits and solution files.

        Args:
            metafits: The observation's Metafits reader (a single instance, not
                a list).
            solns: List of HyperfitsSolution file readers, one per contiguous
                coarse-channel band.

        Raises:
            RuntimeError: If no solution files are provided.
        """
        self.metafits = metafits

        if not len(solns):
            raise RuntimeError("no solutions files provided")
        self.solns = solns

        self.metafits_tiles_df = self.metafits.tiles_df
        self.metafits_chan_info = HyperfitsSolutionGroup.get_metafits_chan_info(self.metafits)
        (
            self.chanblocks_per_coarse,
            self.all_chanblocks_hz,
            self.all_solution_coarse_chan_indices,
        ) = HyperfitsSolutionGroup.get_soln_chan_info(self.metafits_chan_info, self.solns)

        # Populated by load(). None until then -- every method that needs
        # this data calls _ensure_loaded() first, so a caller that forgets
        # to load() gets a clear error rather than a confusing crash deep
        # inside some later computation.
        self.jones: list[NDArray[np.complex128]] | None = None
        self.tile_flag_reasons: NDArray[np.object_] | None = None
        self.channel_flag_reasons: list[NDArray[np.object_]] | None = None

        # Populated by flag_amplitude_outliers() (a flagging decision) /
        # detect_phase_outliers() (report-only, no flagging decision
        # attached), for a later plotting step to show exactly what was
        # used, rather than recomputing against already-cleaned data.
        self.amplitude_fit: list[dict[str, NDArray[np.float64]]] | None = None
        self.amplitude_band: list[dict[str, tuple[NDArray[np.float64], NDArray[np.float64]]]] | None = None
        self.mad_residual_threshold: float | None = None
        self.phase_fits: DataFrame | None = None

        # Populated by run_flagging_pipeline(): a snapshot of jones/tile
        # flag reasons/channel flag reasons/phase fits taken right after
        # apply_tile_flags() (structural flags only), before the rest of
        # the pipeline runs. Used by
        # calvin.plots.stats_table.write_before_after_stats() to report a
        # "before" state alongside the group's final "after" state, and
        # by callers needing a not-yet-fully-flagged snapshot for
        # amplitude-outlier plots (see plot_outlier_gains's pristine_jones
        # argument).
        self.before_jones: list[NDArray[np.complex128]] | None = None
        self.before_tile_flag_reasons: NDArray[np.object_] | None = None
        self.before_channel_flag_reasons: list[NDArray[np.object_]] | None = None
        self.before_phase_fits: DataFrame | None = None

    def _ensure_loaded(self) -> None:
        """Raise a clear error if load() hasn't been called yet.

        Every method that reads or writes self.jones (or the flag-reason
        arrays) calls this first.
        """
        if self.jones is None:
            raise RuntimeError(
                "HyperfitsSolutionGroup.load() must be called before this method (self.jones is not yet populated)."
            )

    def load(self) -> None:
        """Read every solution file's Jones matrices into memory.

        Populates self.jones (one complex array per file, shape (n_tiles,
        n_chanblocks, 2, 2) -- see HyperfitsSolution.get_jones()) and
        self.tile_flag_reasons / self.channel_flag_reasons (initialised to
        NONE, then immediately updated with two channel-level reasons that
        describe the raw file's state before any flagging stage runs:
        PRE_EXISTING_NAN (the whole Jones was already NaN) and
        NON_CONVERGED (that chanblock's RESULTS precision was NaN).

        This is a separate, explicit step rather than being done in
        __init__ so that constructing a group (e.g. just to inspect channel
        info) doesn't always pay the cost of reading every SOLUTIONS HDU.
        """
        self.jones = [soln.get_jones() for soln in self.solns]
        self.tile_flag_reasons = np.full(len(self.metafits_tiles_df), TileFlagReason.NONE, dtype=object)
        self.channel_flag_reasons = [
            np.full(file_jones.shape[:2], ChannelFlagReason.NONE, dtype=object) for file_jones in self.jones
        ]

        for file_jones, file_reasons, soln in zip(self.jones, self.channel_flag_reasons, self.solns, strict=True):
            pre_existing_nan = np.any(np.isnan(file_jones), axis=(-2, -1))  # shape (n_tiles, n_chanblocks)
            file_reasons[pre_existing_nan] |= ChannelFlagReason.PRE_EXISTING_NAN

            try:
                non_converged = ~soln.chanblock_converged  # shape (n_chanblocks,), broadcasts over tiles
            except KeyError:
                # No RESULTS HDU (older hyperdrive files) -- nothing to mark.
                continue
            still_unflagged = file_reasons == ChannelFlagReason.NONE
            file_reasons[still_unflagged & non_converged[np.newaxis, :]] |= ChannelFlagReason.NON_CONVERGED

    @classmethod
    def get_metafits_chan_info(cls, metafits: Metafits) -> ChanInfo:
        """Get coarse channel information from the observation's metafits.

        Validates that the coarse channel ranges do not overlap.

        Args:
            metafits: The observation's Metafits reader.

        Returns:
            Combined ChanInfo object.

        Raises:
            RuntimeError: If channel info is inconsistent or ranges overlap.
        """
        first_chan_info = metafits.chan_info
        all_ranges = sorted([*first_chan_info.coarse_chan_ranges], key=lambda x: x[0])

        # assert coarse channel ranges do not overlap
        for left, right in itertools.pairwise(all_ranges):
            if left[0] == right[0] or left[-1] >= right[0]:
                raise RuntimeError(f"coarse channel ranges from metafits overlap. {[left, right]}, {metafits=}")

        return ChanInfo(
            coarse_chan_ranges=all_ranges,
            fine_chan_width_hz=first_chan_info.fine_chan_width_hz,
            fine_chans_per_coarse=first_chan_info.fine_chans_per_coarse,
        )

    @classmethod
    def get_soln_chan_info(
        cls, metafits_chan_info: ChanInfo, solns: list[HyperfitsSolution]
    ) -> tuple[int, list[NDArray[np.int_]], list[int]]:
        """Get channel block information for provided solutions.

        Validates that channel info from metafits is consistent with solutions.

        Args:
            metafits_chan_info: Channel information from metafits files.
            solns: List of solution files.

        Returns:
            A tuple of (chanblocks_per_coarse, list of chanblocks_hz arrays,
            sorted list of coarse channel indices present across all solutions).

        Raises:
            RuntimeError: If channel info is inconsistent between solution and metafits.
        """
        chanblocks_per_coarse = None
        all_chanblocks_hz = []
        all_solution_coarse_chans: list[int] = []

        metafits_coarse_chans = np.concatenate(metafits_chan_info.coarse_chan_ranges)
        metafits_fine_chan_width_hz = metafits_chan_info.fine_chan_width_hz
        metafits_fine_chans_per_coarse = metafits_chan_info.fine_chans_per_coarse
        metafits_coarse_bandwidth_hz = metafits_fine_chan_width_hz * metafits_fine_chans_per_coarse

        for soln in solns:
            # coarse_chans = chaninfo.coarse_chan_ranges[coarse_chan_range_idx]
            chanblocks_hz = soln.chanblocks_hz

            if len(chanblocks_hz) < 2:
                raise RuntimeError(f"{soln.filename} - not enough chanblocks found ({chanblocks_hz=})")

            chanblock_width_hz = chanblocks_hz[1] - chanblocks_hz[0]

            if chanblock_width_hz % metafits_fine_chan_width_hz != 0:
                raise RuntimeError(
                    f"{soln.filename} - chanblock width in solution file ({chanblock_width_hz})"
                    f" is not a multiple of fine channel width in metafits ({metafits_fine_chan_width_hz})"
                )

            chans_per_block = int(chanblock_width_hz // metafits_fine_chan_width_hz)
            chanblocks_per_coarse_ = int(metafits_fine_chans_per_coarse // chans_per_block)

            if chanblocks_per_coarse is None:
                chanblocks_per_coarse = chanblocks_per_coarse_
            else:
                if chanblocks_per_coarse != chanblocks_per_coarse_:
                    raise RuntimeError(
                        f"{soln.filename} - chanblocks_per_coarse {chanblocks_per_coarse_}"
                        f" does not match previous value {chanblocks_per_coarse}"
                    )

            # break chanblocks into coarse channels
            soln_coarse_chans = []
            for coarse_chanblocks in np.split(chanblocks_hz, len(chanblocks_hz) // chanblocks_per_coarse):
                if len(coarse_chanblocks) == 1:
                    coarse_centroid_hz = coarse_chanblocks[0]
                else:
                    coarse_bandwidth_hz = coarse_chanblocks[-1] - coarse_chanblocks[0]
                    if coarse_bandwidth_hz > metafits_coarse_bandwidth_hz:
                        raise RuntimeError(
                            f"{soln.filename} - solution {coarse_bandwidth_hz=} > {metafits_coarse_bandwidth_hz=}"
                        )
                    coarse_centroid_hz = np.mean(coarse_chanblocks + chanblock_width_hz / 2)

                # NOTE: // already floors, so the np.round() that used to wrap this
                # was a no-op for the non-negative frequencies we deal with here.
                coarse_chan_idx = coarse_centroid_hz // metafits_coarse_bandwidth_hz

                if coarse_chan_idx not in metafits_coarse_chans:
                    raise RuntimeError(
                        f"{soln.filename} - solution coarse centroid {coarse_centroid_hz}Hz ({coarse_chan_idx=}) "
                        "not found in metafits coarse channels"
                    )

                if coarse_chan_idx in soln_coarse_chans:
                    raise RuntimeError(
                        f"{soln.filename} - solution coarse centroid {coarse_centroid_hz}Hz ({coarse_chan_idx=}) "
                        "already found in solution coarse channels"
                    )

                soln_coarse_chans.append(coarse_chan_idx)

            range_ncoarse = len(soln_coarse_chans)
            soln_ncoarse = len(chanblocks_hz) // chanblocks_per_coarse

            if range_ncoarse != soln_ncoarse:
                logger.warning(
                    f"{soln.filename} - warning: number of coarse channels in solution file ({soln_ncoarse=})"
                    f" does not match number of coarse channels in metafits for this range ({range_ncoarse=})"
                    f" given {chanblocks_per_coarse=}, {chans_per_block=}"
                )

            # Accumulate coarse channel indices found in this solution file so
            # we can later detect which metafits channels are missing solutions.
            all_solution_coarse_chans.extend(int(c) for c in soln_coarse_chans)

            all_chanblocks_hz.append(chanblocks_hz)

        if all_chanblocks_hz is None:
            raise RuntimeError("No valid channels found")

        if chanblocks_per_coarse is None:
            raise RuntimeError("chanblocks_per_coarse is none")

        return (
            chanblocks_per_coarse,
            all_chanblocks_hz,
            sorted(all_solution_coarse_chans),
        )

    @property
    def combined_tile_flags(self) -> NDArray[np.bool_]:
        """Get per-tile flagging, OR'd across all three independent sources.

        Combines, for every solution file in the group:
        - the metafits flag column (freshest -- regenerated every time,
          so it can carry tile-flagging updates made after the hyperdrive
          run that produced these solutions);
        - each file's TILES HDU flag column;
        - each file's BASELINES-HDU-inferred flagging (see
          read_baseline_tile_flags).

        Note: `refant` and `apply_tile_flags` both use this, so all three
        sources are honoured consistently.

        Returns:
            Boolean array, shape (n_tiles,). True where the tile is flagged
            by any of the three sources, in any of the group's solution
            files.
        """
        combined_flag = self.metafits_tiles_df["flag"].to_numpy(dtype=bool).copy()
        for soln in self.solns:
            combined_flag = np.logical_or(combined_flag, soln.tile_flags)
            combined_flag = np.logical_or(combined_flag, soln.baseline_tile_flags)
        return combined_flag

    def apply_tile_flags(self) -> None:
        """NaN out every chanblock of every tile flagged by any of the
        three tile-flag sources, and record why in tile_flag_reasons.

        Unlike combined_tile_flags (which just OR's everything together),
        this tracks each source separately so tile_flag_reasons can record
        which source(s) flagged a given tile.
        """
        self._ensure_loaded()
        assert self.jones is not None
        assert self.tile_flag_reasons is not None

        metafits_flagged = self.metafits_tiles_df["flag"].to_numpy(dtype=bool)
        tiles_hdu_flagged = np.zeros(len(self.metafits_tiles_df), dtype=bool)
        baseline_flagged = np.zeros(len(self.metafits_tiles_df), dtype=bool)
        for soln in self.solns:
            tiles_hdu_flagged |= soln.tile_flags
            baseline_flagged |= soln.baseline_tile_flags

        self.tile_flag_reasons[metafits_flagged] |= TileFlagReason.METAFITS
        self.tile_flag_reasons[tiles_hdu_flagged] |= TileFlagReason.HYPERDRIVE_TILE
        self.tile_flag_reasons[baseline_flagged] |= TileFlagReason.HYPERDRIVE_BASELINE

        bad_tile_mask = metafits_flagged | tiles_hdu_flagged | baseline_flagged
        for file_jones in self.jones:
            file_jones[bad_tile_mask, :, :, :] = np.nan + 1j * np.nan

    def enforce_whole_jones_nan(self) -> None:
        """Promote any partially-NaN Jones matrix to fully NaN.

        A "partial" entry has at least one of its 4 complex terms NaN but
        not all 4 -- e.g. only Dx is NaN while gx/Dy/gy are finite. Entries
        already fully NaN (whether from load()'s PRE_EXISTING_NAN/
        NON_CONVERGED marking or from apply_tile_flags()) are left alone,
        so this never re-marks or double-counts an already-handled entry.
        """
        self._ensure_loaded()
        assert self.jones is not None
        assert self.channel_flag_reasons is not None

        for file_jones, file_reasons in zip(self.jones, self.channel_flag_reasons, strict=True):
            any_nan = np.any(np.isnan(file_jones), axis=(-2, -1))
            all_nan = np.all(np.isnan(file_jones), axis=(-2, -1))
            partial = any_nan & ~all_nan
            file_jones[partial] = np.nan + 1j * np.nan
            file_reasons[partial] |= ChannelFlagReason.PARTIAL_JONES

    @property
    def all_chanblocks_hz_concat(self) -> NDArray[np.float64]:
        """Get all_chanblocks_hz concatenated into a single flat array.

        all_chanblocks_hz is a list of one array per solution file;
        several methods (process_phase_fits, process_gain_fits_for_db) need the
        whole group's chanblocks as a single array matching self.jones's
        concatenated tile/chanblock layout.
        """
        return np.concatenate(self.all_chanblocks_hz).astype(np.float64)

    @property
    def refant(self):
        """Get reference antenna (unflagged tile with lowest ID).

        Returns the first unflagged tile in the solutions and metafits,
        checking against combined_tile_flags (all three flag sources:
        metafits, TILES HDU, and BASELINES-HDU-inferred). Previously this
        computed its own metafits-OR-TILES-HDU check locally, missing the
        BASELINES source; now that combined_tile_flags is actually used
        elsewhere in the real pipeline, leaving refant on the weaker check
        would be a real correctness gap (it could pick a BASELINES-flagged
        tile as reference).

        Returns:
            A pandas Series representing the reference antenna row.

        Raises:
            ValueError: If no unflagged tiles are found.
        """
        unflagged_mask = ~self.combined_tile_flags
        if not unflagged_mask.any():
            raise ValueError("No unflagged tiles found")

        # Return the row with the lowest tile ID among unflagged tiles.
        candidate_ids = self.metafits_tiles_df["id"].to_numpy()
        best_idx = np.where(unflagged_mask)[0][np.argmin(candidate_ids[unflagged_mask])]
        return self.metafits_tiles_df.iloc[best_idx]

    @property
    def calibrator(self) -> str | None:
        """Get calibrator source name(s) from metafits file."""
        return self.metafits.calibrator

    @property
    def results(self) -> NDArray[np.float64]:
        """Get the combined results array from all solutions."""
        # Each file's results are fetched once and reused for both the length
        # check and the concatenate, to avoid opening the underlying FITS file
        # twice per access to this property.
        per_file_results = [soln.results for soln in self.solns]

        for soln, chanblocks_hz, soln_results in zip(self.solns, self.all_chanblocks_hz, per_file_results, strict=True):
            if len(chanblocks_hz) != len(soln_results):
                raise RuntimeError(
                    f"{soln.filename} - number of chanblocks ({len(chanblocks_hz)})"
                    f" does not match number of results ({len(soln_results)})"
                )

        results = np.concatenate(per_file_results)

        if results.size == 0:
            raise RuntimeError("No valid results found")

        return results

    @property
    def weights(self) -> NDArray[np.float64]:
        """Generate per-channel weights from hyperdrive convergence results.

        Convergence values < 0 or > 1e-4 are treated as invalid (set to NaN)
        and excluded from normalisation. The remaining values are transformed
        via exp(-result) and normalised to [0, 1].

        Returns:
            Float64 array of weights in [0, 1], one per chanblock. Invalid
            or NaN entries become 0.0 via np.nan_to_num.

        Note:
            Falls back to uniform weights of 1.0 if the solution file does
            not contain a RESULTS HDU (older hyperdrive versions).
        """
        try:
            results = self.results.copy()  # copy so we can mutate safely
            results[results < 0] = np.nan
            results[results > 1e-4] = np.nan
            exp_results = np.exp(-results)
            return np.nan_to_num(
                (exp_results - np.nanmin(exp_results)) / (np.nanmax(exp_results) - np.nanmin(exp_results))
            )
        except KeyError:
            # No RESULTS HDU (older hyperdrive files): fall back to uniform
            # weights. Must cover EVERY file's chanblocks, not just the first
            # one's -- callers index this alongside all_chanblocks_hz_concat,
            # so for a picket-fence observation with several solution files a
            # first-file-only length gave a silently mismatched array.
            n_chanblocks_total = sum(len(chanblocks_hz) for chanblocks_hz in self.all_chanblocks_hz)
            return np.full(n_chanblocks_total, 1.0)

    def _find_ref_tile_idx(self, refant_name: str) -> int:
        """Find and validate the reference antenna's tile index.

        Args:
            refant_name: Name of the reference antenna.

        Returns:
            The tile index (row position, matching self.jones's tile axis).

        Raises:
            RuntimeError: If the name isn't found, matches more than one
                tile, or that tile is flagged.
        """
        tile_names = self.metafits_tiles_df["name"].to_numpy()
        ref_mask = tile_names == refant_name
        if not ref_mask.any():
            raise RuntimeError(f"reference tile {refant_name} not found")
        if ref_mask.sum() > 1:
            raise RuntimeError(f"more than one tile named {refant_name} found")
        ref_tile_idx = int(np.where(ref_mask)[0][0])
        if self.combined_tile_flags[ref_tile_idx]:
            raise RuntimeError(f"reference tile {refant_name} is flagged (index {ref_tile_idx})")
        return ref_tile_idx

    def get_solns(self, refant_name=None) -> tuple[NDArray[np.int_], NDArray[np.complex128], NDArray[np.complex128]]:
        """Get tile IDs and XX/YY solutions for the reference antenna, from
        the in-memory Jones matrices (self.jones -- see load()).

        Args:
            refant_name: Name of the reference antenna. If None, no reference normalization is applied.

        Returns:
            A tuple of (tile_ids, xx_solutions, yy_solutions), each solution
            array shape (n_tiles, n_chanblocks_total) -- concatenated across
            all files in the group, no leading timeblock axis.

        Raises:
            RuntimeError: If load() hasn't been called, or the reference
                antenna is not found or flagged.
        """
        self._ensure_loaded()
        assert self.jones is not None
        tile_ids = self.metafits_tiles_df["id"].to_numpy()

        if refant_name is None:
            all_xx = np.concatenate([file_jones[..., 0, 0] for file_jones in self.jones], axis=1)
            all_yy = np.concatenate([file_jones[..., 1, 1] for file_jones in self.jones], axis=1)
            return tile_ids, all_xx, all_yy

        ref_tile_idx = self._find_ref_tile_idx(refant_name)
        all_ref_xx, all_ref_yy = [], []
        for file_jones in self.jones:
            ref_xx, ref_yy = _ref_normalise_xx_yy(file_jones, ref_tile_idx)
            all_ref_xx.append(ref_xx)
            all_ref_yy.append(ref_yy)
        return tile_ids, np.concatenate(all_ref_xx, axis=1), np.concatenate(all_ref_yy, axis=1)

    def get_solns_both(
        self, refant_name: str
    ) -> tuple[
        NDArray[np.int_],
        NDArray[np.complex128],
        NDArray[np.complex128],
        NDArray[np.complex128],
        NDArray[np.complex128],
    ]:
        """Return tile IDs, raw and reference-normalised XX/YY solutions,
        from the in-memory Jones matrices (self.jones -- see load()).

        The raw (un-normalised) solutions are needed for gain fitting; the
        reference-normalised solutions are needed for phase fitting. The
        reference normalisation applies the standard 2x2 determinant-inverse
        formula against the reference tile's Jones matrix, per chanblock.

        Args:
            refant_name: Name of the reference antenna (must not be flagged).

        Returns:
            A 5-tuple of ``(tile_ids, noref_xx, noref_yy, ref_xx, ref_yy)``,
            each solution array shape (n_tiles, n_chanblocks_total) --
            concatenated across all files in the group, no leading
            timeblock axis.

        Raises:
            RuntimeError: If load() hasn't been called, or the reference
                antenna is not found or flagged.
        """
        self._ensure_loaded()
        assert self.jones is not None
        tile_ids = self.metafits_tiles_df["id"].to_numpy()
        ref_tile_idx = self._find_ref_tile_idx(refant_name)

        all_noref_xx, all_noref_yy = [], []
        all_ref_xx, all_ref_yy = [], []
        for file_jones in self.jones:
            all_noref_xx.append(file_jones[..., 0, 0])
            all_noref_yy.append(file_jones[..., 1, 1])
            ref_xx, ref_yy = _ref_normalise_xx_yy(file_jones, ref_tile_idx)
            all_ref_xx.append(ref_xx)
            all_ref_yy.append(ref_yy)

        return (
            tile_ids,
            np.concatenate(all_noref_xx, axis=1),
            np.concatenate(all_noref_yy, axis=1),
            np.concatenate(all_ref_xx, axis=1),
            np.concatenate(all_ref_yy, axis=1),
        )

    def process_phase_fits(self, refant_name: str, phase_fit_niter: int) -> DataFrame:
        """Fit linear phase ramps to each tile and polarization.

        Args:
            refant_name: Name of the reference antenna.
            phase_fit_niter: Number of iterations for fitting.

        Returns:
            DataFrame with phase fit parameters for each tile and polarization,
            columns ["tile_id", "soln_idx", "pol", *PhaseFitInfo._fields].
        """
        self._ensure_loaded()
        soln_tile_ids, _noref_xx, _noref_yy, ref_xx, ref_yy = self.get_solns_both(refant_name)
        chanblocks_hz = self.all_chanblocks_hz_concat
        weights = self.weights

        futures = {}
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, ref_xx, ref_yy, strict=True)):
                for pol, solns in [(COL_XX, xx_solns), (COL_YY, yy_solns)]:
                    future = executor.submit(
                        _phase_fit_one,
                        soln_idx,
                        tile_id,
                        pol,
                        solns,
                        chanblocks_hz,
                        weights,
                        phase_fit_niter,
                        self.metafits_tiles_df,
                    )
                    futures[future] = (soln_idx, tile_id, pol)

        fits = [result for future in as_completed(futures) if (result := future.result()) is not None]
        return DataFrame(fits, columns=[COL_TILE_ID, COL_SOLN_IDX, COL_POL, *PhaseFitInfo._fields])

    def process_gain_fits_for_db(self, refant_name: str) -> DataFrame:
        """Fit gain solutions to each tile and polarization.

        Args:
            refant_name: Name of the reference antenna.

        Returns:
            DataFrame with gain fit parameters for each tile and polarization,
            columns ["tile_id", "soln_idx", "pol", *GainFitInfo._fields].
        """
        self._ensure_loaded()
        soln_tile_ids, noref_xx, noref_yy, _ref_xx, _ref_yy = self.get_solns_both(refant_name)
        chanblocks_hz = self.all_chanblocks_hz_concat
        weights = self.weights

        futures = {}
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(
                zip(soln_tile_ids, noref_xx, noref_yy, strict=True)
            ):
                for pol, solns in [(COL_XX, xx_solns), (COL_YY, yy_solns)]:
                    future = executor.submit(
                        _gain_fit_one,
                        soln_idx,
                        tile_id,
                        pol,
                        solns,
                        chanblocks_hz,
                        weights,
                        self.chanblocks_per_coarse,
                        self.metafits_tiles_df,
                    )
                    futures[future] = (soln_idx, tile_id, pol)

        fits = [result for future in as_completed(futures) if (result := future.result()) is not None]
        return DataFrame(fits, columns=[COL_TILE_ID, COL_SOLN_IDX, COL_POL, *GainFitInfo._fields])

    def flag_gain_max_cutoff(self, gain_max_cutoff: float | None) -> None:
        """Flag any (tile, chanblock) entry whose gx or gy amplitude
        exceeds an absolute sanity ceiling, regardless of population or
        per-tile trend.

        This catches a failure mode flag_amplitude_outliers cannot:
        hyperdrive's own per-chanblock convergence flag is shared across
        every tile at that chanblock, so it can't detect one tile's
        solve diverging to a spurious-but-numerically-stable value (e.g.
        gain amplitudes of 1e10+) while every other tile at the same
        chanblock converges normally. flag_amplitude_outliers wouldn't
        catch this either -- it fits each tile's own channel-to-channel
        trend, so if the whole trace is uniformly enormous, the fit (and
        its acceptance band) simply tracks the enormous baseline,
        flagging at most a handful of the most extreme points on top of
        it. An absolute ceiling, run first, removes the whole diverged
        trace before that per-tile fit ever sees it.

        Deliberately whole-Jones (both gx and gy NaN'd together, matching
        every other channel-level flag in this pipeline) even if only one
        polarisation exceeds the ceiling -- a Jones matrix with only one
        sane polarisation isn't meaningfully usable either.

        Run early enough (right after enforce_whole_jones_nan, before
        detect_phase_outliers and flag_amplitude_outliers) that neither of
        those later stages is misled by a diverged tile's garbage values:
        detect_phase_outliers's phase fit is computed from the same
        underlying complex Jones entries this removes, and
        flag_amplitude_outliers's per-tile fit would otherwise adapt to
        (and hide within) the same diverged baseline described above.

        If enough of a tile's channels exceed the ceiling,
        flag_mostly_bad_tiles (running after flag_amplitude_outliers)
        promotes it to fully flagged automatically -- no separate
        whole-tile logic is needed here.

        Args:
            gain_max_cutoff: Absolute gain-amplitude ceiling. None
                disables this check entirely (matches the historical
                "gains cut off/clipping disabled" config behaviour).
        """
        logger.info("flag_gain_max_cutoff")

        self._ensure_loaded()
        assert self.jones is not None
        assert self.channel_flag_reasons is not None

        if gain_max_cutoff is None:
            return

        for file_jones, file_reasons in zip(self.jones, self.channel_flag_reasons, strict=True):
            gx_amp = np.abs(file_jones[..., 0, 0])
            gy_amp = np.abs(file_jones[..., 1, 1])
            # NaN comparisons are always False, so already-NaN (already
            # bad) entries are naturally excluded without an explicit check.
            exceeds_cutoff = (gx_amp > gain_max_cutoff) | (gy_amp > gain_max_cutoff)
            if exceeds_cutoff.any():
                file_jones[exceeds_cutoff, :, :] = np.nan + 1j * np.nan
                file_reasons[exceeds_cutoff] |= ChannelFlagReason.GAIN_MAX_CUTOFF

    def flag_amplitude_outliers(self, poly_degree: int = 2, mad_residual_threshold: float = 10.0) -> None:
        """Flag per-channel gain-amplitude outliers, one contiguous file at a time.

        For each tile in each file, gx and gy amplitude are each fit
        independently with a sigma-clipped polynomial vs. chanblock index
        (see iterative_poly_clip_batch in calibration/outliers.py, which fits
        all tiles for a pol/file at once). A channel is
        flagged if EITHER polarisation's fit residual exceeds
        mad_residual_threshold -- if one polarisation's gain is corrupted,
        the other usually can't be trusted either.

        Deliberately run per file rather than across the whole group's
        concatenated chanblocks: a picket-fence observation's files can be
        widely separated in frequency, and a single polynomial fit across
        that gap would be meaningless.

        Also deliberately does NOT compare a tile's gains against other
        tiles: real gain bandpasses vary tile-to-tile for physical reasons
        (cable length, dipole position, beam response), so cross-tile
        comparison produces false positives.

        Stores the fit curves and acceptance bands used (self.amplitude_fit,
        self.amplitude_band -- one dict per file, keys "gx"/"gy") so a
        later plotting step can show exactly what was used to make this
        decision, rather than a fit recomputed against already-cleaned data.
        Also stores mad_residual_threshold itself (self.mad_residual_
        threshold), for that same plotting step to describe an
        amplitude-outlier reason accurately (e.g. "outside 10 MAD")
        without needing it threaded through as a separate parameter.

        Args:
            poly_degree: Degree of the polynomial fit to gain amplitude vs.
                chanblock index, per tile per polarisation.
            mad_residual_threshold: Number of residual-MADs beyond which a
                channel is an outlier (see iterative_poly_clip_batch).
        """
        logger.info("flag_amplitude_outliers")

        self._ensure_loaded()
        assert self.jones is not None
        assert self.channel_flag_reasons is not None

        self.amplitude_fit = []
        self.amplitude_band = []
        self.mad_residual_threshold = mad_residual_threshold

        for file_jones, file_reasons in zip(self.jones, self.channel_flag_reasons, strict=True):
            n_chanblocks = file_jones.shape[1]
            gx_amp = np.abs(file_jones[..., 0, 0])
            gy_amp = np.abs(file_jones[..., 1, 1])
            chan_idx = np.arange(n_chanblocks, dtype=np.float64)

            # Entries already bad for any reason (pre-existing NaN,
            # non-converged, partial-Jones-promoted, or already
            # tile-flagged -- tile-flagged entries are fully NaN so they
            # fall out of this NaN check automatically) are excluded from
            # the fit.
            already_bad = (file_reasons != ChannelFlagReason.NONE) | np.any(np.isnan(file_jones), axis=(-2, -1))
            initial_valid = ~already_bad

            valid_gx, _res_gx, fit_gx, mad_gx, med_gx = iterative_poly_clip_batch(
                chan_idx, gx_amp, poly_degree, mad_residual_threshold, initial_valid
            )
            valid_gy, _res_gy, fit_gy, mad_gy, med_gy = iterative_poly_clip_batch(
                chan_idx, gy_amp, poly_degree, mad_residual_threshold, initial_valid
            )

            poly_bad = initial_valid & (~valid_gx | ~valid_gy)
            if poly_bad.any():
                file_jones[poly_bad, :, :] = np.nan + 1j * np.nan
                file_reasons[poly_bad] |= ChannelFlagReason.AMPLITUDE_OUTLIER

            band_lower_gx = fit_gx + med_gx[:, None] - mad_residual_threshold * mad_gx[:, None]
            band_upper_gx = fit_gx + med_gx[:, None] + mad_residual_threshold * mad_gx[:, None]
            band_lower_gy = fit_gy + med_gy[:, None] - mad_residual_threshold * mad_gy[:, None]
            band_upper_gy = fit_gy + med_gy[:, None] + mad_residual_threshold * mad_gy[:, None]

            self.amplitude_fit.append({COL_GX: fit_gx, COL_GY: fit_gy})
            self.amplitude_band.append({COL_GX: (band_lower_gx, band_upper_gx), COL_GY: (band_lower_gy, band_upper_gy)})

    def detect_phase_outliers(self, refant_name: str, phase_fit_niter: int, nstd: float = 3.0) -> None:
        """Detect tiles whose phase fit is a population outlier, for reporting only.

        Runs process_phase_fits (a whole-group delay-line fit per tile per
        polarisation -- see fit_phase_line), then marks a tile as an
        outlier if either its XX or YY fit is a population outlier
        (median + nstd*MAD, robust and iteratively refined -- see
        reject_outliers) on either chi2dof or sigma_resid, relative to
        other tiles sharing both its polarisation *and* its receiver
        flavour (rx_type, e.g. RRI/SHAO/NI). Mirrors
        calibration.outliers.reject_outliers's existing use in
        debug_phase_fits (chi2dof then sigma_resid, sequentially).

        Grouping by flavour in addition to polarisation matters because
        different receiver flavours have measurably different natural
        chi2dof/sigma_resid distributions even after each tile's own
        cable delay is fit out -- confirmed on a real MWA observation,
        where one flavour's population was visibly tighter than another's
        even excluding genuine outliers. Pooling every flavour into one
        population before thresholding lets whichever flavour has the
        most tiles set a threshold that's too strict for a
        naturally-noisier minority flavour (over-flagging it) and too
        lenient for a naturally-tighter one (under-flagging it). See
        CALVIN.md's "Phase-outlier detection" section for the worked
        example this was based on.

        Deliberately does NOT flag or modify anything: unlike
        flag_amplitude_outliers, this never touches self.jones or
        self.tile_flag_reasons, and a tile found to be a phase outlier is
        never NaN'd or excluded. This was a permanent policy decision
        (not a config toggle): researchers wanted phase-outlier status
        reported (stats.txt's Flavor/PhOutlier columns, and the
        phase-fit debug plots) without the underlying calibration
        solution being touched.

        Runs last in run_flagging_pipeline (after flag_amplitude_outliers
        and flag_mostly_bad_tiles), specifically so self.phase_fits ends
        up equal to the truly final, fully-cleaned state --
        calvin.plots.stats_table.write_before_after_stats reuses it directly
        for the "after" stats-table row, and
        calvin.plots.phases.write_debug_phase_fit_plots reuses the same
        value for every phase-fit plot, rather
        than paying for a second, equally expensive process_phase_fits()
        call to get the same thing (phase fitting isn't cheap: roughly
        2 minutes for a 256-tile observation in testing). If this method
        moves earlier again for some reason, that reuse silently becomes
        wrong -- self.phase_fits would then reflect a mid-pipeline state,
        not the final one write_before_after_stats's callers expect.

        Args:
            refant_name: Name of the reference antenna.
            phase_fit_niter: Number of iterations for the phase ramp fit.
            nstd: Number of (MAD-derived) standard-deviation-equivalents
                beyond the population's robust median (per metric, per
                polarisation, per receiver flavour) before a tile is an
                outlier.
        """
        logger.info("detect_phase_outliers")

        self._ensure_loaded()

        phase_fits = self.process_phase_fits(refant_name, phase_fit_niter)
        self.phase_fits = annotate_phase_outliers(phase_fits, self.metafits_tiles_df, nstd=nstd)

    def flag_mostly_bad_tiles(self, threshold: float = 0.5) -> None:
        """Promote a partially-flagged tile to fully flagged if too much of it is bad.

        For each tile not already tile-flagged, computes the fraction of
        its chanblocks (summed across all files) carrying any per-channel
        flag reason (pre-existing NaN, non-converged, partial-Jones, above the
        gain-max cutoff, or amplitude-outlier -- combined total, regardless of
        which reason),
        and promotes it to fully flagged (MOSTLY_BAD_CHANNELS) if that
        fraction is >= threshold.

        Args:
            threshold: Fraction of chanblocks that must be bad (0-1) before
                the whole tile is flagged.
        """
        logger.info("flag_mostly_bad_tiles")

        self._ensure_loaded()
        assert self.jones is not None
        assert self.tile_flag_reasons is not None
        assert self.channel_flag_reasons is not None

        n_tiles = len(self.metafits_tiles_df)
        total_channels = np.zeros(n_tiles, dtype=int)
        bad_channels = np.zeros(n_tiles, dtype=int)
        for file_reasons in self.channel_flag_reasons:
            total_channels += file_reasons.shape[1]
            bad_channels += np.sum(file_reasons != ChannelFlagReason.NONE, axis=1)

        already_tile_flagged = self.tile_flag_reasons != TileFlagReason.NONE
        bad_fraction = np.divide(bad_channels, total_channels, out=np.zeros(n_tiles), where=total_channels > 0)
        to_promote = (~already_tile_flagged) & (bad_fraction >= threshold)
        if not to_promote.any():
            return

        self.tile_flag_reasons[to_promote] |= TileFlagReason.MOSTLY_BAD_CHANNELS
        for file_jones in self.jones:
            file_jones[to_promote, :, :, :] = np.nan + 1j * np.nan

    def run_flagging_pipeline(
        self,
        refant_name: str,
        phase_fit_niter: int,
        poly_degree: int = 2,
        mad_residual_threshold: float = 10.0,
        phase_outlier_nstd: float = 3.0,
        tile_bad_channel_fraction: float = 0.5,
        gain_max_cutoff: float | None = 100.0,
    ) -> None:
        """Run the full flagging pipeline in the standard order, capturing a
        "before" snapshot along the way.

        Consolidates the sequence previously duplicated between
        mwax_calvin_processor and cal_utils: apply_tile_flags() (cheap,
        structural), a "before" snapshot for later before/after reporting
        (see self.before_jones etc.), enforce_whole_jones_nan(),
        flag_gain_max_cutoff() (absolute sanity ceiling, does flag/NaN --
        run first so neither of the next two stages is misled by a
        diverged tile's garbage values; see its docstring),
        flag_amplitude_outliers() (per-file, per-tile, does flag/NaN),
        flag_mostly_bad_tiles() (sees the combined result of everything
        before it -- a tile cut off entirely by flag_gain_max_cutoff is
        promoted to fully flagged here via the ordinary
        bad-channel-fraction mechanism, with no separate whole-tile logic
        needed), then finally detect_phase_outliers() (whole-observation,
        report-only -- see its docstring for why this doesn't flag
        anything).

        detect_phase_outliers() deliberately runs last, not third: its
        result (self.phase_fits) is only ever actually consumed by
        calvin.plots.stats_table.write_before_after_stats() for the "after"
        stats-table row and calvin.plots.phases.write_debug_phase_fit_plots()
        for every phase-fit plot, both of which want the
        truly final, fully-cleaned state -- the same state
        write_stats_and_debug_plots (the two functions' unsplit
        predecessor) used to recompute for itself via a
        second, equally expensive process_phase_fits() call (phase
        fitting is not cheap: ~2 minutes for a 256-tile observation in
        testing). Running detect_phase_outliers after flag_amplitude_
        outliers/flag_mostly_bad_tiles makes self.phase_fits already
        equal to that final state, so write_before_after_stats can
        reuse it directly instead of paying that cost twice. (Running it
        before flag_gain_max_cutoff would be actively wrong -- a
        gain-diverged tile's phase fit, and the population statistics
        every other tile's outlier status is judged against, would be
        computed on visibly garbage data -- but there's no such
        dependency on flag_amplitude_outliers or flag_mostly_bad_tiles;
        it was simply never moved after them until now.)

        The "before" snapshot is captured right after apply_tile_flags() --
        i.e. before any of the phase/amplitude/mostly-bad-tile outlier
        detection has run, but with the cheap structural flags (metafits/
        TILES HDU/BASELINES-inferred) already applied. It's stored as
        self.before_jones, self.before_tile_flag_reasons,
        self.before_channel_flag_reasons, and self.before_phase_fits, for
        use by calvin.plots.stats_table.write_before_after_stats() (or a
        caller's own amplitude-outlier plots, e.g. plot_outlier_gains's
        pristine_jones argument). This one can't be reused for anything
        else -- it deliberately reflects the true pre-Calvin state, not
        the final one.

        Args:
            refant_name: Name of the reference antenna, used for both the
                "before" phase fit captured here and detect_phase_outliers().
            phase_fit_niter: Number of iterations for the phase ramp fit.
            poly_degree: Degree of the polynomial fit to gain amplitude vs.
                chanblock index (see flag_amplitude_outliers).
            mad_residual_threshold: MAD residual threshold for gain-
                amplitude outlier detection (see flag_amplitude_outliers).
            phase_outlier_nstd: Number of (MAD-derived) standard-deviation-
                equivalents beyond the population's robust median before a
                tile's phase fit is reported as an outlier (see
                detect_phase_outliers). Purely advisory --
                does not affect flagging. Must match the phase_outlier_nstd
                passed to a later write_before_after_stats()/
                write_debug_phase_fit_plots() call, or
                its "after" reporting will silently reflect this value
                instead of whatever it was itself given (see those
                functions' docstrings).
            tile_bad_channel_fraction: Fraction (0-1) of a
                tile's chanblocks that must already be flagged bad before
                the whole tile is promoted to fully flagged (see
                flag_mostly_bad_tiles).
            gain_max_cutoff: Absolute gain-amplitude ceiling (see
                flag_gain_max_cutoff). None disables this check.
        """
        self._ensure_loaded()
        self.apply_tile_flags()

        assert self.jones is not None
        assert self.tile_flag_reasons is not None
        assert self.channel_flag_reasons is not None
        self.before_jones = [file_jones.copy() for file_jones in self.jones]
        self.before_tile_flag_reasons = self.tile_flag_reasons.copy()
        self.before_channel_flag_reasons = [reasons.copy() for reasons in self.channel_flag_reasons]
        self.before_phase_fits = self.process_phase_fits(refant_name, phase_fit_niter)

        self.enforce_whole_jones_nan()
        self.flag_gain_max_cutoff(gain_max_cutoff)
        self.flag_amplitude_outliers(poly_degree, mad_residual_threshold)
        self.flag_mostly_bad_tiles(tile_bad_channel_fraction)
        self.detect_phase_outliers(refant_name, phase_fit_niter, nstd=phase_outlier_nstd)

    def commit(self, metafits_context: mwalib.MetafitsContext) -> list[str | None]:
        """Write all in-memory changes to disk: one backup + one write per file.

        Call this exactly once, after every flagging stage (apply_tile_flags,
        enforce_whole_jones_nan, flag_gain_max_cutoff, flag_amplitude_outliers,
        flag_mostly_bad_tiles, detect_phase_outliers) has run and any
        "after" plots have already been generated from self.jones --
        nothing written here should be modified further afterwards.

        Args:
            metafits_context: Populated mwalib.MetafitsContext, used to add
                the digital-gains column to each file's TILES HDU.

        Returns:
            List of backup file paths, one per solution file (parallel to
            self.solns), or None for any file written with backup=False
            (not currently used here, but write_jones supports it).
        """
        logger.info("writing solutions back to solutions files")

        self._ensure_loaded()
        assert self.jones is not None

        backup_paths = [soln.write_jones(file_jones) for soln, file_jones in zip(self.solns, self.jones, strict=True)]

        for soln in self.solns:
            add_digital_gains_column(soln.filename, metafits_context)

        return backup_paths
