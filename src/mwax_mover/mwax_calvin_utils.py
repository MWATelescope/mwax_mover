"""Calibration support utilities for the Calvin pipeline.

Provides data structures (Tile, Metafits, HyperfitsSolution, HyperfitsSolutionGroup,
GainFitInfo, PhaseFitInfo), the CalvinJobType enum (realtime / mwa_asvo), AOCAL
binary format constants and read/write helpers, SBATCH script generation
(create_sbatch_script) and submission (submit_sbatch), phase and gain fitting
functions, and estimate_birli_output_bytes() for storage pre-checks.
"""

from pathlib import Path

import mwalib

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
import datetime
from enum import Enum
import glob
import re
import json
import logging
import mimetypes
import os
import shutil
import time
import numpy as np
from astropy.io import fits
from astropy import units as u
from astropy.constants import c  # ty: ignore[unresolved-import]
from scipy.optimize import minimize
import pandas as pd
from pandas import DataFrame
import seaborn as sns
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from mwalib import MetafitsContext
import numpy.typing as npt  # noqa: F401
from numpy.typing import ArrayLike, NDArray
from typing import NamedTuple, List, Tuple, Optional, Union, Any  # noqa: F401
import sys
from mwax_mover.mwax_command import (
    run_command_ext,
    run_command_popen,
    check_popen_finished,
)
from mwax_mover.utils import is_int, extract_channels_from_filename, get_png_dimensions

logger = logging.getLogger(__name__)

V_LIGHT_M_S = 299792458.0

AOCAL_INTRO = "MWAOCAL\0".encode("utf8")
AOCAL_FILE_TYPE = 0
AOCAL_STRUCTURE_TYPE = 0
AOCAL_INTERVAL_COUNT = 1
AOCAL_POLS = 4  # XX,XY,YX,YY
AOCAL_VALUES = 2  # real and imaginary
DOUBLE_SIZE = 8  # 8 bytes for a float64
AOCAL_HEADER_FORMAT = "<8s6I2d"

# Standard number of MWA coarse channels.
MWA_NUM_COARSE_CHANS = 24


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

    coarse_chan_ranges: List[NDArray[np.int_]]  # List[Tuple(int, int)]
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

    def __init__(self, metafits: Union[str, MetafitsContext]):
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
    def tiles(self) -> List[Tile]:
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
    def inputs(self) -> List[Input]:
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
    def calibrator(self) -> Optional[str]:
        """Get calibrator source name from metafits.

        Returns None when the metafits carries an empty CALIBSRC string.
        """
        return self._mc.calibrator_source or None

    @property
    def obsid(self) -> int:
        """Get observation ID (GPS time) from metafits."""
        return self._mc.obs_id


class HyperfitsSolution:
    """A single calibration solution in hyperdrive FITS format"""

    def __init__(self, filename) -> None:
        """Initialize a HyperfitsSolution file reader.

        Args:
            filename: Path to the hyperdrive FITS solution file.
        """
        self.filename = filename

    @property
    def chanblocks_hz(self) -> NDArray[np.int_]:
        """Get channel block frequencies from the solution file."""
        with fits.open(self.filename) as hdus:
            freq_data = hdus["CHANBLOCKS"].data["Freq"].astype(np.int_)
            result = np.array(ensure_system_byte_order(freq_data))
            assert len(result), f"no chanblocks found in {self.filename}"

            # if multiple chanblocks, validate they are in order
            if len(result) > 1:
                diff = np.diff(result)
                if not np.all(diff >= 0):
                    raise RuntimeError(f"chanblocks are not in ascending order. {result=}")
                if not np.all(diff[1:] == diff[0]):
                    raise RuntimeError(f"chanblocks are not contiguous. {result=}")

            return result

    # @property
    # def tile_names_flags(self) -> List[Tuple[str, bool]]:
    #     """Get the tile names and flags ordered by index"""
    #     with fits.open(self.filename) as hdus:
    #         tile_data = hdus['TILES'].data
    #         return [
    #             (tile["TileName"], tile["Flag"])
    #             for tile in tile_data
    #         ]

    @property
    def tile_flags(self) -> List[bool]:
        """Get tile flags ordered by antenna index."""
        with fits.open(self.filename) as hdus:
            tile_data = hdus["TILES"].data
            return tile_data["Flag"]

    def get_average_times(self) -> List[float]:
        """Get the average time for each timeblock.

        Raises:
            KeyError: If TIMEBLOCKS HDU is not present.
        """
        with fits.open(self.filename) as hdus:
            time_data = hdus["TIMEBLOCKS"].data
            return [time["Average"] for time in time_data]

    def get_solutions(self) -> List[NDArray[np.complex128]]:
        """Get solutions as complex arrays.

        Returns:
            A list of four complex arrays (XX, XY, YX, YY) each with shape [time, tile, chan].
        """
        with fits.open(self.filename) as hdus:
            solutions = hdus["SOLUTIONS"].data
            return [
                solutions[:, :, :, 0] + 1j * solutions[:, :, :, 1],
                solutions[:, :, :, 2] + 1j * solutions[:, :, :, 3],
                solutions[:, :, :, 4] + 1j * solutions[:, :, :, 5],
                solutions[:, :, :, 6] + 1j * solutions[:, :, :, 7],
            ]

    def get_ref_solutions(self, ref_tile_idx=None) -> List[NDArray[np.complex128]]:
        """Get solutions divided by reference tile.

        Args:
            ref_tile_idx: Index of the reference tile. If None, returns raw solutions.

        Returns:
            A list of four complex arrays (XX, XY, YX, YY) each with shape [time, tile, chan],
            or raw solutions if ref_tile_idx is None.
        """
        solutions = self.get_solutions()

        if ref_tile_idx is None:
            return solutions

        # divide solutions by reference
        ref_solutions = [solution[:, ref_tile_idx, :] for solution in solutions]

        # divide solutions jones matrix by reference jones matrix, via inverse determinant
        ref_inv_det = np.divide(
            1 + 0j,
            ref_solutions[0] * ref_solutions[3] - ref_solutions[1] * ref_solutions[2],
        )

        return [
            (solutions[0] * ref_solutions[3] - solutions[1] * ref_solutions[2]) * ref_inv_det,
            (solutions[1] * ref_solutions[0] - solutions[0] * ref_solutions[1]) * ref_inv_det,
            (solutions[2] * ref_solutions[3] - solutions[3] * ref_solutions[2]) * ref_inv_det,
            (solutions[3] * ref_solutions[0] - solutions[2] * ref_solutions[1]) * ref_inv_det,
        ]

    @property
    def results(self) -> NDArray[np.float64]:
        """Get convergence results from the solution file.

        Returns:
            1-D float64 array of per-channel convergence values.

        Raises:
            KeyError: If the RESULTS HDU is not present. This is expected for
                older hyperdrive solution files. Callers that can tolerate missing
                results should catch KeyError and fall back to uniform weights.
        """
        with fits.open(self.filename) as hdus:
            # flatten() is required because astropy returns a structured array
            # from FITS binary table columns, not a plain ndarray.
            return hdus["RESULTS"].data.flatten()


class HyperfitsSolutionGroup:
    """A group of Hyperdrive FITS calibration solutions and corresponding metafits files."""

    def __init__(self, metafits: Metafits, solns: List[HyperfitsSolution]):
        """Initialize a solution group with metafits and solution files.

        Args:
            metafits: List of Metafits file readers.
            solns: List of HyperfitsSolution file readers.

        Raises:
            RuntimeError: If no metafits or solution files are provided.
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

    @classmethod
    def get_metafits_chan_info(cls, metafits: Metafits) -> ChanInfo:
        """Get combined channel information from all metafits files.

        Validates that channel ranges do not overlap and that channel info is consistent.

        Args:
            metafits: Metafits file object.

        Returns:
            Combined ChanInfo object.

        Raises:
            RuntimeError: If channel info is inconsistent or ranges overlap.
        """
        first_chan_info = metafits.chan_info
        all_ranges = sorted([*first_chan_info.coarse_chan_ranges], key=lambda x: x[0])

        # assert coarse channel ranges do not overlap
        for left, right in zip(all_ranges[:-1], all_ranges[1:]):
            if left[0] == right[0] or left[-1] >= right[0]:
                raise RuntimeError(f"coarse channel ranges from metafits overlap. {[left, right]}, {metafits=}")

        return ChanInfo(
            coarse_chan_ranges=all_ranges,
            fine_chan_width_hz=first_chan_info.fine_chan_width_hz,
            fine_chans_per_coarse=first_chan_info.fine_chans_per_coarse,
        )

    @classmethod
    def get_soln_chan_info(
        cls, metafits_chan_info: ChanInfo, solns: List[HyperfitsSolution]
    ) -> Tuple[int, List[NDArray[np.int_]], List[int]]:
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

                coarse_chan_idx = np.round(coarse_centroid_hz // metafits_coarse_bandwidth_hz)

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

        return (chanblocks_per_coarse, all_chanblocks_hz, sorted(all_solution_coarse_chans))

    @property
    def refant(self) -> pd.Series:
        """Get reference antenna (unflagged tile with lowest ID).

        Returns the first unflagged tile in the solutions and metafits.

        Returns:
            A pandas Series representing the reference antenna row.

        Raises:
            ValueError: If no unflagged tiles are found.
        """
        # Start from metafits flags then OR-in the solution flags for each file,
        # without copying the full DataFrame.
        combined_flag = self.metafits_tiles_df["flag"].to_numpy(dtype=bool)
        for soln in self.solns:
            combined_flag = np.logical_or(combined_flag, soln.tile_flags)

        unflagged_mask = ~combined_flag
        if not unflagged_mask.any():
            raise ValueError("No unflagged tiles found")

        # Return the row with the lowest tile ID among unflagged tiles.
        candidate_ids = self.metafits_tiles_df["id"].to_numpy()
        best_idx = np.where(unflagged_mask)[0][np.argmin(candidate_ids[unflagged_mask])]
        return self.metafits_tiles_df.iloc[best_idx]

    @property
    def calibrator(self):
        """Get calibrator source name(s) from metafits file."""
        return self.metafits.calibrator

    @property
    def results(self) -> NDArray[np.float64]:
        """Get the combined results array from all solutions."""
        for soln, chanblocks_hz in zip(self.solns, self.all_chanblocks_hz):
            if len(chanblocks_hz) != len(soln.results):
                raise RuntimeError(
                    f"{soln.filename} - number of chanblocks ({len(chanblocks_hz)})"
                    f" does not match number of results ({len(soln.results)})"
                )

        results = np.concatenate([soln.results for soln in self.solns])

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
            return np.full(len(self.all_chanblocks_hz[0]), 1.0)

    def get_solns(self, refant_name=None) -> Tuple[NDArray[np.int_], NDArray[np.complex128], NDArray[np.complex128]]:
        """Get tile IDs and XX/YY solutions for the reference antenna.

        Args:
            refant_name: Name of the reference antenna. If None, no reference normalization is applied.

        Returns:
            A tuple of (tile_ids, xx_solutions, yy_solutions).

        Raises:
            RuntimeError: If reference antenna is not found or flagged in solutions.
        """
        # Pre-extract metafits arrays once to avoid per-iteration DataFrame copies.
        tile_names = self.metafits_tiles_df["name"].to_numpy()
        tile_ids = self.metafits_tiles_df["id"].to_numpy()
        metafits_flags = self.metafits_tiles_df["flag"].to_numpy(dtype=bool)

        soln_tile_ids = None
        ref_tile_idx = None
        all_xx_solns = None
        all_yy_solns = None

        for chanblocks_hz, soln in zip(self.all_chanblocks_hz, self.solns):
            # TODO: ch_flags = hdus['CHANBLOCKS'].data['Flag']
            # TODO: results = hdus['RESULTS'].data.flatten()

            # Merge metafits and solution flags without copying the full DataFrame.
            combined_flag = np.logical_or(metafits_flags, soln.tile_flags)

            if refant_name is not None:
                ref_mask = tile_names == refant_name

                if not ref_mask.any():
                    raise RuntimeError(f"{soln.filename} - reference tile {refant_name} not found in solution file")

                if ref_mask.sum() > 1:
                    raise RuntimeError(
                        f"{soln.filename} - more than one tile with name {refant_name} found in solution file"
                    )

                _ref_tile_idx = int(np.where(ref_mask)[0][0])

                if combined_flag[_ref_tile_idx]:
                    raise RuntimeError(
                        f"{soln.filename} - reference tile {refant_name}"
                        f" is flagged in solutions file (index {_ref_tile_idx})"
                    )

                # FIX 2: use `is None` instead of falsy check so that index 0 is handled correctly
                if ref_tile_idx is None:
                    ref_tile_idx = _ref_tile_idx
                elif ref_tile_idx != _ref_tile_idx:
                    raise RuntimeError(
                        f"{soln.filename} - reference tile in solution file does not match previous solution files"
                    )

            _tile_ids = tile_ids

            # _tile_ids, _ref_tile_idx = soln.validate_tiles(tiles_by_name, refant)
            if soln_tile_ids is None or not len(soln_tile_ids):
                soln_tile_ids = tile_ids
            elif not np.array_equal(soln_tile_ids, _tile_ids):
                raise RuntimeError(
                    f"{soln.filename} - tile selection in solution file"
                    f" does not match previous solution files.\n"
                    f" previous:\n{_tile_ids}\n"
                    f" this:\n{soln_tile_ids}"
                )

            # validate timeblocks
            try:
                avg_times = soln.get_average_times()
            except KeyError:
                # actual time values are not actually used anyway, just length.
                solutions = soln.get_solutions()
                n_times = solutions[0].shape[0]
                avg_times = [float("nan")] * n_times

            # TODO: support multiple timeblocks
            if len(avg_times) != 1:
                raise RuntimeError(f"{soln.filename} - exactly 1 timeblock must be provided: ({len(avg_times)})")

            # TODO: compare with metafits times

            # validate solutions
            solutions = soln.get_ref_solutions(ref_tile_idx)

            for solution in solutions:
                if (ntimes := solution.shape[0]) != 1:
                    raise RuntimeError(
                        f"{soln.filename} - number of timeblocks in SOLUTIONS HDU ({ntimes})"
                        f" does not match number of timeblocks in TIMEBLOCKS HDU ({len(avg_times)})"
                    )

                if (ntiles := solution.shape[1]) != len(soln_tile_ids):
                    raise RuntimeError(
                        f"{soln.filename} - number of tiles in SOLUTIONS HDU ({ntiles})"
                        f" does not match number of tiles in TILES HDU ({len(soln_tile_ids)})"
                    )

                if (nchans := solution.shape[2]) != len(chanblocks_hz):
                    raise RuntimeError(
                        f"{soln.filename} - number of channels in SOLUTIONS HDU ({nchans})"
                        f" does not match number of channels in CHANBLOCKS HDU ({len(chanblocks_hz)})"
                    )

            # TODO: sanity check, ref_solutions should be identity matrix or NaN

            if all_xx_solns is None:
                all_xx_solns = solutions[0]
            else:
                all_xx_solns = np.concatenate((all_xx_solns, solutions[0]), axis=2)

            if all_yy_solns is None:
                all_yy_solns = solutions[3]
            else:
                all_yy_solns = np.concatenate((all_yy_solns, solutions[3]), axis=2)

        if soln_tile_ids is None or all_xx_solns is None or all_yy_solns is None:
            raise RuntimeError("No valid solutions found")

        return soln_tile_ids, all_xx_solns, all_yy_solns

    def get_solns_both(
        self, refant_name: str
    ) -> Tuple[
        NDArray[np.int_],
        NDArray[np.complex128],
        NDArray[np.complex128],
        NDArray[np.complex128],
        NDArray[np.complex128],
    ]:
        """Return tile IDs, raw and reference-normalised XX/YY solutions in one FITS read pass.

        Equivalent to calling ``get_solns()`` and ``get_solns(refant_name)``
        back-to-back, but reads each solution FITS file only once instead of
        twice.  The raw (un-normalised) solutions are needed for gain fitting;
        the reference-normalised solutions are needed for phase fitting.

        The reference normalisation replicates
        ``HyperfitsSolution.get_ref_solutions()`` on the already-read raw data,
        avoiding a second FITS open.  The Jones matrix inverse of the reference
        tile is applied channel-wise via the standard 2×2 determinant formula.

        Args:
            refant_name: Name of the reference antenna (must not be flagged in
                either the metafits or the solution file).

        Returns:
            A 5-tuple of ``(tile_ids, noref_xx, noref_yy, ref_xx, ref_yy)``.

        Raises:
            RuntimeError: If the reference antenna is not found, is flagged, or
                if solution data is inconsistent across files.
        """
        # Pre-extract metafits arrays once so the per-file loop needs no
        # DataFrame copies and no repeated column access.
        tile_names = self.metafits_tiles_df["name"].to_numpy()
        tile_ids = self.metafits_tiles_df["id"].to_numpy()
        metafits_flags = self.metafits_tiles_df["flag"].to_numpy(dtype=bool)

        soln_tile_ids = None
        ref_tile_idx: Optional[int] = None
        all_noref_xx: Optional[NDArray[np.complex128]] = None
        all_noref_yy: Optional[NDArray[np.complex128]] = None
        all_ref_xx: Optional[NDArray[np.complex128]] = None
        all_ref_yy: Optional[NDArray[np.complex128]] = None

        for chanblocks_hz, soln in zip(self.all_chanblocks_hz, self.solns):
            # Merge flags without a DataFrame copy.
            combined_flag = np.logical_or(metafits_flags, soln.tile_flags)

            # Find and validate the reference antenna.
            ref_mask = tile_names == refant_name
            if not ref_mask.any():
                raise RuntimeError(f"{soln.filename} - reference tile {refant_name} not found in solution file")
            if ref_mask.sum() > 1:
                raise RuntimeError(
                    f"{soln.filename} - more than one tile with name {refant_name} found in solution file"
                )
            _ref_tile_idx = int(np.where(ref_mask)[0][0])
            if combined_flag[_ref_tile_idx]:
                raise RuntimeError(
                    f"{soln.filename} - reference tile {refant_name}"
                    f" is flagged in solutions file (index {_ref_tile_idx})"
                )
            if ref_tile_idx is None:
                ref_tile_idx = _ref_tile_idx
            elif ref_tile_idx != _ref_tile_idx:
                raise RuntimeError(
                    f"{soln.filename} - reference tile in solution file does not match previous solution files"
                )

            # Tile ID consistency check.
            if soln_tile_ids is None:
                soln_tile_ids = tile_ids
            elif not np.array_equal(soln_tile_ids, tile_ids):
                raise RuntimeError(f"{soln.filename} - tile IDs do not match previous solution files")

            # Single FITS read for all solution data — this is the key difference
            # from two separate get_solns() calls.
            raw_solutions = soln.get_solutions()

            # Validate timeblock count directly from the solutions array,
            # avoiding a separate FITS open for the TIMEBLOCKS HDU.
            n_times = raw_solutions[0].shape[0]
            if n_times != 1:
                raise RuntimeError(f"{soln.filename} - exactly 1 timeblock must be provided: ({n_times})")

            # Shape validation.
            for solution in raw_solutions:
                if (ntimes := solution.shape[0]) != 1:
                    raise RuntimeError(f"{soln.filename} - SOLUTIONS HDU timeblock count ({ntimes}) != 1")
                if (ntiles := solution.shape[1]) != len(soln_tile_ids):
                    raise RuntimeError(
                        f"{soln.filename} - number of tiles in SOLUTIONS HDU ({ntiles})"
                        f" does not match TILES HDU ({len(soln_tile_ids)})"
                    )
                if (nchans := solution.shape[2]) != len(chanblocks_hz):
                    raise RuntimeError(
                        f"{soln.filename} - number of channels in SOLUTIONS HDU ({nchans})"
                        f" does not match CHANBLOCKS HDU ({len(chanblocks_hz)})"
                    )

            # Raw (un-normalised) XX and YY for gain fitting.
            noref_xx = raw_solutions[0]
            noref_yy = raw_solutions[3]

            # Reference-normalised XX and YY for phase fitting.
            # Replicates HyperfitsSolution.get_ref_solutions() on already-read data.
            # ref[i] has shape (1, n_chans); broadcasting over the tiles axis is implicit.
            ref = [s[:, ref_tile_idx, :] for s in raw_solutions]
            ref_inv_det = np.divide(1 + 0j, ref[0] * ref[3] - ref[1] * ref[2])
            ref_xx = (raw_solutions[0] * ref[3] - raw_solutions[1] * ref[2]) * ref_inv_det
            ref_yy = (raw_solutions[3] * ref[0] - raw_solutions[2] * ref[1]) * ref_inv_det

            # Accumulate across solution files.
            if all_noref_xx is None or all_noref_yy is None or all_ref_xx is None or all_ref_yy is None:
                all_noref_xx, all_noref_yy = noref_xx, noref_yy
                all_ref_xx, all_ref_yy = ref_xx, ref_yy
            else:
                all_noref_xx = np.concatenate((all_noref_xx, noref_xx), axis=2)
                all_noref_yy = np.concatenate((all_noref_yy, noref_yy), axis=2)
                all_ref_xx = np.concatenate((all_ref_xx, ref_xx), axis=2)
                all_ref_yy = np.concatenate((all_ref_yy, ref_yy), axis=2)

        if (
            soln_tile_ids is None
            or all_noref_xx is None
            or all_noref_yy is None
            or all_ref_xx is None
            or all_ref_yy is None
        ):
            raise RuntimeError("No valid solutions found")

        return soln_tile_ids, all_noref_xx, all_noref_yy, all_ref_xx, all_ref_yy


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
    gains: List[float]
    pol0: List[float]
    pol1: List[float]
    sigma_resid: List[float]

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
    values: List[float],
    actual_chans: List[int],
    expected_chans: NDArray[np.int_],
) -> List[float]:
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
    padded: List[float] = [np.nan] * n_expected
    for i, chan_idx in enumerate(actual_chans):
        positions = np.where(expected_chans == chan_idx)[0]
        if len(positions) == 1:
            padded[positions[0]] = values[i]
    return padded


def pad_gain_fit_info(
    gain_fit: GainFitInfo,
    actual_coarse_chans: List[int],
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
) -> Optional[pd.DataFrame]:
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
        plot_phase_residual(freqs, soln_xx, soln_yy, weights, prefix, title, plot_residual, residual_vmax, flavor_fits)
    if len(flavor_fits):
        plot_phase_intercepts(prefix, show, title, flavor_fits)

    phase_fits_pivot = pivot_phase_fits(phase_fits, tiles)
    weights2 = weights**2

    if prefix:
        phase_fits_pivot.to_csv(f"{prefix}phase_fits.tsv", sep="\t", index=False)

    if len(phase_fits_pivot):
        plot_phase_fits(freqs, soln_xx, soln_yy, prefix, show, title, half_blues, phase_fits_pivot, weights2)

    return phase_fits_pivot


def reject_outliers(data, quality_key, nstd=3.0):
    """Mark outliers in a DataFrame based on a quality metric.

    Args:
        data: Input DataFrame with a 'pol' column and quality column.
        quality_key: Name of the column to use for outlier detection.
        nstd: Number of standard deviations for outlier threshold (default: 3.0).

    Returns:
        DataFrame with an 'outlier' column added/updated marking outliers.
    """
    if nstd == 0:
        return data
    if "outlier" not in data.columns:
        data["outlier"] = False
    for pol in data["pol"].unique():
        idx_pol_good = np.where(np.logical_and(data["pol"] == pol, ~data["outlier"]))[0]
        quality_thresh = data.loc[idx_pol_good, quality_key].mean() + nstd * data.loc[idx_pol_good, quality_key].std()
        if nstd >= 0:
            data.loc[data[quality_key] >= quality_thresh, "outlier"] = True
        else:
            data.loc[data[quality_key] <= quality_thresh, "outlier"] = True

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
                f"{fit['name']}|{fit['soln_idx']}", color=color, weight="semibold", fontfamily="monospace"
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
        subplot_kws=dict(projection="polar"),
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


def plot_phase_residual(freqs, soln_xx, soln_yy, weights, prefix, title, plot_res, residual_vmax, flavor_fits):
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
        soln_idxs: pd.Series, pols: pd.Series, flavs: pd.Series, lengths: pd.Series, intercepts: pd.Series, **kwargs
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
    tile_columns += [*(set(tiles.columns) - set(tile_columns) - set(["id"]))]
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
    soln = HyperfitsSolution(solutions_fits_file)
    results = soln.results
    converged_channel_indices = np.where(~np.isnan(results))
    summary = []
    summary.append(results)
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
    obs_id: int,
    hyperdrive_solution_filename: str,
    hyperdrive_binary_path: str,
    metafits_filename: str,
) -> Tuple[bool, str]:
    """Generate solution plots.

    Args:
        obs_id: Observation ID.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        metafits_filename: Path to the metafits file.

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(f"{obs_id} generating plots for {hyperdrive_solution_filename}...")

    try:
        # Now run hyperdrive again to do some plots
        hyp_soln_plot_args = f"--max-amp 5 --output-directory {os.path.dirname(hyperdrive_solution_filename)}"
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
    stats_filename: str,
    hyperdrive_solution_filename: str,
) -> Tuple[bool, str]:
    """Write convergence statistics.

    Args:
        obs_id: Observation ID.
        stats_filename: Path to write statistics to.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(f"{obs_id} Writing stats for {hyperdrive_solution_filename} to {stats_filename}...")

    try:
        conv_summary_list = get_convergence_summary(hyperdrive_solution_filename)

        with open(stats_filename, "w", encoding="UTF-8") as stats:
            for row in conv_summary_list:
                stats.write(f"{row[0]}: {row[1]}\n")

        logger.info(f"{obs_id} Finished running hyperdrive stats on {hyperdrive_solution_filename}.")
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
    input_uvfits_files: List[str],
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


def run_hyperdrive_stats(
    input_solution_files: list[str],
    metafits_filename: str,
    obs_id: int,
    hyperdrive_binary_path: str,
    hyperdrive_output_path: str,
) -> bool:
    """Generate statistics and plots from hyperdrive solution files.

    Args:
        input_solution_files: List of input hyperdrive solution files (full filename).
        metafits_filename: Path to the metafits file.
        obs_id: Observation ID.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        hyperdrive_output_path: Directory containing hyperdrive outputs.

    Returns:
        True if all stats generation succeeded, False otherwise.
    """
    # produce stats/plots
    plots_successful: int = 0
    stats_successful: int = 0

    logger.info(
        f"{obs_id}: {len(input_solution_files)} contiguous bands detected."
        f" Running hyperdrive stats {len(input_solution_files)} times...."
    )

    for hyperdrive_run, solution_filename in enumerate(input_solution_files):
        # Take the filename which for picket fence will also have
        # the band info and in all cases the obsid. We will use
        # this as a base for other files we work with
        obsid_and_band = os.path.basename(solution_filename).replace("_solutions.fits", "")

        (
            plots_success,
            plots_error,
        ) = generate_hyperdrive_plots(
            obs_id,
            solution_filename,
            hyperdrive_binary_path,
            metafits_filename,
        )

        # Write the stats to the output dir
        stats_filename = os.path.join(hyperdrive_output_path, f"{obsid_and_band}_stats.txt")

        (
            stats_success,
            stats_error,
        ) = write_hyperdrive_stats(
            obs_id,
            stats_filename,
            solution_filename,
        )

        if plots_success:
            plots_successful += 1
        else:
            logger.warning(
                f"{obs_id}: hyperdrive plots run"
                f" {hyperdrive_run + 1}/{len(input_solution_files)} FAILED:"
                f" {plots_error}."
            )

        if stats_success:
            stats_successful += 1
        else:
            logger.warning(
                f"{obs_id}: hyperdrive stats run"
                f" {hyperdrive_run + 1}/{len(input_solution_files)} FAILED:"
                f" {stats_error}."
            )

    if plots_successful == len(input_solution_files):
        logger.info(f"{obs_id}: All {plots_successful} hyperdrive plots runs successful")
    else:
        logger.warning(f"{obs_id}: Not all hyperdrive plots runs were successful.")

    if stats_successful == len(input_solution_files):
        logger.info(f"{obs_id}: All {stats_successful} hyperdrive stats runs successful")
    else:
        logger.warning(f"{obs_id}: Not all hyperdrive stats runs were successful.")

    return plots_successful == stats_successful == len(input_solution_files)


def process_phase_fits(tiles, chanblocks_hz, all_xx_solns, all_yy_solns, weights, soln_tile_ids, phase_fit_niter):
    """Fit linear phase ramps to each tile and polarization.

    Args:
        tiles: DataFrame with tile information.
        chanblocks_hz: Array of channel block frequencies in Hz.
        all_xx_solns: XX polarization solutions for all tiles.
        all_yy_solns: YY polarization solutions for all tiles.
        weights: Weight values for each solution.
        soln_tile_ids: Tile IDs in the solutions.
        phase_fit_niter: Number of iterations for fitting.

    Returns:
        DataFrame with phase fit parameters for each tile and polarization.
    """
    futures = {}

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
            for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
                future = executor.submit(
                    _phase_fit_one,
                    soln_idx,
                    tile_id,
                    pol,
                    solns,
                    chanblocks_hz,
                    weights,
                    phase_fit_niter,
                    tiles,
                )
                futures[future] = (soln_idx, tile_id, pol)

    fits = [result for future in as_completed(futures) if (result := future.result()) is not None]

    return DataFrame(fits, columns=["tile_id", "soln_idx", "pol", *PhaseFitInfo._fields])


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
    name = tile.name
    try:
        fit = fit_phase_line(chanblocks_hz, solns, weights, niter=phase_fit_niter)
    except Exception:
        logger.exception(f"Error: {tile_id=:4} {pol} ({name})")
        return None
    # uncomment me for verbose debug
    # logger.debug(f"{tile_id=:4} {pol} ({name}) {fit=}")
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
    name = tile.name
    try:
        fit = fit_gain(chanblocks_hz, solns, weights, chanblocks_per_coarse)
        # uncomment me for verbose debug
        # logger.debug(f"{tile_id=:4} {pol} ({name}) {fit=}")
        # logger.debug(f"gains: {fit.gains}")
    except Exception:
        logger.exception(f"Error: {tile_id=:4} {pol} ({name})")
        return None
    return [tile_id, soln_idx, pol, *fit]


def process_gain_fits(
    tiles: DataFrame,
    chanblocks_hz: NDArray[np.float64],
    all_xx_solns: NDArray[np.complex128],
    all_yy_solns: NDArray[np.complex128],
    weights: NDArray[np.float64],
    soln_tile_ids: NDArray[np.signedinteger[Any]],
    chanblocks_per_coarse: int,
) -> DataFrame:
    """Fit gain solutions to each tile and polarization.

    Args:
        tiles: DataFrame with tile information.
        chanblocks_hz: Array of channel block frequencies in Hz.
        all_xx_solns: XX polarization solutions for all tiles.
        all_yy_solns: YY polarization solutions for all tiles.
        weights: Weight values for each solution.
        soln_tile_ids: Tile IDs in the solutions.
        chanblocks_per_coarse: Number of channel blocks per coarse channel.

    Returns:
        DataFrame with gain fit parameters for each tile and polarization.
    """
    futures = {}

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
            for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
                future = executor.submit(
                    _gain_fit_one,
                    soln_idx,
                    tile_id,
                    pol,
                    solns,
                    chanblocks_hz,
                    weights,
                    chanblocks_per_coarse,
                    tiles,
                )
                futures[future] = (soln_idx, tile_id, pol)

    fits = [result for future in as_completed(futures) if (result := future.result()) is not None]

    return DataFrame(fits, columns=["tile_id", "soln_idx", "pol", *GainFitInfo._fields])


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


def submit_sbatch(script_path: str, script: str, obs_id: int, request_ids: list[int]) -> Tuple[bool, Optional[int]]:
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
        logger.exception(f"{str(obs_id)} failure creating temp sbatch script.")
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
                exit(-10)
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


# def split_aocal_file_into_coarse_channels(
#     obs_id: int, input_aocal_filename: str, input_rec_chans: list[int], output_dir: str
# ) -> list[str]:
#     """Split an AOCAL file into one file per coarse channel.

#     Args:
#         obs_id: Observation ID.
#         input_aocal_filename: Path to the input AOCAL binary file.
#         input_rec_chans: List of receiver channel numbers in the file.
#         output_dir: Directory to write the split AOCAL files to.

#     Returns:
#         List of output AOCAL filenames written.

#     Raises:
#         ValueError: If the AOCAL file format is invalid.
#     """
#     # Given any aocal file which may have 1 - 24 coarse channels of data within, split it into 1 file per coarse channel
#     # Since aocal files have minimal metadata in the file, give the function input_rec_chans which is a hint as to how many
#     # and what the coarse chans are in the aocal file and what their receiver chan numbers are.
#     #
#     # Returns a list of new aocal filenames written
#     #
#     # Assuming a normal contiguous aocal file of 24 coarse chans, you would get 24 aocal files, all with the same header
#     # called: obsid_chXXX_aocal.bin where XXX is the rec chan number

#     # If it is picket fence and there are, say, 3 8 channel aocal files, you would run this function 3 times and each time
#     # you would get 8 files, 1 per coarse chan

#     # See: https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/1190658052/aocal+File+Format for description of file format

#     # get count of coarse channels worth of cal data provided
#     input_aocal_coarse_chans = len(input_rec_chans)

#     # Do some validity checks
#     file_size_bytes: int = os.stat(input_aocal_filename).st_size

#     # header size
#     header_size_bytes = struct.calcsize(AOCAL_HEADER_FORMAT)

#     # Data size
#     data_size_bytes = file_size_bytes - header_size_bytes

#     # Read the header of the file
#     with open(input_aocal_filename, "rb") as in_file:
#         header_bytes = in_file.read(header_size_bytes)
#         (
#             intro,
#             file_type,
#             structure_type,
#             interval_count,
#             antenna_count,
#             input_aocal_fine_channel_count,
#             polarisation_count,
#             start_gpstime,
#             end_gpstime,
#         ) = struct.unpack(AOCAL_HEADER_FORMAT, header_bytes)

#         if intro != AOCAL_INTRO:
#             raise ValueError(f"aocal file {input_aocal_filename} does not start with expected string {AOCAL_INTRO}")

#         if file_type != AOCAL_FILE_TYPE:
#             raise ValueError(
#                 f"aocal file {input_aocal_filename} has invalid file_type {file_type} expected {AOCAL_FILE_TYPE}"
#             )

#         if structure_type != AOCAL_STRUCTURE_TYPE:
#             raise ValueError(
#                 f"aocal file {input_aocal_filename} has invalid structure_type {structure_type} expected {AOCAL_STRUCTURE_TYPE}"
#             )

#         if interval_count != AOCAL_INTERVAL_COUNT:
#             raise ValueError(
#                 f"aocal file {input_aocal_filename} has invalid interval_count {interval_count} expected {AOCAL_INTERVAL_COUNT}"
#             )

#         if polarisation_count != AOCAL_POLS:
#             raise ValueError(
#                 f"aocal file {input_aocal_filename} has invalid polarisation_count {polarisation_count} expected {AOCAL_POLS}"
#             )

#         input_data = in_file.read()

#     # Expected data size
#     expected_input_data_size_bytes = (
#         AOCAL_INTERVAL_COUNT
#         * antenna_count
#         * input_aocal_fine_channel_count
#         * polarisation_count
#         * AOCAL_VALUES
#         * DOUBLE_SIZE
#     )

#     if expected_input_data_size_bytes != data_size_bytes:
#         raise ValueError(
#             f"aocal file {input_aocal_filename} data size of {data_size_bytes} doesn't match expected size of {expected_input_data_size_bytes}"
#         )

#     if expected_input_data_size_bytes != len(input_data):
#         raise ValueError(
#             f"aocal file {input_aocal_filename} read data size of {len(input_data)} which doesn't match expected size of {expected_input_data_size_bytes}"
#         )

#     fine_chans_per_coarse = int(input_aocal_fine_channel_count / input_aocal_coarse_chans)

#     np_data = np.frombuffer(input_data, dtype=np.float64)
#     np_data = np.reshape(
#         np_data,
#         (
#             AOCAL_INTERVAL_COUNT,
#             antenna_count,
#             input_aocal_coarse_chans,
#             fine_chans_per_coarse,
#             polarisation_count,
#             AOCAL_VALUES,
#         ),
#     )

#     # This is the number of doubles
#     values_per_coarse_chan = (
#         AOCAL_INTERVAL_COUNT * antenna_count * fine_chans_per_coarse * polarisation_count * AOCAL_VALUES
#     )
#     bytes_per_coarse_chan = values_per_coarse_chan * 8

#     out_filenames = []

#     for c_idx, rec_chan_no in enumerate(input_rec_chans):
#         out_filename = os.path.join(
#             output_dir,
#             get_aocal_filename(obs_id, antenna_count, fine_chans_per_coarse, rec_chan_no),
#         )

#         # create new file
#         with open(out_filename, "wb") as out_file:
#             out_file.write(
#                 struct.pack(
#                     AOCAL_HEADER_FORMAT,
#                     AOCAL_INTRO,
#                     AOCAL_FILE_TYPE,
#                     AOCAL_STRUCTURE_TYPE,
#                     interval_count,
#                     antenna_count,
#                     fine_chans_per_coarse,
#                     polarisation_count,
#                     start_gpstime,
#                     end_gpstime,
#                 )
#             )

#             # Write out just this coarse channel
#             subarray_le = np.asarray(np_data[:, :, c_idx, :, :, :], dtype="<f8")

#             bytes_written = out_file.write(subarray_le.tobytes(order="C"))

#             if bytes_written != bytes_per_coarse_chan:
#                 raise ValueError(
#                     f"aocal file {input_aocal_filename}: wrote wrong number of bytes {bytes_written} (should have been {bytes_per_coarse_chan}) to new aocal file {out_filename}"
#                 )

#             out_filenames.append(out_filename)

#     return out_filenames


# def get_aocal_filename(obsid: int, num_tiles: int, num_fine_chans: int, rec_chan_no: int) -> str:
#     """Generate the standard AOCAL filename.

#     Args:
#         obsid: Observation ID.
#         num_tiles: Number of tiles in the array.
#         num_fine_chans: Total number of fine channels.
#         rec_chan_no: Receiver channel number.

#     Returns:
#         The AOCAL filename string.
#     """
#     return f"{obsid}_{num_tiles:03}_{num_fine_chans:04}_{rec_chan_no:03}_calfile.bin"


# def get_partial_aocal_filename(obsid: int, rec_chan_no: int) -> str:
#     """Generate a partial AOCAL filename pattern for globbing.

#     Args:
#         obsid: Observation ID.
#         rec_chan_no: Receiver channel number.

#     Returns:
#         The AOCAL filename pattern string with wildcards.
#     """
#     return f"{obsid}_*_{rec_chan_no:03}_calfile.bin"


def get_solution_fits_filename(solutions_dir: str, obs_id: int, rec_chan: int) -> Optional[str]:
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


def parse_solution_channels(filename: str) -> Optional[tuple[int, int]]:
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
            "generated_at": datetime.datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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


def populate_index_json_entry(filename: str | Path, fit_id: int, plot_front_end_url: str) -> Optional[dict]:
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

    if ext not in (".png", ".tsv", ".txt"):
        return None

    stat = path.stat()
    last_modified = datetime.datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
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


def clip_hyperdrive_solution_gains(hyperdrive_fits_file: str, cut_off: float, mc: mwalib.MetafitsContext):
    """Clip hyperdrive calibration solution gains exceeding a threshold.

    Opens a hyperdrive FITS solution file, finds any Jones matrices where at
    least one complex gain amplitude exceeds the cut_off threshold, sets all
    four polarisations of those Jones matrices to NaN, and writes the result
    back to disk.

    Args:
        hyperdrive_fits_file: Path to the hyperdrive FITS solution file.
        cut_off: Threshold above which an entire Jones matrix is set to NaN.
        mc: MetafitsContext used to determine the tileid and name from the
            antenna indices in the solutions file.
    """

    HDU = "SOLUTIONS"
    # Polarisation names indexed by their position in the last axis of the
    # solutions array: 0=XX, 1=XY, 2=YX, 3=YY
    POL_NAMES = ["XX", "XY", "YX", "YY"]

    with fits.open(hyperdrive_fits_file, mode="update") as hdul:
        if HDU not in hdul:
            raise Exception(f"Warning: No SOLUTIONS HDU found in {hyperdrive_fits_file}")

        logger.info(f"checking solutions file {hyperdrive_fits_file} for gains > {cut_off}")

        # The SOLUTIONS HDU stores each complex gain as two consecutive float64
        # values (real, imag), so the raw FITS column layout is:
        #   shape: (time, antenna, chan, 8)  -- 8 floats = 4 pols × (re + im)
        #
        # Force native byte order (little-endian on x86): FITS files are
        # big-endian, and np.view() below requires a native-endian array to
        # safely reinterpret the memory as complex128.
        data = np.array(hdul[HDU].data, dtype=np.float64)
        # shape: (time, antenna, chan, 8)

        # Reinterpret each consecutive pair of float64 (re, im) as one
        # complex128 value. np.view() does not copy data; it aliases the same
        # memory with a different dtype, halving the size of the last axis.
        data_complex = data.view(np.complex128)
        # shape: (time, antenna, chan, 4)  -- last axis: [XX, XY, YX, YY]
        #
        # data_complex and data share the same underlying buffer, so writes to
        # data_complex are visible through data (and vice versa). This is what
        # allows us to flag via data_complex and then assign data back to the HDU.

        # Compute the amplitude (absolute value) of every complex gain sample.
        amp = np.abs(data_complex)
        # shape: (time, antenna, chan, 4)  -- last axis: [XX, XY, YX, YY]

        # Total counts used in logging below.
        total_samples = amp.size  # total (time, ant, chan, pol) samples
        total_jones = amp.shape[0] * amp.shape[1] * amp.shape[2]  # total (time, ant, chan) Jones matrices

        # Boolean mask: True wherever an individual gain amplitude exceeds the
        # threshold. Kept separate from the Jones-matrix flag below so we can
        # report how many individual pol samples actually triggered the cutoff.
        mask = amp > cut_off
        # shape: (time, antenna, chan, 4)  -- same layout as amp

        # Reduce across the polarisation axis: True for any (time, ant, chan)
        # where at least one of XX, XY, YX, YY exceeded the threshold.
        # If any one pol is bad the whole Jones matrix is considered unreliable,
        # so we flag all four pols together.
        any_flagged = mask.any(axis=-1)
        # shape: (time, antenna, chan)  -- pol axis removed; True = entire Jones matrix flagged

        # Set all four polarisations of every flagged Jones matrix to NaN + NaN*j.
        # Indexing data_complex with a (time, ant, chan) boolean array selects
        # rows of shape (4,) — one row per flagged Jones matrix — so the single
        # assignment sets all four pols at once.
        # Because data_complex is a view of data, this also updates data in-place;
        # no separate copy-back is needed for the flags.
        data_complex[any_flagged] = np.nan + 1j * np.nan

        # Count the pol samples that actually exceeded the cutoff (what triggered flagging).
        exceeded_count = int(mask.sum())
        # Count how many Jones matrices were flagged, and the resulting NaN pol samples
        # (always 4× the Jones matrix count since all four pols are set to NaN together).
        jones_flagged_count = int(any_flagged.sum())
        nan_pol_count = jones_flagged_count * 4

        logger.debug(
            f"Gains > {cut_off}:"
            f" {exceeded_count} / {total_samples} pol samples exceeded cutoff"
            f" ({100 * exceeded_count / total_samples:.2f}%);"
            f" {jones_flagged_count} / {total_jones} Jones matrices"
            f" ({nan_pol_count} / {total_samples} pol samples) set to NaN"
            f" ({100 * jones_flagged_count / total_jones:.2f}% of Jones matrices)"
        )

        if jones_flagged_count > 0:
            # np.argwhere(any_flagged) returns the indices of every True element.
            # Each row is one flagged Jones matrix: [time_idx, ant_idx, chan_idx]
            flagged_indices = np.argwhere(any_flagged)
            # shape: (jones_flagged_count, 3)  -- columns: [time, antenna, chan]

            # Collapse to unique (antenna, chan) pairs so we log one line per
            # affected tile+channel rather than one line per time step.
            # Slice columns 1:3 to get just [ant_idx, chan_idx].
            unique_ant_chan = np.unique(flagged_indices[:, 1:3], axis=0)
            # shape: (n_unique_ant_chan, 2)  -- columns: [antenna, chan]

            for ant_idx, chan_idx in unique_ant_chan:
                # Report the worst (max) amplitude seen across all time steps for
                # each of the four polarisations at this (antenna, chan) pair.
                # amp[:, ant_idx, chan_idx, p] selects all time steps for a fixed
                # (antenna, chan, pol) triple; shape: (n_timesteps,).
                # All four pols are shown regardless of which one(s) triggered the
                # cutoff, since the entire Jones matrix has been set to NaN.
                pol_details = ", ".join(
                    f"{POL_NAMES[p]}(Gain={amp[:, ant_idx, chan_idx, p].max():.4f})" for p in range(4)
                )

                logger.debug(
                    f"  antenna_idx={ant_idx}"
                    f" [TileId: {mc.antennas[ant_idx].tile_id}"
                    f" Name: {mc.antennas[ant_idx].tile_name}],"
                    f" chan_idx={chan_idx}: all pols set to NaN: {pol_details}"
                )

        # Write the modified float64 array (which contains the NaN-flagged
        # complex pairs) back to the HDU and flush to disk.
        # Note: we assign `data` (the float64 view) rather than `data_complex`
        # because that is the shape the HDU originally held.
        hdul[HDU].data = data
        hdul.flush()

        logger.info(f"finished rewriting solutions file {hyperdrive_fits_file}")
