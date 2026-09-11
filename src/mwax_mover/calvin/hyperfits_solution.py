"""A single hyperdrive FITS calibration solution file.

Provides HyperfitsSolution, the single source of truth for reading and
writing one hyperdrive solutions FITS file. See calvin.hyperfits_solution_group
for HyperfitsSolutionGroup (a set of these, one per contiguous coarse-channel
band, combined with the observation's metafits) and calvin.hyperdrive for
running hyperdrive itself. See calibration/ for the shared data structures
and pure numeric fitting/outlier functions these use, and calvin.plots for
plotting. Split out of calvin.hyperdrive -- see docs/HYPERDRIVE_PARALLELISM.md
Phase 1.
"""

import os
import shutil
import logging

import numpy as np
from astropy.io import fits
from numpy.typing import NDArray

from mwax_mover.calibration.fitting import ensure_system_byte_order
from mwax_mover.calibration.solutions import (
    read_baseline_tile_flags,
    read_results_hdu,
    read_solutions_hdu_complex,
    read_tiles_hdu,
)
from mwax_mover.constants import EXT_FITS

logger = logging.getLogger(__name__)


class HyperfitsSolution:
    """A single calibration solution in hyperdrive FITS format"""

    def __init__(self, filename) -> None:
        """Initialize a HyperfitsSolution file reader.

        Args:
            filename: Path to the hyperdrive FITS solution file.
        """
        self.filename = filename

        # Cache for the RESULTS HDU (see the `results` property). Two-state so a
        # missing HDU is remembered too: _results_cached distinguishes "not read
        # yet" from "read, and there is no RESULTS HDU", which must keep raising
        # KeyError for callers that fall back to uniform weights.
        self._results_cached: bool = False
        self._results: NDArray[np.float64] | None = None

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

    @property
    def tile_flags(self) -> NDArray[np.bool_]:
        """Get tile flags ordered by antenna index."""
        with fits.open(self.filename) as hdus:
            _antennas, _tile_names, flags = read_tiles_hdu(hdus["TILES"].data)
            return flags

    def get_average_times(self) -> list[float]:
        """Get the average time for each timeblock.

        Raises:
            KeyError: If TIMEBLOCKS HDU is not present.
        """
        with fits.open(self.filename) as hdus:
            time_data = hdus["TIMEBLOCKS"].data
            return [time["Average"] for time in time_data]

    def get_solutions(self) -> list[NDArray[np.complex128]]:
        """Get solutions as complex arrays.

        Returns:
            A list of four complex arrays (XX, XY, YX, YY) each with shape [time, tile, chan].
        """
        with fits.open(self.filename) as hdus:
            complex_solutions = read_solutions_hdu_complex(hdus["SOLUTIONS"].data)
            return [complex_solutions[..., i] for i in range(4)]

    def get_ref_solutions(self, ref_tile_idx=None) -> list[NDArray[np.complex128]]:
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

        Read from disk once and cached thereafter. The RESULTS HDU is immutable
        for the life of this object (nothing here writes it -- write_jones only
        touches SOLUTIONS), and it is read repeatedly: every access to
        HyperfitsSolutionGroup.results touches it twice per file (once in the
        length-validation loop, once in the concatenate), and .weights goes
        through that on each of its several accesses per pipeline run. Uncached,
        a 24-file picket-fence observation opened solution files 192 times per
        run, against 8 for a contiguous one, all over a shared filesystem.

        Returns:
            1-D float64 array of per-channel convergence values, for
            timeblock 0.

        Raises:
            KeyError: If the RESULTS HDU is not present. This is expected for
                older hyperdrive solution files. Callers that can tolerate missing
                results should catch KeyError and fall back to uniform weights.
                A missing HDU is cached as such, so this keeps raising without
                re-reading the file.
        """
        if not self._results_cached:
            try:
                with fits.open(self.filename) as hdus:
                    self._results = read_results_hdu(hdus["RESULTS"].data)
            except KeyError:
                self._results = None
                self._results_cached = True
                raise
            self._results_cached = True

        if self._results is None:
            raise KeyError(f"no RESULTS HDU in {self.filename}")

        return self._results

    @property
    def chanblock_converged(self) -> NDArray[np.bool_]:
        """Get per-chanblock convergence, for timeblock 0.

        Returns:
            1-D bool array, shape (n_chanblocks,). True where that
            chanblock's RESULTS precision was non-NaN (i.e. the joint
            solve across all tiles converged for it).

        Raises:
            KeyError: If the RESULTS HDU is not present (see `results`).
        """
        return ~np.isnan(self.results)

    @property
    def baseline_tile_flags(self) -> NDArray[np.bool_]:
        """Get per-tile flagging inferred from the BASELINES HDU.

        A third, independent source of tile flagging alongside `tile_flags`
        (TILES HDU) and the metafits flag column -- see
        read_baseline_tile_flags for why this is kept separate rather than
        assumed to always agree with `tile_flags`.

        Returns:
            Boolean array, shape (n_tiles,). True where the tile is flagged.
            All-False if the BASELINES HDU is absent (older or synthetic
            solution files) -- this source simply has no information to
            contribute in that case, rather than the file being unusable.
        """
        with fits.open(self.filename) as hdus:
            n_tiles = len(self.tile_flags)
            if "BASELINES" not in hdus:
                logger.debug(f"{self.filename} - no BASELINES HDU; baseline_tile_flags defaulting to all-False.")
                return np.zeros(n_tiles, dtype=bool)
            baseline_weights = hdus["BASELINES"].data.astype(np.float64)
            return read_baseline_tile_flags(baseline_weights, n_tiles)

    def get_jones(self) -> NDArray[np.complex128]:
        """Get solutions as a complex 2x2 Jones matrix array, for timeblock 0.

        Assumes (and asserts) a single timeblock -- unlike get_solutions(),
        which keeps the timeblock axis. This is the shape every flagging
        method in this module works with, matching what
        mwax_calvin_quality.CalSolutionQuality.gains used historically.

        Returns:
            Complex128 array, shape (n_tiles, n_chanblocks, 2, 2). Indices
            [..., 0, 0] = gx, [..., 0, 1] = Dx, [..., 1, 0] = Dy,
            [..., 1, 1] = gy, matching hyperdrive's SOLUTIONS HDU layout.

        Raises:
            RuntimeError: If the file contains more than one timeblock.
        """
        xx, xy, yx, yy = self.get_solutions()  # each shape (timeblock, tile, chan)

        if xx.shape[0] != 1:
            raise RuntimeError(f"{self.filename} - exactly 1 timeblock must be provided: ({xx.shape[0]})")

        gx, dx, dy, gy = xx[0], xy[0], yx[0], yy[0]  # each shape (tile, chan)
        return np.stack(
            [np.stack([gx, dx], axis=-1), np.stack([dy, gy], axis=-1)],
            axis=-2,
        )  # shape: (tile, chanblock, 2, 2)

    def write_jones(self, jones: NDArray[np.complex128], backup: bool = True) -> str | None:
        """Overwrite this file's SOLUTIONS HDU with the given Jones matrices.

        Args:
            jones: Complex128 array, shape (n_tiles, n_chanblocks, 2, 2),
                same layout as get_jones() returns. Written back as
                timeblock 0; this tool does not support multiple
                timeblocks.
            backup: If True (default), copy the original file to
                "{filename}.original.fits" first (overwriting any existing
                backup at that path, with a warning). Set False only when
                a caller has already made its own backup earlier in the
                same pipeline run.

        Returns:
            The backup file path, or None if backup=False.

        Raises:
            RuntimeError: If the file has more than one timeblock, or if
                jones's shape doesn't match the file's (n_tiles,
                n_chanblocks). Checked before any backup or write happens.
        """
        with fits.open(self.filename) as hdul_check:
            n_timeblocks, n_tiles, n_chanblocks, _ = hdul_check["SOLUTIONS"].data.shape
        if n_timeblocks != 1:
            raise RuntimeError(f"{self.filename} - exactly 1 timeblock must be provided: ({n_timeblocks})")
        if jones.shape != (n_tiles, n_chanblocks, 2, 2):
            raise RuntimeError(
                f"{self.filename} - jones shape {jones.shape}"
                f" does not match SOLUTIONS HDU shape (tile={n_tiles}, chanblock={n_chanblocks}, 2, 2)"
            )

        backup_path: str | None = None
        if backup:
            backup_path = self.filename.replace(EXT_FITS, ".original" + EXT_FITS)
            if os.path.exists(backup_path):
                logger.warning(f"Warning: backup {backup_path} already exists and will be overwritten.")
            shutil.copy2(self.filename, backup_path)
            logger.debug(f"Backed up original file to {backup_path}")

        with fits.open(self.filename, mode="update") as hdul:
            # Force native byte order, matching the historical
            # _write_bad_gains_as_nan behaviour: FITS files are big-endian
            # on disk, and we need a native-endian array to safely write
            # into the 8-float-per-entry layout without corrupting
            # adjacent bytes.
            data = np.array(hdul["SOLUTIONS"].data, dtype=np.float64)  # shape: (timeblock, tile, chanblock, 8)

            gx, dx, dy, gy = jones[..., 0, 0], jones[..., 0, 1], jones[..., 1, 0], jones[..., 1, 1]
            # Interleave (real, imag) pairs for each of the 4 terms, in
            # SOLUTIONS HDU order [XX, XY, YX, YY] == [gx, Dx, Dy, gy].
            for i, term in enumerate((gx, dx, dy, gy)):
                data[0, :, :, 2 * i] = term.real
                data[0, :, :, 2 * i + 1] = term.imag

            hdul["SOLUTIONS"].data = data
            hdul.flush()

        logger.info(f"Wrote Jones matrices to {self.filename}")

        return backup_path
