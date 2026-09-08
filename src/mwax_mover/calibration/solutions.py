"""Raw hyperdrive solution-file HDU array readers.

Each function converts one HDU's raw FITS array data (SOLUTIONS, RESULTS,
TILES, or BASELINES) into a more usable form. These are hyperdrive
solution-format-specific, not generic FITS utilities -- see
calvin.hyperdrive for the higher-level reader built on these.
"""

import numpy as np
from numpy.typing import NDArray


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
