"""Tests for calibration.solutions: raw hyperdrive HDU array readers.

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import numpy as np
import pytest

from mwax_mover.calibration.solutions import read_results_hdu, read_solutions_hdu_complex, read_tiles_hdu


class TestReadSolutionsHduComplex:
    """Tests for read_solutions_hdu_complex()."""

    def test_known_values_split_into_correct_complex_terms(self):
        """The 8 floats per entry split into [XX, XY, YX, YY] in order."""
        # One (timeblock, tile, chan) entry: gx=1+2j, Dx=3+4j, Dy=5+6j, gy=7+8j
        data = np.array([[[[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]]]])

        result = read_solutions_hdu_complex(data)

        assert result.shape == (1, 1, 1, 4)
        assert result[0, 0, 0, 0] == complex(1, 2)  # XX / gx
        assert result[0, 0, 0, 1] == complex(3, 4)  # XY / Dx
        assert result[0, 0, 0, 2] == complex(5, 6)  # YX / Dy
        assert result[0, 0, 0, 3] == complex(7, 8)  # YY / gy

    def test_preserves_leading_axes(self):
        """Only the trailing axis (8 -> 4) changes; leading axes are untouched."""
        rng = np.random.default_rng(42)
        data = rng.standard_normal((2, 3, 5, 8))

        result = read_solutions_hdu_complex(data)

        assert result.shape == (2, 3, 5, 4)

    def test_matches_manual_re_im_split(self):
        """Result matches manually pairing up (re, im) floats for every term."""
        rng = np.random.default_rng(7)
        data = rng.standard_normal((1, 4, 6, 8))

        result = read_solutions_hdu_complex(data)

        expected = np.stack(
            [data[..., 2 * i] + 1j * data[..., 2 * i + 1] for i in range(4)],
            axis=-1,
        )
        assert np.array_equal(result, expected)

    def test_nan_values_propagate(self):
        """NaN in either the real or imaginary float propagates to that complex term only."""
        data = np.ones((1, 1, 1, 8), dtype=np.float64)
        data[0, 0, 0, 2] = np.nan  # Dx real part

        result = read_solutions_hdu_complex(data)

        assert np.isnan(result[0, 0, 0, 1])  # XY / Dx is NaN
        assert not np.isnan(result[0, 0, 0, 0])  # XX / gx unaffected
        assert not np.isnan(result[0, 0, 0, 2])  # YX / Dy unaffected
        assert not np.isnan(result[0, 0, 0, 3])  # YY / gy unaffected


class TestReadResultsHdu:
    """Tests for read_results_hdu()."""

    def test_default_timeblock_is_zero(self):
        """With no timeblock argument, row 0 is returned."""
        data = np.array([[0.1, 0.2, 0.3], [9.0, 9.0, 9.0]])

        result = read_results_hdu(data)

        assert np.array_equal(result, [0.1, 0.2, 0.3])

    def test_explicit_timeblock_selects_that_row(self):
        """An explicit timeblock index selects that row, not row 0."""
        data = np.array([[0.1, 0.2, 0.3], [9.0, 9.0, 9.0]])

        result = read_results_hdu(data, timeblock=1)

        assert np.array_equal(result, [9.0, 9.0, 9.0])

    def test_single_timeblock_file_returns_1d_chanblock_array(self):
        """The common single-timeblock case: shape (1, n_chanblocks) -> (n_chanblocks,)."""
        data = np.array([[np.nan, 1e-5, 2e-5, np.nan]])

        result = read_results_hdu(data)

        assert result.shape == (4,)
        assert np.isnan(result[0])
        assert result[1] == pytest.approx(1e-5)

    def test_does_not_merge_multiple_timeblocks(self):
        """Regression check: must not concatenate/flatten across timeblocks.

        Prior to sharing this helper, HyperfitsSolution.results() used
        .flatten() on the RESULTS ImageHDU, which -- for a file with more
        than one timeblock -- would merge all timeblocks' chanblock values
        into one long 1-D array instead of selecting a single timeblock.
        """
        data = np.array([[1.0, 2.0], [3.0, 4.0]])

        result = read_results_hdu(data, timeblock=0)

        # A flatten()-based implementation would return length 4 here.
        assert result.shape == (2,)
        assert np.array_equal(result, [1.0, 2.0])


class TestReadTilesHdu:
    """Tests for read_tiles_hdu()."""

    @staticmethod
    def _tiles_recarray(antennas, tile_names, flags):
        """Build a minimal structured array matching the real TILES HDU's
        Antenna/TileName/Flag columns (see the fixture-verified dtype in
        mwax_calvin_quality._tile_names_from_tiles_hdu's docstring).
        """
        # astropy's FITS_rec auto-decodes 'A'-format (ASCII) columns to plain
        # str, not bytes -- verified against a real TILES HDU -- so the
        # synthetic array here uses a unicode dtype to match.
        dtype = [("Antenna", "i4"), ("Flag", "i2"), ("TileName", "U8")]
        arr = np.zeros(len(antennas), dtype=dtype)
        arr["Antenna"] = antennas
        arr["Flag"] = flags
        arr["TileName"] = tile_names
        return arr

    def test_already_ascending_antennas_unchanged(self):
        """When Antenna is already ascending, order is preserved."""
        tiles_data = self._tiles_recarray(antennas=[0, 1, 2], tile_names=["Tile0", "Tile1", "Tile2"], flags=[0, 1, 0])

        antennas, names, flags = read_tiles_hdu(tiles_data)

        assert list(antennas) == [0, 1, 2]
        assert names == ["Tile0", "Tile1", "Tile2"]
        assert list(flags) == [False, True, False]

    def test_out_of_order_antennas_are_sorted(self):
        """When Antenna is out of order, names/flags are reordered to match."""
        tiles_data = self._tiles_recarray(antennas=[2, 0, 1], tile_names=["TileC", "TileA", "TileB"], flags=[1, 0, 1])

        antennas, names, flags = read_tiles_hdu(tiles_data)

        assert list(antennas) == [0, 1, 2]
        # TileA (antenna 0), TileB (antenna 1), TileC (antenna 2)
        assert names == ["TileA", "TileB", "TileC"]
        assert list(flags) == [False, True, True]

    def test_flags_returned_as_bool_array(self):
        """Flags are cast to a proper bool array regardless of the HDU's int dtype."""
        tiles_data = self._tiles_recarray(antennas=[0, 1], tile_names=["T0", "T1"], flags=[0, 1])

        _antennas, _names, flags = read_tiles_hdu(tiles_data)

        assert flags.dtype == np.bool_
