"""Tests for calvin.solution_files: solution-filename parsing/lookup.

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import pytest

from mwax_mover.calvin.solution_files import (
    get_solution_fits_filename,
    get_sorted_solution_files,
    parse_solution_channels,
)


# ===========================================================================
# From test014: split_aocal_file_into_coarse_channels
# ===========================================================================
class TestParseSolutionChannels:
    """Tests for parse_solution_channels."""

    # Flavour 1: all channels
    @pytest.mark.parametrize(
        "filename",
        [
            "1234567890_solutions.fits",
            "1234567890_solutions.bin",
        ],
    )
    def test_all_channels(self, filename: str) -> None:
        assert parse_solution_channels(filename) is None

    # Flavour 2: single channel
    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("1234567890_ch1_solutions.fits", (1, 1)),
            ("1234567890_ch57_solutions.fits", (57, 57)),
            ("1234567890_ch121_solutions.fits", (121, 121)),
            ("1234567890_ch1_solutions.bin", (1, 1)),
            ("1234567890_ch57_solutions.bin", (57, 57)),
            ("1234567890_ch121_solutions.bin", (121, 121)),
        ],
    )
    def test_single_channel(self, filename: str, expected: tuple[int, int]) -> None:
        assert parse_solution_channels(filename) == expected

    # Flavour 3: channel range
    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("1234567890_ch1-12_solutions.fits", (1, 12)),
            ("1234567890_ch57-68_solutions.fits", (57, 68)),
            ("1234567890_ch109-120_solutions.fits", (109, 120)),
            ("1234567890_ch1-12_solutions.bin", (1, 12)),
            ("1234567890_ch57-68_solutions.bin", (57, 68)),
            ("1234567890_ch109-120_solutions.bin", (109, 120)),
        ],
    )
    def test_channel_range(self, filename: str, expected: tuple[int, int]) -> None:
        assert parse_solution_channels(filename) == expected

    # Unrecognised filenames
    @pytest.mark.parametrize(
        "filename",
        [
            "not_a_solution_file.fits",
            "1234567890_ch1_solutions.txt",
            "1234567890_ch1_solutions",
            "1234567890_chABC_solutions.fits",
            "",
        ],
    )
    def test_invalid_filename_raises(self, filename: str) -> None:
        with pytest.raises(ValueError, match="could not be determined"):
            parse_solution_channels(filename)


class TestGetSortedSolutionFiles:
    """Tests for get_sorted_solution_files."""

    OBS_ID = 1234567890

    def _touch(self, path) -> None:
        """Create an empty file."""
        path.touch()

    def test_sorts_by_channel_number(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_ch100-112_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_ch57_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.fits")

        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID)

        assert result == [
            str(tmp_path / f"{self.OBS_ID}_solutions.fits"),
            str(tmp_path / f"{self.OBS_ID}_ch57_solutions.fits"),
            str(tmp_path / f"{self.OBS_ID}_ch100-112_solutions.fits"),
        ]

    def test_bin_extension(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_ch100-112_solutions.bin")
        self._touch(tmp_path / f"{self.OBS_ID}_ch57_solutions.bin")
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.bin")

        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID, extension="bin")

        assert result == [
            str(tmp_path / f"{self.OBS_ID}_solutions.bin"),
            str(tmp_path / f"{self.OBS_ID}_ch57_solutions.bin"),
            str(tmp_path / f"{self.OBS_ID}_ch100-112_solutions.bin"),
        ]

    def test_default_extension_ignores_bin(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.bin")

        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID)

        assert result == [str(tmp_path / f"{self.OBS_ID}_solutions.fits")]

    def test_bin_extension_ignores_fits(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.bin")

        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID, extension="bin")

        assert result == [str(tmp_path / f"{self.OBS_ID}_solutions.bin")]

    def test_unknown_extension_ignores_fits(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_solutions.bin")

        with pytest.RaisesExc(ValueError):
            _result = get_sorted_solution_files(str(tmp_path), self.OBS_ID, extension=".fits")

    def test_empty_directory(self, tmp_path) -> None:
        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID)
        assert result == []

    def test_no_matching_files(self, tmp_path) -> None:
        self._touch(tmp_path / "unrelated_file.fits")
        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID)
        assert result == []

    def test_single_file(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_ch57_solutions.fits")

        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID)

        assert result == [str(tmp_path / f"{self.OBS_ID}_ch57_solutions.fits")]

    def test_multiple_ranges_sorted(self, tmp_path) -> None:
        self._touch(tmp_path / f"{self.OBS_ID}_ch109-120_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_ch1-12_solutions.fits")
        self._touch(tmp_path / f"{self.OBS_ID}_ch57-68_solutions.fits")

        result = get_sorted_solution_files(str(tmp_path), self.OBS_ID)

        assert result == [
            str(tmp_path / f"{self.OBS_ID}_ch1-12_solutions.fits"),
            str(tmp_path / f"{self.OBS_ID}_ch57-68_solutions.fits"),
            str(tmp_path / f"{self.OBS_ID}_ch109-120_solutions.fits"),
        ]


def test_get_solution_fits_filename_flavour1_all_channels(tmp_path):
    """Flavour 1: obsid_solutions.fits matches any rec_chan"""
    fits = tmp_path / "1234567890_solutions.fits"
    fits.touch()
    for chan in [1, 12, 24]:
        result = get_solution_fits_filename(str(tmp_path), 1234567890, chan)
        assert result == str(fits)


def test_get_solution_fits_filename_flavour2_single_channel(tmp_path):
    """Flavour 2: obsid_chN_solutions.fits matches exact channel"""
    fits = tmp_path / "1234567890_ch5_solutions.fits"
    fits.touch()
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 5)
    assert result == str(fits)


def test_get_solution_fits_filename_flavour2_single_channel_no_match(tmp_path):
    """Flavour 2: obsid_chN_solutions.fits does not match a different channel"""
    fits = tmp_path / "1234567890_ch5_solutions.fits"
    fits.touch()
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 6)
    assert result is None


def test_get_solution_fits_filename_flavour2_no_zero_padding(tmp_path):
    """Flavour 2: channel numbers are not zero-padded"""
    fits = tmp_path / "1234567890_ch007_solutions.fits"
    fits.touch()
    # ch007 should NOT match rec_chan=7 (not zero-padded)
    # with pytest.raises(ValueError, match=f"The channels for {os.path.basename(fits)} could not be determined"):
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 7)
    assert result is not None


def test_get_solution_fits_filename_flavour3_range_channel_within(tmp_path):
    """Flavour 3: rec_chan within range [N, M] matches"""
    fits = tmp_path / "1234567890_ch1-12_solutions.fits"
    fits.touch()
    for chan in [1, 6, 12]:
        result = get_solution_fits_filename(str(tmp_path), 1234567890, chan)
        assert result == str(fits)


def test_get_solution_fits_filename_flavour3_range_channel_outside(tmp_path):
    """Flavour 3: rec_chan outside range [N, M] does not match"""
    fits = tmp_path / "1234567890_ch1-12_solutions.fits"
    fits.touch()
    for chan in [0, 13, 24]:
        result = get_solution_fits_filename(str(tmp_path), 1234567890, chan)
        assert result is None


def test_get_solution_fits_filename_flavour3_range_boundary_values(tmp_path):
    """Flavour 3: rec_chan at exact boundaries matches"""
    fits = tmp_path / "1234567890_ch5-10_solutions.fits"
    fits.touch()
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 5) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 10) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 4) is None
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 11) is None


def test_get_solution_fits_filename_flavour3_multi_digit_channels(tmp_path):
    """Flavour 3: multi-digit channel numbers (e.g. ch13-24) work correctly"""
    fits = tmp_path / "1234567890_ch13-24_solutions.fits"
    fits.touch()
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 13) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 24) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 12) is None


def test_get_solution_fits_filename_flavour1_takes_priority_over_flavour2(tmp_path):
    """Flavour 1 is returned first if both flavour 1 and 2 exist"""
    f1 = tmp_path / "1234567890_solutions.fits"
    f1.touch()
    f2 = tmp_path / "1234567890_ch5_solutions.fits"
    f2.touch()
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 5)
    assert result == str(f1)


def test_get_solution_fits_filename_flavour1_takes_priority_over_flavour3(tmp_path):
    """Flavour 1 is returned first if both flavour 1 and 3 exist"""
    f1 = tmp_path / "1234567890_solutions.fits"
    f1.touch()
    f3 = tmp_path / "1234567890_ch1-12_solutions.fits"
    f3.touch()
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 6)
    assert result == str(f1)


def test_get_solution_fits_filename_no_files_returns_none(tmp_path):
    """Empty directory returns None"""
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 5)
    assert result is None


def test_get_solution_fits_filename_wrong_obsid_ignored(tmp_path):
    """Files for a different obs_id are not matched"""
    fits = tmp_path / "9999999999_solutions.fits"
    fits.touch()
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 1)
    assert result is None


def test_get_solution_fits_filename_directory_does_not_exist():
    """Non-existent directory returns None without raising"""
    result = get_solution_fits_filename("/nonexistent/path", 1234567890, 5)
    assert result is None
