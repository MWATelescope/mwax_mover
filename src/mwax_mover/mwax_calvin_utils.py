"""Utility Functions to support mwax_calvin processor"""

import datetime
import glob
import os
import shutil
import time
import traceback
import numpy as np
from astropy.io import fits
from astropy import units as u
from astropy.constants import c
from scipy.optimize import minimize
import pandas as pd
from pandas import DataFrame
import seaborn as sns
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from mwax_mover.mwax_command import (
    run_command_ext,
    run_command_popen,
    check_popen_finished,
)

import numpy.typing as npt  # noqa: F401
from numpy.typing import ArrayLike, NDArray
from typing import NamedTuple, List, Tuple, Dict, Optional  # noqa: F401

# from nptyping import NDArray, Shape
import sys


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


# class Config(Enum):
#     """Array configuration"""
#     COMPACT = 0
#     EXTENDED = 1

#     @staticmethod
#     def from_tiles(tiles: List[Tile]) -> 'Config':
#         """Determine array config from tiles"""
#         nhex = len([tile for tile in tiles if tile.tile_name.startswith("Hex")])
#         nlb = len([tile for tile in tiles if tile.tile_name.startswith("LB")])
#         if nhex > nlb:
#             return Config.COMPACT
#         elif nlb > nhex:
#             return Config.EXTENDED
#         else:
#             raise ValueError(f"Unknown array config with {nlb=} and {nhex=}")


def ensure_system_byte_order(arr):
    system_byte_order = ">" if sys.byteorder == "big" else "<"
    if arr.dtype.byteorder not in f"{system_byte_order}|=":
        return arr.newbyteorder(system_byte_order)
    return arr


def parse_csv_header(value: str, dtype: type) -> ArrayLike:
    """
    parse comma separated values (in metafits header)
    """
    return np.array(value.split(","), dtype=dtype)


class Metafits:
    """MWA Metadata file in FITS format"""

    def __init__(self, filename: str):
        self.filename = filename

    @property
    def tiles(self) -> List[Tile]:
        """Get tile info from metafits, sorted by index"""

        with fits.open(self.filename) as hdus:
            metafits_inputs = hdus["TILEDATA"].data  # type: ignore

        # using a set here to avoid duplicates (pol=X,Y)
        tiles = set(
            Tile(
                name=metafits_input["TileName"],
                id=metafits_input["Tile"],
                flag=metafits_input["Flag"],
                # index=metafits_input["Antenna"],
                rx=metafits_input["Rx"],
                slot=metafits_input["Slot"],
                flavor=metafits_input["Receiver_Types"],
            )
            for metafits_input in metafits_inputs
        )

        return sorted([*tiles], key=lambda tile: tile.id)

    @property
    def inputs(self) -> List[Input]:
        """Get tile info from metafits, sorted by index"""

        with fits.open(self.filename) as hdus:
            metafits_inputs = hdus["TILEDATA"].data  # type: ignore

        inputs = set(
            Input(
                id=metafits_input["Input"],
                name=metafits_input["TileName"] + metafits_input["Pol"],
                flag=metafits_input["Flag"],
                pol=metafits_input["Pol"],
                # index=metafits_input["Antenna"],
                rx=metafits_input["Rx"],
                slot=metafits_input["Slot"],
                # flavor=metafits_input["Flavors"],
                flavor=metafits_input["Receiver_Types"],
                length=float(metafits_input["Length"][3:]),
            )
            for metafits_input in metafits_inputs
        )

        return sorted([*inputs], key=lambda inp: inp.id)

    @property
    def tiles_df(self) -> pd.DataFrame:
        """Get reference antenna (unflagged tile with lowest id) and tiles as df"""
        # determine array configuration (compact or extended)
        # config = Config.from_tiles(tiles)
        tiles = self.tiles
        return pd.DataFrame(tiles, columns=Tile._fields)
        # unflagged = tiles[tiles.flag == 0]
        # if not len(unflagged):
        #     raise ValueError("No unflagged tiles found")
        # # tiles_by_id = sorted(tiles, key=lambda tile: tile.id)
        # return unflagged.sort_values(by=["id"]).take([0])["name"], tiles

    @property
    def inputs_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.inputs, columns=Input._fields)

    @property
    def chan_info(self) -> ChanInfo:
        """
        Get coarse channels from metafits, sorted

        Assumptions:
        - fine_chan_width is a multiple of 200Hz (or 10kHz), so is an integer
        - total_bandwidth is a multiple of fine_chan_width, so is an integer
        """
        with fits.open(self.filename) as hdus:
            hdu = hdus["PRIMARY"]
            header: fits.header.Header = hdu.header  # type: ignore

            # coarse channels
            coarse_chans = parse_csv_header(header["CHANNELS"], int)
            # coarse channel selection
            chansel = parse_csv_header(header["CHANSEL"], int)
            if len(chansel) != len(coarse_chans):
                raise RuntimeError(f"channel selection is not tested. {chansel=}")
            # coarse_chans = np.sort(coarse_chans[chansel])
            coarse_chans = np.sort(coarse_chans)

            # fine channel width
            fine_chan_width_hz = int(float(header["FINECHAN"]) * 1000)  # type: ignore
            # total observation bandwidth
            total_bandwidth_hz = int(float(header["BANDWDTH"]) * 1000000)  # type: ignore
            # number of fine channels in observation
            obs_num_fine_chans = int(header["NCHANS"])  # type: ignore

        # sanity checks
        if total_bandwidth_hz != fine_chan_width_hz * obs_num_fine_chans:
            raise ValueError(
                f"{self.filename} - ({total_bandwidth_hz=})" f" != ({fine_chan_width_hz=}) * ({obs_num_fine_chans=})"
            )

        if obs_num_fine_chans % len(coarse_chans) != 0:
            raise ValueError(
                f"Number of fine channels ({obs_num_fine_chans}) "
                f"not a multiple of the number of coarse channels ({len(coarse_chans)})"
            )

        # calculate number of fine channels per coarse channel
        fine_chans_per_coarse = obs_num_fine_chans // len(coarse_chans)

        coarse_chan_ranges = []
        for _, g in enumerate(np.split(coarse_chans, np.where(np.diff(coarse_chans) != 1)[0] + 1)):
            coarse_chan_ranges.append(g)

        return ChanInfo(
            coarse_chan_ranges=coarse_chan_ranges,
            fine_chan_width_hz=fine_chan_width_hz,
            fine_chans_per_coarse=fine_chans_per_coarse,
        )

    @property
    def time_info(self) -> TimeInfo:
        """
        Get time info from metafits
        """
        with fits.open(self.filename) as hdus:
            hdu = hdus["PRIMARY"]
            header = hdu.header

            inttime = header["INTTIME"]
            nscans = header["NSCANS"]

        return TimeInfo(
            num_times=nscans,
            int_time_s=inttime,
        )

    @property
    def calibrator(self):
        with fits.open(self.filename) as hdus:
            hdu = hdus["PRIMARY"]
            header = hdu.header  # type: ignore
            if header.get("CALIBSRC"):
                return header["CALIBSRC"]

    @property
    def obsid(self):
        with fits.open(self.filename) as hdus:
            hdu = hdus["PRIMARY"]
            header = hdu.header  # type: ignore
            if header.get("GPSTIME"):
                return header["GPSTIME"]


class HyperfitsSolution:
    """A single calibration solution in hyperdrive FITS format"""

    def __init__(self, filename) -> None:
        self.filename = filename

    @property
    def chanblocks_hz(self) -> NDArray[np.int_]:
        """
        Get channels from solution file

        Validate:
        - channels are contiguous
        - channels are in ascending order

        Assumptions:
        - channel frequencies are multiples of 200Hz, so are integer Hz values

        """
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
    #         tile_data = hdus['TILES'].data  # type: ignore
    #     return [
    #         (tile["TileName"], tile["Flag"])
    #         for tile in tile_data
    #     ]

    @property
    def tile_flags(self) -> List[bool]:
        """Get the tile flags ordered by Antenna index"""
        with fits.open(self.filename) as hdus:
            tile_data = hdus["TILES"].data  # type: ignore
        return tile_data["Flag"]

    def get_average_times(self) -> List[float]:
        """Get the average time for each timeblock

        Raises KeyError if TIMEBLOCKS not present
        """
        with fits.open(self.filename) as hdus:
            time_data = hdus["TIMEBLOCKS"].data  # type: ignore
            return [time["Average"] for time in time_data]

    def get_solutions(self) -> List[NDArray[np.complex128]]:
        """Get solutions as a complex array for each pol: [time, tile, chan]"""
        with fits.open(self.filename) as hdus:
            solutions = hdus["SOLUTIONS"].data  # type: ignore
        return [
            solutions[:, :, :, 0] + 1j * solutions[:, :, :, 1],
            solutions[:, :, :, 2] + 1j * solutions[:, :, :, 3],
            solutions[:, :, :, 4] + 1j * solutions[:, :, :, 5],
            solutions[:, :, :, 6] + 1j * solutions[:, :, :, 7],
        ]

    def get_ref_solutions(self, ref_tile_idx=None) -> List[NDArray[np.complex128]]:
        """Get solutions divided by reference tile as a complex array for each pol: [time, tile, chan]"""
        solutions = self.get_solutions()
        if ref_tile_idx is None:
            return solutions
        # divide solutions by reference
        ref_solutions = [solution[:, ref_tile_idx, :] for solution in solutions]  # type: ignore
        # divide solutions jones matrix by reference jones matrix, via inverse determinant
        ref_inv_det = np.divide(
            1 + 0j, ref_solutions[0] * ref_solutions[3] - ref_solutions[1] * ref_solutions[2]  # type: ignore
        )
        return [  # type: ignore
            (solutions[0] * ref_solutions[3] - solutions[1] * ref_solutions[2]) * ref_inv_det,
            (solutions[1] * ref_solutions[0] - solutions[0] * ref_solutions[1]) * ref_inv_det,
            (solutions[2] * ref_solutions[3] - solutions[3] * ref_solutions[2]) * ref_inv_det,
            (solutions[3] * ref_solutions[0] - solutions[2] * ref_solutions[1]) * ref_inv_det,
        ]

    @property
    def results(self) -> NDArray[np.float64]:
        """Code adapted from Chris Jordan's scripts"""
        with fits.open(self.filename) as hdus:
            return hdus["RESULTS"].data.flatten()  # type: ignore

        # Not sure why I need flatten!


class HyperfitsSolutionGroup:
    """
    A group of Hyperdrive .fits calibration solutions and corresponding metafits files
    """

    def __init__(self, metafits: List[Metafits], solns: List[HyperfitsSolution]):
        if not len(metafits):
            raise RuntimeError("no metafits files provided")
        self.metafits = metafits
        if not len(solns):
            raise RuntimeError("no solutions files provided")
        self.solns = solns
        self.metafits_tiles_df = HyperfitsSolutionGroup.get_metafits_tiles_df(self.metafits)
        self.metafits_chan_info = HyperfitsSolutionGroup.get_metafits_chan_info(self.metafits)
        self.chanblocks_per_coarse, self.all_chanblocks_hz = HyperfitsSolutionGroup.get_soln_chan_info(
            self.metafits_chan_info, self.solns
        )

    @classmethod
    def get_metafits_chan_info(cls, metafits: List[Metafits]) -> ChanInfo:
        """
        Get the combined ChanInfo, chanblocks_per_coarse and chanblocks_hz array for all provided metafits

        Validate that chan ranges do not overlap, and that channel info is consistent
        """
        first_chan_info = metafits[0].chan_info
        all_ranges = [*first_chan_info.coarse_chan_ranges]

        for metafits_ in metafits[1:]:
            chan_info = metafits_.chan_info
            if chan_info.fine_chans_per_coarse != first_chan_info.fine_chans_per_coarse:
                raise RuntimeError(
                    f"fine channels per coarse mismatch between metafits files. "
                    f"{metafits[0].filename} ({first_chan_info.fine_chans_per_coarse}) != "
                    f"{metafits_.filename} ({chan_info.fine_chans_per_coarse})"
                )
            if chan_info.fine_chan_width_hz != first_chan_info.fine_chan_width_hz:
                raise RuntimeError(
                    f"fine channel width mismatch between metafits files. "
                    f"{metafits[0].filename} ({first_chan_info.fine_chan_width_hz}) != "
                    f"{metafits_.filename} ({chan_info.fine_chan_width_hz})"
                )
            all_ranges.extend(chan_info.coarse_chan_ranges)

        all_ranges = sorted(all_ranges, key=lambda x: x[0])

        # assert coarse channel ranges do not overlap
        for left, right in zip(all_ranges[:-1], all_ranges[1:]):
            if left[0] == right[0] or left[-1] >= right[0]:
                raise RuntimeError("coarse channel ranges from metafits overlap. " f"{[left, right]}, {metafits=}")
        return ChanInfo(
            coarse_chan_ranges=all_ranges,
            fine_chan_width_hz=first_chan_info.fine_chan_width_hz,
            fine_chans_per_coarse=first_chan_info.fine_chans_per_coarse,
        )

    @classmethod
    def get_soln_chan_info(
        cls, metafits_chan_info: ChanInfo, solns: List[HyperfitsSolution]
    ) -> Tuple[int, List[NDArray[np.int_]]]:
        """
        Get the chanblocks_per_coarse and chanblocks_hz array for provided solutions

        Validate that channel info from metafits is consistent with solutions:
            - should all have the same chanblocks_per_coarse
        """

        chanblocks_per_coarse = None
        all_chanblocks_hz = []
        metafits_coarse_chans = np.concatenate(metafits_chan_info.coarse_chan_ranges)
        metafits_fine_chan_width_hz = metafits_chan_info.fine_chan_width_hz
        metafits_fine_chans_per_coarse = metafits_chan_info.fine_chans_per_coarse
        metafits_coarse_bandwidth_hz = metafits_fine_chan_width_hz * metafits_fine_chans_per_coarse

        for soln in solns:
            # coarse_chans = chaninfo.coarse_chan_ranges[coarse_chan_range_idx]
            chanblocks_hz = soln.chanblocks_hz
            if len(chanblocks_hz) < 2:
                raise RuntimeError(f"{soln.filename} - not enough chanblocks found ({chanblocks_hz=})")

            chanblock_width_hz = chanblocks_hz[1] - chanblocks_hz[0]  # type: ignore
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
                            f"{soln.filename} - solution {coarse_bandwidth_hz=}" f" > {metafits_coarse_bandwidth_hz=}"
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
                print(
                    f"{soln.filename} - warning: number of coarse channels in solution file ({soln_ncoarse=})"
                    f" does not match number of coarse channels in metafits for this range ({range_ncoarse=})"
                    f" given {chanblocks_per_coarse=}, {chans_per_block=}"
                )

            all_chanblocks_hz.append(chanblocks_hz)

        if all_chanblocks_hz is None:
            raise RuntimeError("No valid channels found")
        return (chanblocks_per_coarse, all_chanblocks_hz)

    @classmethod
    def get_metafits_tiles_df(cls, metafits) -> pd.DataFrame:
        """
        Get tiles dataframe, assert that all metafits have the same tiles
        """
        columns = list(set(Tile._fields) - set(["flag"]))
        tiles_df = metafits[0].tiles_df
        for metafits_ in metafits[1:]:
            for column in columns:
                if not tiles_df[column].equals(metafits_.tiles_df[column]):
                    raise RuntimeError(
                        f"tiles dataframes from metafits files do not match on {column=}. "
                        f"{metafits[0].filename} != {metafits_.filename}\n"
                        f"{tiles_df[column].tolist()}\n\n"
                        f"{metafits_.tiles_df[column].tolist()}\n"
                    )

        return tiles_df

    @property
    def refant(self) -> pd.Series:
        """
        Get reference antenna (unflagged tile with lowest id) which is not flagged in solutions
        """
        tiles_df = self.metafits_tiles_df.copy()
        # flag tiles_df with solution flags
        for soln in self.solns:
            tiles_df["flag_metafits"] = tiles_df["flag"]
            tiles_df["flag_soln"] = soln.tile_flags
            tiles_df["flag"] = np.logical_or(tiles_df["flag_metafits"], tiles_df["flag_soln"])
            tiles_df.drop(columns=["flag_metafits", "flag_soln"], inplace=True)
        tiles = tiles_df[tiles_df.flag == 0]
        if not len(tiles):
            raise ValueError("No unflagged tiles found")
        # tiles_by_id = sorted(tiles, key=lambda tile: tile.id)
        return tiles.sort_values(by=["id"]).take([0]).iloc[0]

    @property
    def calibrator(self):
        calibrators = set(filter(None, [meta.calibrator for meta in self.metafits]))
        return " ".join(calibrators)  # type: ignore

    @property
    def obsids(self):
        obsids = set(filter(None, [meta.obsid for meta in self.metafits]))
        return [*obsids]  # type: ignore

    @property
    def results(self) -> NDArray[np.float64]:
        """
        Get the combined results array for all solutions

        Pad results if edge channels have been removed
        """
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
        """
        Generate an array of weights for each solution, based on results
        """
        try:
            results = self.results
            results[results < 0] = np.nan
            results[results > 1e-4] = np.nan
            exp_results = np.exp(-results)
            return np.nan_to_num(
                (exp_results - np.nanmin(exp_results)) / (np.nanmax(exp_results) - np.nanmin(exp_results))
            )
        except KeyError:
            return np.full(len(self.all_chanblocks_hz[0]), 1.0)

    def get_solns(self, refant_name=None) -> Tuple[NDArray[np.int_], NDArray[np.complex128], NDArray[np.complex128]]:
        """
        Get the tile ids in the order they appear in the solutions, as well as xx and yy solutions
        for the reference antenna
        """
        soln_tile_ids = None
        ref_tile_idx = None
        all_xx_solns = None
        all_yy_solns = None

        for chanblocks_hz, soln in zip(self.all_chanblocks_hz, self.solns):

            # TODO: ch_flags = hdus['CHANBLOCKS'].data['Flag']
            # TODO: results = hdus['RESULTS'].data.flatten()

            # validate tile selection
            # join the tile dataframe on name, just in case order is different

            soln_tiles = self.metafits_tiles_df.copy()
            soln_tiles["flag_metafits"] = soln_tiles["flag"]
            soln_tiles["flag_soln"] = soln.tile_flags
            soln_tiles["flag"] = np.logical_or(soln_tiles["flag_soln"], soln_tiles["flag_metafits"])
            soln_tiles.drop(columns=["flag_metafits", "flag_soln"], inplace=True)
            # self.logger.debug(f"{soln.filename} - tiles:\n{soln_tiles.to_string(max_rows=999)}")
            if refant_name is not None:
                _ref_tiles = soln_tiles[soln_tiles["name"] == refant_name]
                if not len(_ref_tiles):
                    raise RuntimeError(f"{soln.filename} - reference tile {refant_name}" f" not found in solution file")
                if len(_ref_tiles) > 1:
                    raise RuntimeError(
                        f"{soln.filename} - more than one tile with name {refant_name}" f" found in solution file"
                    )
                _ref_tile_idx = _ref_tiles.index[0]
                _ref_tile_flag = _ref_tiles.iloc[0]["flag"]
                if _ref_tile_flag:
                    raise RuntimeError(
                        f"{soln.filename} - reference tile {refant_name}"
                        f" is flagged in solutions file (index {_ref_tile_idx})"
                    )

                if not ref_tile_idx:
                    ref_tile_idx = _ref_tile_idx
                    # self.logger.debug(f"{soln.filename} - ref tile found at index {ref_tile_idx}")
                elif ref_tile_idx != _ref_tile_idx:
                    raise RuntimeError(
                        f"{soln.filename} - reference tile in solution file" f" does not match previous solution files"
                    )

            _tile_ids = soln_tiles["id"].to_numpy()
            # _tile_ids, _ref_tile_idx = soln.validate_tiles(tiles_by_name, refant)
            if soln_tile_ids is None or not len(soln_tile_ids):
                soln_tile_ids = soln_tiles["id"].to_numpy()
                # self.logger.debug(f"{soln.filename} - found {len(soln_tile_ids)} matching tiles")  # type: ignore
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


v_light_m_s = 299792458.0


class PhaseFitInfo(NamedTuple):
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
    quality: float
    gains: List[float]
    pol0: List[float]
    pol1: List[float]
    sigma_resid: List[float]

    @staticmethod
    def default():
        return GainFitInfo(
            quality=1.0,
            gains=[1.0] * 24,
            pol0=[0.0] * 24,
            pol1=[0.0] * 24,
            sigma_resid=[0.0] * 24,
        )

    @staticmethod
    def nan():
        return GainFitInfo(
            quality=np.nan,
            gains=[np.nan] * 24,
            pol0=[np.nan] * 24,
            pol1=[np.nan] * 24,
            sigma_resid=[np.nan] * 24,
        )


def wrap_angle(angle):
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
    """
    Linear fit phases
        - freqs: array of frequencies in Hz
        - solution: complex array of solutions
        - niter: number of iterations to perform

    Credit: Dr. Sammy McSweeny
    """

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
    #   - is roughly centered on the DC bin
    #   - has a large amount of zero padding on either side
    ν = freqs_hz * u.Hz  # type: ignore
    bins = np.round((ν / dν).decompose().value).astype(int)
    ctr_bin = (np.min(bins) + np.max(bins)) // 2
    shifted_bins = bins - ctr_bin  # Now "bins" represents where I want to put the solution values

    # ...except that ~1/2 of them are negative, so I'll have to add a certain amount
    # once I decide how much zero padding to include.
    # This is set by the resolution I want in delay space (Nyquist rate)
    # type: ignore
    dm = 0.01 * u.m  # type: ignore
    dt = dm / c  # The target time resolution
    νmax = 0.5 / dt  # The Nyquist rate
    N = 2 * int(np.round(νmax / dν))  # The number of bins to use during the FFTs

    shifted_bins[
        shifted_bins < 0
    ] += N  # Now the "negative" frequencies are put at the end, which is where FFT wants them

    # Create a zero-padded, shifted version of the spectrum, which I'll call sol0
    # sol0: This shifts the non-zero data down to a set of frequencies straddling the DC bin.
    #       This makes the peak in delay space broad, and lets us hone in near the optimal solution by
    #       finding the peak in delay space
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

    resid_std, chi2dof, stderr = None, None, None
    # while len(mask) >= 2 and (niter:= niter - 1) <= 0:
    while True:
        res = minimize(objective, params, args=(ν.to(u.Hz).value, solution))
        params = res.x
        # print(f"{params=}")
        # print(f"{res.hess_inv=}")
        # print(f"{np.angle(solution)[:3]=}")
        constructed = model(ν.to(u.Hz).value, *params)
        # print(f"{constructed[:3]=}")
        residuals = wrap_angle(np.angle(solution) - np.angle(constructed))
        # print(f"{residuals[:3]=}")
        chi2dof = np.sum(np.abs(residuals) ** 2) / (len(residuals) - len(params))
        # print(f"{chi2dof=}")
        resid_std = residuals.std()
        # print(f"{resid_std=}")
        resid_var = residuals.var(ddof=len(params))
        # print(f"{resid_var=}")
        stderr = np.sqrt(np.diag(res.hess_inv * resid_var))
        # print(f"{stderr=}")
        mask = np.where(np.abs(residuals) < 2 * stderr[0])[0]
        solution = solution[mask]
        ν = ν[mask]
        # TODO: iterations?
        # niter = niter-1
        # if len(mask) < 2 or niter <= 0:
        #     break
        break

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


def fit_gain(chanblocks_hz, solns, weights, chanblocks_per_coarse) -> GainFitInfo:
    """
    Fit gain solutions
    """
    # length check
    assert len(chanblocks_hz) == len(solns) == len(weights)
    n_coarse = len(chanblocks_hz) // chanblocks_per_coarse

    amps = np.abs(solns)

    gains = np.full(n_coarse, np.nan)
    pol0 = np.full(n_coarse, np.nan)
    pol1 = np.full(n_coarse, np.nan)
    sigma_resid = np.full(n_coarse, np.nan)

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
        coarse_amps = coarse_amps[coarse_mask]
        coarse_hz = coarse_hz[coarse_mask]
        coarse_weights = coarse_weights[coarse_mask]

        gains[coarse_idx] = np.sum(coarse_amps * coarse_weights) / np.sum(coarse_weights)
        # TODO(Dev): finish this bit
        pol0[coarse_idx] = 0.0
        pol1[coarse_idx] = 0.0
        sigma_resid[coarse_idx] = 0.0

    # TODO(Dev): calculate quality
    quality = 1.0

    return GainFitInfo(
        quality=quality,
        gains=gains.tolist(),
        pol0=pol0.tolist(),
        pol1=pol1.tolist(),
        sigma_resid=sigma_resid.tolist(),
    )


def poly_str(coeffs, independent_var="x"):
    """
    Given a dataframe of [tile, pol, fit...]:
    - plot intercepts
    - save fits to tsv
    - plot fits
    - save residuals to tsv
    - plot residuals
    - return pivoted dataframe
    """

    def xpow(i):
        if i == 0:
            return ""
        elif i == 1:
            return f"×{independent_var}"
        else:
            return f"×{independent_var}" + "⁰¹²³⁴⁵⁶⁷⁸⁹"[i]

    return " ".join(
        filter(None, [f"{coeff:+.3}{xpow(i)}" for i, coeff in enumerate(coeffs[::-1])])  # if abs(coeff) > 1e-20 else ""
    )


def textwrap(s, width=70):
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
    """
    Given a dataframe of [tile, pol, fit...]:
    - plot intercepts
    - save fits to tsv
    - plot fits
    - save residuals to tsv
    - plot residuals
    - return pivoted dataframe
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
        print(f"flagged {len(bad_fits)} of {n_total} fits as outliers:")
        print(bad_fits[["name", "pol"]].to_string(index=False))

    # make a new colormap for weighted data
    half_blues = LinearSegmentedColormap.from_list(
        colors=mpl.colormaps["Blues"](np.linspace(0.5, 1, 256)),
        name="HalfBlues",
    )

    if len(flavor_fits):
        rx_means = plot_rx_lengths(flavor_fits, prefix, show, title)
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
            signal = soln[fit["soln_idx"]]  # type: ignore
            if fit["flag"] or np.isnan(signal).all():
                continue
            mask = np.where(np.logical_and(np.isfinite(signal), weights2 > 0))[0]
            angle = np.angle(signal)  # type: ignore
            mask_freq: ArrayLike = freqs[mask]  # type: ignore
            model_freqs = np.linspace(mask_freq.min(), mask_freq.max(), len(freqs))  # type: ignore
            rx_idx = np.where(rxs == fit["rx"])[0][0]
            slot_idx = np.where(slots == fit["slot"])[0][0]
            ax = axs[rx_idx][slot_idx]  # type: ignore
            ax.axis("on")
            gradient = (2 * np.pi * u.rad * (fit[f"length_{pol}"] * u.m) / c).to(u.rad / u.Hz).value
            intercept = fit[f"intercept_{pol}"]
            model = gradient * model_freqs + intercept
            ax.scatter(model_freqs, wrap_angle(model), c="red", s=0.5)
            mask_weights: ArrayLike = weights2[mask]  # type: ignore
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
        intercepts = intercepts.to_numpy()
        pol = pols.iloc[0]
        flav = flavs.iloc[0]
        if pol == "XX":
            solns = soln_xx[soln_idxs.values]
        elif pol == "YY":
            solns = soln_yy[soln_idxs.values]
        else:
            raise RuntimeError(f"wut pol? {pol}")
        models = gradients[:, np.newaxis] * freqs[np.newaxis, :] + intercepts[:, np.newaxis]
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
                    print(traceback.format_exc())
                    print(
                        f"Skipping polyfit({order=}, {indep_var=}) due to "
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

        print(f"{flav=} {pol=} {eqn=}")

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
    """
    Given two dataframes:
    - per-pol phase fits - [tile, pol, fit...]:
    - tile metadata - [soln_idx, name, tile_id, rx, slot, flavor]
    pivot the dataframe to [tile, fit_xx, fit_yy, ...]
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
    """Returns a list of tuples which represent a summary
    of the convergence of the solutions"""
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


def write_stats(
    logger,
    obs_id,
    stats_filename,
    hyperdrive_solution_filename,
    hyperdrive_binary_path,
    metafits_filename,
) -> Tuple[bool, str]:
    """This method produces convergence stats and plots
    Returns:
    bool = Success/fail
    str  = Error message if fail"""
    logger.info(f"{obs_id} Writing stats for {hyperdrive_solution_filename}...")

    try:
        conv_summary_list = get_convergence_summary(hyperdrive_solution_filename)

        with open(stats_filename, "w", encoding="UTF-8") as stats:
            for row in conv_summary_list:
                stats.write(f"{row[0]}: {row[1]}\n")

        # Now run hyperdrive again to do some plots
        hyp_soln_plot_args = (
            f"--max-amp 2 --no-ref-tile --output-directory {os.path.dirname(hyperdrive_solution_filename)}"
        )
        cmd = (
            f"{hyperdrive_binary_path} solutions-plot {hyp_soln_plot_args} "
            f"-m"
            f" {metafits_filename} {hyperdrive_solution_filename}"
        )

        return_value, _ = run_command_ext(logger, cmd, -1, timeout=10, use_shell=False)

        logger.info(
            f"{obs_id} Finished running hyperdrive stats on" f" {hyperdrive_solution_filename}. Return={return_value}"
        )
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    # try:
    #     if return_value:
    #         # Currently, hyperdrive writes the solution files to same dir as
    #         # the current directory mwax_calvin_processor is run from
    #         # Move them to the processing_complete dir
    #         plot_filename_base = os.path.basename(f"{os.path.splitext(hyperdrive_solution_filename)[0]}")

    #         amps_plot_filename = f"{plot_filename_base}_amps.png"
    #         phase_plot_filename = f"{plot_filename_base}_phases.png"
    #         shutil.move(
    #             os.path.join(os.getcwd(), amps_plot_filename),
    #             os.path.join(stats_path, f"{amps_plot_filename}"),
    #         )
    #         shutil.move(
    #             os.path.join(os.getcwd(), phase_plot_filename),
    #             os.path.join(stats_path, f"{phase_plot_filename}"),
    #         )
    #         logger.info(f"{obs_id} plots moved successfully to {stats_path}.")

    #         logger.info(f"{obs_id} write_stats() complete successfully")
    #     else:
    #         logger.error(f"{obs_id} write_stats() failed")
    # except Exception as catch_all_exception:
    #     return False, str(catch_all_exception)

    return True, ""


def write_readme_file(logger, filename, cmd, exit_code, stdout, stderr):
    """This will create a small readme.txt file which will
    hopefully help whoever poor sap is checking why birli
    or hyperdrive or the upload_handler did or did not work!"""
    try:
        with open(filename, "w", encoding="UTF-8") as readme:
            if exit_code == 0:
                readme.write("This run succeded at:" f" {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
            else:
                readme.write("This run failed at:" f" {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
            readme.write(f"Command: {cmd}\n")
            readme.write(f"Exit code: {exit_code}\n")
            readme.write(f"stdout: {stdout}\n")
            readme.write(f"stderr: {stderr}\n")

    except Exception:
        logger.warning(
            (f"Could not write text file {filename} describing the" " problem observation."),
            exc_info=True,
        )


def run_birli(
    processor,
    metafits_filename: str,
    uvfits_filename: str,
    obs_id: int,
    processing_dir: str,
) -> bool:
    """Execute Birli, returning true on success, false on failure"""
    birli_success: bool = False
    start_time = time.time()
    stderr = ""

    cmdline = None
    exit_code = None
    stdout = None
    try:
        # Get only data files
        data_files = glob.glob(os.path.join(processing_dir, "*.fits"))
        # Remove the metafits (we specify it seperately)
        try:
            data_files.remove(metafits_filename)
        except ValueError:
            # Metafits was not in the file list
            raise Exception(f"Metafits file '{metafits_filename}' was not found. Cannot run birli.")

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

        # TODO: set default edge_width res from config
        edge_width_hz = 80e3  # default
        edge_width_hz = np.max([fine_chan_width_hz, edge_width_hz])
        assert edge_width_hz >= fine_chan_width_hz, f"{edge_width_hz=} must be >= {fine_chan_width_hz=}"
        assert edge_width_hz % fine_chan_width_hz == 0, f"{edge_width_hz=} must multiple of {fine_chan_width_hz=}"

        # TODO: set minimum freq res from config
        min_freq_res = 40e3
        avg_arg = ""
        if fine_chan_width_hz < min_freq_res:
            avg_arg += f" --avg-freq-res={int(min_freq_res/1e3)}"

        # TODO: set minimum time res from config
        min_time_res = 2
        if time_time_s < min_time_res:
            avg_arg += f" --avg-time-res={min_time_res}"

        # Run birli
        cmdline = (
            f"{processor.birli_binary_path}"
            f" --metafits {metafits_filename}"
            " --no-draw-progress"
            f" --uvfits-out={uvfits_filename}"
            f" --flag-edge-width={int(edge_width_hz/1e3)}"
            f" --max-memory={processor.birli_max_mem_gib}"
            f" {avg_arg} {data_file_arg}"
        )

        processor.birli_popen_process = run_command_popen(processor.logger, cmdline, -1, False)

        exit_code, stdout, stderr = check_popen_finished(
            processor.logger,
            processor.birli_popen_process,
            processor.birli_timeout,
        )

        # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
        elapsed = time.time() - start_time

        if exit_code == 0:
            # Success!
            processor.logger.info(f"{obs_id}: Birli run successful in {elapsed:.3f} seconds")
            birli_success = True
            processor.birli_popen_process = None

            # Success!
            # Write out a useful file of command line info
            readme_filename = os.path.join(processing_dir, f"{obs_id}_birli_readme.txt")
            write_readme_file(
                processor.logger,
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
        else:
            processor.logger.error(
                f"{obs_id}: Birli run FAILED: Exit code of {exit_code} in" f" {elapsed:.3f} seconds: {stderr}"
            )
    except Exception as birli_run_exception:
        elapsed = time.time() - start_time
        processor.logger.error(
            f"{obs_id}: birli run FAILED: Unhandled exception"
            f" {birli_run_exception} in {elapsed:.3f} seconds:"
            f" {stderr}"
        )

    if not birli_success:
        # If we are not shutting down,
        # Move the files to an error dir
        #
        # If we are shutting down, then this error is because
        # we have effectively sent it a SIGINT. This should not be a
        # reason to abandon processing. Leave it there to be picked up
        # next run (ie this will trigger the "else" which does nothing)
        if processor.running:
            error_path = os.path.join(processor.processing_error_path, str(obs_id))
            processor.logger.info(
                f"{obs_id}: moving failed files to {error_path} for manual" " analysis and writing readme_error.txt"
            )

            # Move the processing dir
            shutil.move(processing_dir, error_path)

            # Write out a useful file of error and command line info
            readme_filename = os.path.join(error_path, "readme_error.txt")
            write_readme_file(
                processor.logger,
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )

    return birli_success


def run_hyperdrive(
    processor,
    uvfits_files,
    metafits_filename: str,
    obs_id: int,
    processing_dir: str,
) -> bool:
    """Runs hyperdrive N times and returns true on success or false if not all runs worked"""
    processor.logger.info(
        f"{obs_id}: {len(uvfits_files)} contiguous bands detected." f" Running hyperdrive {len(uvfits_files)} times...."
    )

    # Keep track of the number of successful hyperdrive runs
    hyperdrive_runs_success: int = 0

    for hyperdrive_run, uvfits_file in enumerate(uvfits_files):
        # Take the filename which for picket fence will also have
        # the band info and in all cases the obsid. We will use
        # this as a base for other files we work with
        obsid_and_band = uvfits_file.replace(".uvfits", "")

        try:
            hyperdrive_solution_filename = f"{obsid_and_band}_solutions.fits"
            bin_solution_filename = f"{obsid_and_band}_solutions.bin"

            # Run hyperdrive
            # Output to hyperdrive format and old aocal format (bin)
            cmdline = (
                f"{processor.hyperdrive_binary_path} di-calibrate"
                " --no-progress-bars --data"
                f" {uvfits_file} {metafits_filename} --num-sources 99"
                " --source-list"
                f" {processor.source_list_filename} --source-list-type"
                f" {processor.source_list_type} --outputs"
                f" {hyperdrive_solution_filename} {bin_solution_filename}"
            )

            start_time = time.time()

            # run hyperdrive
            processor.logger.info(f"{obs_id}: Running hyperdrive on {uvfits_file}...")
            processor.hyperdrive_popen_process = run_command_popen(processor.logger, cmdline, -1, False)

            exit_code, stdout, stderr = check_popen_finished(
                processor.logger,
                processor.hyperdrive_popen_process,
                processor.hyperdrive_timeout,
            )

            # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
            elapsed = time.time() - start_time

            if exit_code == 0:
                processor.logger.info(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(uvfits_files)} successful"
                    f" in {elapsed:.3f} seconds"
                )
                processor.hyperdrive_popen_process = None

                # Success!
                # Write out a useful file of command line info
                readme_filename = f"{obsid_and_band}_hyperdrive_readme.txt"

                write_readme_file(
                    processor.logger,
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )

                hyperdrive_runs_success += 1
            else:
                processor.logger.error(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(uvfits_files)} FAILED:"
                    f" Exit code of {exit_code} in"
                    f" {elapsed:.3f} seconds. StdErr: {stderr}"
                )
                # exit for loop- since run failed
                break
        except Exception as hyperdrive_run_exception:
            processor.logger.error(
                f"{obs_id}: hyperdrive run"
                f" {hyperdrive_run + 1}/{len(uvfits_files)} FAILED:"
                " Unhandled exception"
                f" {hyperdrive_run_exception} in"
                f" {elapsed:.3f} seconds. StdErr: {stderr}"
            )
            # exit for loop since run failed
            break

    if hyperdrive_runs_success != len(uvfits_files):
        # We did not run successfully on one or all hyperdrive calls.
        # If we are not shutting down,
        # Move the files to an error dir
        #
        # If we are shutting down, then this error is because
        # we have effectively sent it a SIGINT. This should not be a
        # reason to abandon processing. Leave it there to be picked up
        # next run (ie this will trigger the "else" which does nothing)
        if processor.running:
            error_path = os.path.join(processor.processing_error_path, str(obs_id))
            processor.logger.info(
                f"{obs_id}: moving failed files to {error_path} for manual" " analysis and writing readme_error.txt"
            )

            # Move the processing dir
            shutil.move(processing_dir, error_path)

            # Write out a useful file of error and command line info
            readme_filename = os.path.join(error_path, "readme_error.txt")
            write_readme_file(
                processor.logger,
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
        return False
    else:
        return True


def run_hyperdrive_stats(
    processor,
    uvfits_files,
    metafits_filename: str,
    obs_id: int,
    processing_dir: str,
) -> bool:
    """Call hyperdrive again but just to produce plots and stats"""

    # produce stats/plots
    stats_successful: int = 0

    processor.logger.info(
        f"{obs_id}: {len(uvfits_files)} contiguous bands detected."
        f" Running hyperdrive stats {len(uvfits_files)} times...."
    )

    for hyperdrive_stats_run, uvfits_file in enumerate(uvfits_files):
        # Take the filename which for picket fence will also have
        # the band info and in all cases the obsid. We will use
        # this as a base for other files we work with
        obsid_and_band = uvfits_file.replace(".uvfits", "")

        hyperdrive_solution_filename = f"{obsid_and_band}_solutions.fits"
        stats_filename = f"{obsid_and_band}_stats.txt"

        (
            stats_success,
            stats_error,
        ) = write_stats(
            processor.logger,
            obs_id,
            stats_filename,
            hyperdrive_solution_filename,
            processor.hyperdrive_binary_path,
            metafits_filename,
        )

        if stats_success:
            stats_successful += 1
        else:
            processor.logger.warning(
                f"{obs_id}: hyperdrive stats run"
                f" {hyperdrive_stats_run + 1}/{len(uvfits_files)} FAILED:"
                f" {stats_error}."
            )

    if stats_successful == len(uvfits_files):
        processor.logger.info(f"{obs_id}: All {stats_successful} hyperdrive stats" " runs successful")
        return True
    else:
        processor.logger.warning(f"{obs_id}: Not all hyperdrive stats runs were successful.")
        return False


def process_phase_fits(
    logger, item, tiles, chanblocks_hz, all_xx_solns, all_yy_solns, weights, soln_tile_ids, phase_fit_niter
):
    """
    Fit a line to each tile phase solution, return a dataframe of phase fit parameters for each
    tile and pol
    """
    fits = []
    phase_diff_path = os.path.join(item, "phase_diff.txt")
    # by default we don't want to apply any phase rotation.
    phase_diff = np.full((len(chanblocks_hz),), 1.0, dtype=np.complex128)
    if os.path.exists(phase_diff_path):
        # phase_diff_raw is an array, first column is frequency, second column is phase difference
        phase_diff_raw = np.loadtxt(phase_diff_path)
        for i, chanblock_hz in enumerate(chanblocks_hz):
            # find the closest frequency in phase_diff_raw
            idx = np.abs(phase_diff_raw[:, 0] - chanblock_hz).argmin()
            diff = phase_diff_raw[idx, 1]
            phase_diff[i] = np.exp(-1j * diff)

    for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
        for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
            id_matches = tiles[tiles.id == tile_id]
            if len(id_matches) != 1:
                continue
            tile = id_matches.iloc[0]
            if tile.flag:
                continue
            name = tile.name
            if tile.flavor.endswith("-NI"):
                solns *= phase_diff
            # else:
            #     continue
            try:
                fit = fit_phase_line(chanblocks_hz, solns, weights, niter=phase_fit_niter)
            except Exception as exc:
                logger.error(f"{item} - {tile_id=:4} {pol} ({name}) {exc}")
                continue
            logger.debug(f"{item} - {tile_id=:4} {pol} ({name}) {fit=}")
            fits.append([tile_id, soln_idx, pol, *fit])

    return DataFrame(fits, columns=["tile_id", "soln_idx", "pol", *PhaseFitInfo._fields])


def process_gain_fits(
    logger, item, tiles, chanblocks_hz, all_xx_solns, all_yy_solns, weights, soln_tile_ids, chanblocks_per_coarse
):
    """
    for each tile, pol, fit a GainFitInfo to the gains
    """
    fits = []
    for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
        for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
            id_matches = tiles[tiles.id == tile_id]
            if len(id_matches) != 1:
                continue
            tile = id_matches.iloc[0]
            if tile.flag:
                continue
            name = tile.name
            try:
                fit = fit_gain(chanblocks_hz, solns, weights, chanblocks_per_coarse)
            except Exception as exc:
                logger.error(f"{item} - {tile_id=:4} {pol} ({name}) {exc}")
                continue
            logger.debug(f"{item} - {tile_id=:4} {pol} ({name}) {fit=}")
            fits.append([tile_id, soln_idx, pol, *fit])
    logger.warning("TODO: fake gain fits!")

    return DataFrame(fits, columns=["tile_id", "soln_idx", "pol", *GainFitInfo._fields])
