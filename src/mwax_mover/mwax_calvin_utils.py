"""Utility Functions to support mwax_calvin processor"""
import datetime
import glob
import os
import shutil
import time
import numpy as np
from astropy.io import fits
from enum import Enum
from scipy import stats
from astropy import units as u
from astropy.constants import c
from scipy.optimize import minimize
import pandas as pd
from mwax_mover.mwax_command import (
    run_command_ext,
    run_command_popen,
    check_popen_finished,
)

import numpy.typing as npt
from numpy.typing import ArrayLike, NDArray
from typing import NamedTuple, List, Tuple, Dict
# from nptyping import NDArray, Shape

class Tile(NamedTuple):
    """Info about an MWA tile"""
    name: str
    id: int
    flag: bool
    # index: int
    rx: int
    slot: int

class ChanInfo(NamedTuple):
    """channel selection info"""
    coarse_chan_ranges: List[NDArray[np.int_]]
    fine_chans_per_coarse: int
    fine_chan_width_hz: float

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

class Metafits:
    """MWA Metadata file in FITS format"""
    def __init__(self, filename: str):
        self.filename = filename

    def get_tiles(self) -> List[Tile]:
        """Get tile info from metafits, sorted by index"""

        with fits.open(self.filename) as hdus:
            metafits_tiles = hdus['TILEDATA'].data  # type: ignore

        # using a set here to avoid duplicates (pol=X,Y)
        tiles = set(
            Tile(
                name=metafits_tile["TileName"],
                id=metafits_tile["Tile"],
                flag=metafits_tile["Flag"],
                # index=metafits_tile["Antenna"],
                rx=metafits_tile["Rx"],
                slot=metafits_tile["Slot"],
            ) for metafits_tile in metafits_tiles
        )

        # return sorted(tiles, key=lambda tile: tile.index)
        return [*tiles]
    
    def get_tiles_df(self) -> pd.DataFrame:
        """Get reference antenna (unflagged tile with lowest id) and tiles as df"""
        # determine array configuration (compact or extended)
        # config = Config.from_tiles(tiles)
        tiles = self.get_tiles()
        return pd.DataFrame(tiles, columns=Tile._fields)
        # unflagged = tiles[tiles.flag == 0]
        # if not len(unflagged):
        #     raise ValueError("No unflagged tiles found")
        # # tiles_by_id = sorted(tiles, key=lambda tile: tile.id)
        # return unflagged.sort_values(by=["id"]).take([0])["name"], tiles
    
    def get_chan_info(self) -> ChanInfo:
        """Get coarse channels from metafits, sorted"""
        with fits.open(self.filename) as hdus:
            hdu = hdus['PRIMARY']
            header: fits.header.Header = hdu.header # type: ignore

            # get a list of coarse channels
            coarse_chans = header["CHANNELS"].split(",") # type: ignore
            coarse_chans = np.array(sorted(int(i) for i in coarse_chans))
            # fine channel width
            fine_chan_width_hz = float(header["FINECHAN"]) * 1000 # type: ignore
            # number of fine channels in observation
            obs_num_fine_chans: int = header["NCHANS"] # type: ignore
            # calculate number of fine channels per coarse channel
            if obs_num_fine_chans % len(coarse_chans) != 0:
                raise ValueError(
                    f"Number of fine channels ({obs_num_fine_chans}) "
                    f"not a multiple of the number of coarse channels ({len(coarse_chans)})"
                )
            fine_chans_per_coarse = obs_num_fine_chans // len(coarse_chans)

        coarse_chan_ranges = []
        for _, g in enumerate(np.split(coarse_chans, np.where(np.diff(coarse_chans) != 1)[0] + 1)):
            coarse_chan_ranges.append(g)

        return ChanInfo(
            coarse_chan_ranges=coarse_chan_ranges,
            fine_chan_width_hz=fine_chan_width_hz,
            fine_chans_per_coarse=fine_chans_per_coarse,
        )


class HyperfitsSolution:
    """A single calibration solution in hyperdrive FITS format"""
    def __init__(self, filename) -> None:
        self.filename = filename

    def get_chanblocks_hz(self) -> NDArray[np.float_]:
        """Get channels from solution file"""
        with fits.open(self.filename) as hdus:
            freq_data = np.array(hdus['CHANBLOCKS'].data['Freq']) # type: ignore
        return freq_data
    
    def validate_chanblocks(self, 
                            chaninfo: ChanInfo,
                            coarse_chans: NDArray[np.int_], 
        ) -> Tuple[NDArray[np.float_], float]:
        """
        Validate that the chanblocks in the solution match the chanblocks in the metafits.
        Returns the chanblock frequencies and the number of chanblocks per coarse channel.
        """
        # TODO: validate the actual frequencies agains coarse_chans
        # coarse_chans = chaninfo.coarse_chan_ranges[coarse_chan_range_idx]
        chanblocks_hz = self.get_chanblocks_hz()
        chanblock_width_hz = chanblocks_hz[1] - chanblocks_hz[0]  # type: ignore
        if chanblock_width_hz % chaninfo.fine_chan_width_hz != 0:
            raise RuntimeError(
                f"{self.filename} - chanblock width in solution file ({chanblock_width_hz})"
                f" is not a multiple of fine channel width in metafits ({chaninfo.fine_chan_width_hz})"
            )
    
        chans_per_block = chanblock_width_hz // chaninfo.fine_chan_width_hz
        chanblocks_per_coarse = chaninfo.fine_chans_per_coarse // chans_per_block
        
        if (range_ncoarse:=len(coarse_chans)) != (soln_ncoarse:= len(chanblocks_hz) // chanblocks_per_coarse):  # type: ignore
            raise RuntimeError(
                f"{self.filename} - number of coarse channels in solution file ({soln_ncoarse})"
                f" does not match number of coarse channels in metafits for this range ({range_ncoarse})"
                f" given {chanblocks_per_coarse=}, {chans_per_block=}"
            )
        return (
            chanblocks_hz, 
            chanblocks_per_coarse
        )

    def get_tile_names_flags(self) -> List[Tuple[str, bool]]:
        """Get the tile names and flags ordered by index"""
        with fits.open(self.filename) as hdus:
            tile_data = hdus['TILES'].data  # type: ignore
        return [
            (tile["TileName"], tile["Flag"])
            for tile in tile_data
        ]
    
    # def validate_tiles(self, tiles_by_name: Dict[str, Tile], refant: Tile) -> Tuple[NDArray[np.int_], int]:
    #     """Validate that the tiles in the solution match the tiles in the metafits"""
    #     tile_ids = []
    #     ref_tile_idx = None
    #     for tile_name, flag in self.get_tile_names_flags():
    #         if tile_name not in tiles_by_name:
    #             raise RuntimeError(
    #                 f"{self.filename} - tile {tile_name} in solution file"
    #                 f" is not in metafits"
    #             )
    #         if tile_name == refant.name:
    #             if flag:
    #                 raise RuntimeError(
    #                     f"{self.filename} - reference tile {refant.name}"
    #                     f" is flagged in solutions file"
    #                 )
    #             ref_tile_idx = len(tile_ids)
    #         tile_ids.append(tiles_by_name[tile_name].id)
    #     if ref_tile_idx is None:
    #         raise RuntimeError(
    #             f"{self.filename} - reference tile {refant.name}"
    #             f" not found in solution file"
    #         )
    #     return (
    #         tile_ids,
    #         ref_tile_idx,
    #     )
    
    def get_average_times(self) -> List[float]:
        """Get the average time for each timeblock"""
        with fits.open(self.filename) as hdus:
            time_data = hdus['TIMEBLOCKS'].data  # type: ignore

        return [
            time['Average'] for time in time_data
        ]

    def get_solutions(self) -> List[NDArray[np.complex_]]:
        """Get solutions as a complex array for each pol: [time, tile, chan]"""
        with fits.open(self.filename) as hdus:
            solutions = hdus['SOLUTIONS'].data  # type: ignore
        return [
            solutions[:, :, :, 0] + 1j * solutions[:, :, :, 1],
            solutions[:, :, :, 2] + 1j * solutions[:, :, :, 3],
            solutions[:, :, :, 4] + 1j * solutions[:, :, :, 5],
            solutions[:, :, :, 6] + 1j * solutions[:, :, :, 7],
        ]
    
    def get_ref_solutions(self, ref_tile_idx) -> List[NDArray[np.complex_]]:
        if ref_tile_idx is None:
            raise RuntimeError("no ref_tile_idx")
        """Get solutions divided by reference tile as a complex array for each pol: [time, tile, chan]"""
        solutions = self.get_solutions()
        # divide solutions by reference
        ref_solutions = [solution[:, ref_tile_idx, :] for solution in solutions]  # type: ignore
        # divide solutions jones matrix by reference jones matrix, via inverse determinant
        ref_inv_det = np.divide(1 + 0j, ref_solutions[0] * ref_solutions[3] - ref_solutions[1] * ref_solutions[2])  # type: ignore
        return [ # type: ignore
            (solutions[0] * ref_solutions[3] - solutions[1] * ref_solutions[2]) * ref_inv_det,
            (solutions[1] * ref_solutions[0] - solutions[0] * ref_solutions[1]) * ref_inv_det,
            (solutions[2] * ref_solutions[3] - solutions[3] * ref_solutions[2]) * ref_inv_det,
            (solutions[3] * ref_solutions[0] - solutions[2] * ref_solutions[1]) * ref_inv_det,
        ]
    
    def get_convergence_results(self) -> NDArray[np.float_]:
        """Code adapted from Chris Jordan's scripts"""
        with fits.open(self.filename) as hdus:
            return hdus["RESULTS"].data.flatten()  # type: ignore

        # Not sure why I need flatten!
    
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
        
def wrap_angle(angle):
    return np.mod(angle + np.pi, 2 * np.pi) - np.pi

def fit_phase_line(
        freqs_hz: NDArray[np.float_],  #  NDArray[np.float_],
        solution: NDArray[np.complex_],
        weights: NDArray[np.float_],
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
    mask = np.where(np.logical_and(
        np.isfinite(solution),
        weights > 0
    ))[0]
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
    bins = np.round((ν/dν).decompose().value).astype(int)
    ctr_bin = (np.min(bins) + np.max(bins))//2
    shifted_bins = bins - ctr_bin # Now "bins" represents where I want to put the solution values

    # ...except that ~1/2 of them are negative, so I'll have to add a certain amount
    # once I decide how much zero padding to include.
    # This is set by the resolution I want in delay space (Nyquist rate)
    # type: ignore
    dm = 0.01 * u.m  # type: ignore
    dt = dm / c           # The target time resolution
    νmax = 0.5 / dt               # The Nyquist rate
    N = 2*int(np.round(νmax/dν))  # The number of bins to use during the FFTs
    
    shifted_bins[shifted_bins < 0] += N  # Now the "negative" frequencies are put at the end, which is where FFT wants them
    
    # Create a zero-padded, shifted version of the spectrum, which I'll call sol0
    # sol0: This shifts the non-zero data down to a set of frequencies straddling the DC bin.
    #       This makes the peak in delay space broad, and lets us hone in near the optimal solution by
    #       finding the peak in delay space
    sol0 = np.zeros((N,)).astype(complex)
    sol0[shifted_bins] = solution

    # IFFT of sol0 to get the approximate solution as the peak in delay space
    isol0 = np.fft.ifft(sol0)
    t = -np.fft.fftfreq(len(sol0), d=dν.to(u.Hz).value) * u.s  # (Not sure why this negative is needed)
    d = np.fft.fftshift(c*t)
    isol0 = np.fft.fftshift(isol0)
    
    # Find max peak, and the equivalent slope
    imax = np.argmax(np.abs(isol0))
    dmax = d[imax]
    # print(f"{dmax=:.02f}")
    slope = (2*np.pi*u.rad*dmax/c).to(u.rad/u.Hz)
    # print(f"{slope=:.10f}")
    
    # Now that we're near a local minimum, get a better one by doing a standard minimisation
    # To get the y-intercept, divide the original data by the constructed data
    # and find the average phase of the result
    if fit_iono:
        def model(ν, m, c, α):
            return np.exp(1j*(m*ν + c + α/ν**2))
        y_int = np.angle(np.mean(solution/model(ν.to(u.Hz).value, slope.value, 0, 0)))
        params = (slope.value, y_int, 0)
    else:
        def model(ν, m, c):
            return np.exp(1j*(m*ν + c))
        y_int = np.angle(np.mean(solution/model(ν.to(u.Hz).value, slope.value, 0)))
        params = (slope.value, y_int)
        
    def objective(params, ν, data):
        constructed = model(ν, *params)
        residuals = np.angle(data) - np.angle(constructed)
        cost = np.sum(np.abs(residuals)**2)
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
        residuals = np.angle(solution) - np.angle(constructed)
        # print(f"{residuals[:3]=}")
        chi2dof = np.sum(np.abs(residuals)**2) / (len(residuals) - len(params))
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

def debug_phase_fits(
        item: str,
        fits: pd.DataFrame, 
        tiles: pd.DataFrame, 
        freqs: NDArray[np.float_],
        soln_xx: NDArray[np.complex_],
        soln_yy: NDArray[np.complex_],
        weights: NDArray[np.float_],
    ) -> pd.DataFrame:
    import matplotlib as mpl
    figsize = (20, 36) 
    mpl.rcParams['figure.figsize'] = figsize
    from matplotlib import pyplot as plt
    from matplotlib.axes import Axes
    fits = pd.merge(
        fits[fits["pol"] == "XX"].drop(columns=["pol", "soln_idx"]), 
        fits[fits["pol"] == "YY"].drop(columns=["pol"]), 
        on=["tile_id"], suffixes=["_xx", "_yy"]
    )
    fits = pd.merge(fits, tiles, left_on="tile_id", right_on="id")
    tile_columns = tiles.columns
    fit_columns = [column for column in fits.columns if column not in tile_columns]
    fit_columns = sorted([*set(fit_columns).difference(tile_columns)])
    fits = pd.concat([fits[tile_columns], fits[fit_columns]], axis=1)
    weights = weights ** 2

    fits.to_csv(os.path.join(item, 'phase_fits.tsv'), sep='\t')

    rxs = np.sort(np.unique(fits["rx"]))
    slots = np.sort(np.unique(fits["slot"]))

    for pol, soln in zip(['xx', 'yy'], [soln_xx, soln_yy]):
        plt.clf()
        fig, axs = plt.subplots(len(rxs), len(slots), sharex=True, sharey='row', squeeze=True)
        for ax in axs.flatten():
            ax.axis('off')
        for _, fit in fits.iterrows():
            signal = soln[fit['soln_idx']]  # type: ignore
            if fit['flag'] or np.isnan(signal).all():
                continue
            mask = np.where(np.logical_and(
                np.isfinite(signal),
                weights > 0
            ))[0]
            angle = np.angle(signal)  # type: ignore
            mask_freq: ArrayLike = freqs[mask]  # type: ignore
            model_freqs = np.linspace(mask_freq.min(), mask_freq.max(), len(freqs))  # type: ignore
            
            rx_idx = np.where(rxs == fit['rx'])[0][0]
            slot_idx = np.where(slots == fit['slot'])[0][0]
            ax: Axes = axs[rx_idx][slot_idx]  # type: ignore
            ax.axis('on')
            # ax.title.set_size(8)
            gradient = (2 * np.pi * u.rad * (fit[f'length_{pol}'] * u.m) / c).to(u.rad/u.Hz).value
            intercept = fit[f'intercept_{pol}'] 

            # model = gradient * mask_freq + intercept
            # ax.scatter(mask_freq, wrap_angle(model), c='red', s=1)
            model = gradient * model_freqs + intercept
            ax.scatter(model_freqs, wrap_angle(model), c='red', s=1)
            
            # ax.scatter(mask_freq, wrap_angle(angle[mask]), c='blue', s=1)
            mask_weights: ArrayLike = weights[mask] # type: ignore
            # mask_angle = angle[mask]
            ax.scatter(mask_freq, wrap_angle(angle[mask]), c=mask_weights, cmap='Greens', s=1)
            # mask_unwrapped = np.unwrap(mask_angle)
            # ax.scatter(mask_freq, mask_unwrapped, c=mask_weights, cmap='Blues', s=1)
            # ax.scatter(mask_freq, wrap_angle(mask_unwrapped), c='blue', s=1)
            # fit['name']
            ax.set_title('\n'.join([
                f"{fit['name']}|{fit['soln_idx']}",
                # f"X{fit[f'chi2dof_{pol}']:.2e}",
                f"S{fit[f'sigma_resid_{pol}']:.2e}",
                # f"Q: {fit[f'quality_{pol}']:.2f}",
            ])) # |{fit['id']}

        # fig.set_size_inches(*figsize)
        plt.tight_layout()
        fig.savefig(os.path.join(item, f'phase_fits_{pol}.png'), dpi=300, bbox_inches='tight')

    return fits


# Greg: Here starts my WIP for trying to emulate Marcin's code
# If you uncomment below blocks of code you'll need to import struct, namedtuple and get_mwax_mover_version_string


# def read_cal_solutions(logger, obs_id, metafits_filename, solution_bin_file):
#     """Reads in a single aocal (.bin) calibration solution and returns
#     a list of tuples containing the cal solution info:
#     """
#     tile_info = []

#     # Get tile info from metafits
#     with fits.open(metafits_filename) as metafits_hdul:
#         metafits_tiles = metafits_hdul[1].data  # pylint: disable=no-member

#         for metafits_tile in metafits_tiles:
#             tile = Tile()
#             tile.tile_name = metafits_tile["TileName"]
#             tile.tile_id = metafits_tile["Tile"]
#             tile.flag = metafits_tile["Flag"]
#             tile.tile_index = metafits_tile["Antenna"]

#             tile_info.append(tile)

#     # Loop through the bin file and get info per tile and pol (xx,xy,yx,yy)
#     Header = namedtuple(
#         "header",
#         (
#             "intro fileType structureType intervalCount antennaCount"
#             " channelCount polarizationCount timeStart timeEnd"
#         ),
#     )
#     HEADER_INTRO = "MWAOCAL\0"
#     HEADER_FORMAT = "8s6I2d"
#     HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
#     Header.__new__.__defaults__ = (HEADER_INTRO, 0, 0, 0, 0, 0, 0, 0.0, 0.0)

#     with open(solution_bin_file, "rb") as cal_file:
#         header_string = cal_file.read(struct.calcsize(HEADER_FORMAT))
#         header = Header._make(struct.unpack(HEADER_FORMAT, header_string))
#         assert header.intro == HEADER_INTRO, "File is not a calibrator file"
#         assert header.fileType == 0, (
#             "fileType not recognised. Only 0 (complex Jones solutions) is"
#             " recognised in mwatools/solutionfile.h as of 2013-08-30"
#         )
#         assert header.structureType == 0, (
#             "structureType not recognised. Only 0 (ordered real/imag,"
#             " polarization, channel, antenna, time) is recognised in"
#             " mwatools/solutionfile.h as of 2013-08-30"
#         )
#         count = (
#             header.intervalCount
#             * header.antennaCount
#             * header.channelCount
#             * header.polarizationCount
#         )
#         assert os.path.getsize(
#             solution_bin_file
#         ) == HEADER_SIZE + 2 * count * struct.calcsize(
#             "d"
#         ), "File is the wrong size."


# def get_calibration_fits(logger, db_handler, obs_id) -> bool:
#     """Inserts a calibration fits row"""
#     success = True
#     fit_id = time.time()
#     code_version = get_mwax_mover_version_string()

#     insert_calibration_fits_row(
#         db_handler, fit_id, obs_id, code_version, "mwax_calvin_processor"
#     )
#     return success


# def get_calibration_solutions(
#     logger, db_handler, obs_id, fit_id, solution_bin_files
# ) -> bool:
#     """Given calibration bin files for an obsid,
#     generate the data needed for the calibration_solution rows.
#     """
#     tiles = [
#         1,
#     ]

#     for tile_id in tiles:
#         pass
#         # insert_calibration_solutions_row(
#         #     db_handler,
#         #     fit_id,
#         #     obs_id,
#         #     tile_id,
#         #     x_delay_m,
#         #     x_intercept,
#         #     x_gains,
#         #     y_delay_m,
#         #     y_intercept,
#         #     y_gains,
#         #     x_gains_pol1,
#         #     y_gains_pol1,
#         #     x_phase_sigma_resid,
#         #     x_phase_chi2dof,
#         #     x_phase_fit_quality,
#         #     y_phase_sigma_resid,
#         #     y_phase_chi2dof,
#         #     y_phase_fit_quality,
#         #     x_gains_fit_quality,
#         #     y_gains_fit_quality,
#         #     x_gains_sigma_resid,
#         #     y_gains_sigma_resid,
#         #     x_gains_pol0,
#         #     y_gains_pol0,
#         # )
#     success = True
#     return success


#
# Here Ends my WIP for trying to emulate Marcin's code
#


def get_convergence_summary(solutions_fits_file: str):
    """Returns a list of tuples which represent a summary
    of the convergence of the solutions"""
    soln = HyperfitsSolution(solutions_fits_file)
    convergence_results = soln.get_convergence_results()
    converged_channel_indices = np.where(~np.isnan(convergence_results))
    summary = []
    summary.append(("Total number of channels", len(convergence_results)))
    summary.append(
        (
            "Number of converged channels",
            f"{len(converged_channel_indices[0])}",
        )
    )
    summary.append(
        (
            "Fraction of converged channels",
            (
                f" {len(converged_channel_indices[0]) / len(convergence_results) * 100}%"
            ),
        )
    )
    summary.append(
        (
            "Average channel convergence",
            f" {np.mean(convergence_results[converged_channel_indices])}",
        )
    )
    return summary


def write_stats(
    logger,
    obs_id,
    stats_path,
    stats_filename,
    hyperdrive_solution_filename,
    hyperdrive_binary_path,
    metafits_filename,
) -> Tuple[bool, str]:
    """This method produces convergence stats and plots
    Returns:
    bool = Success/fail
    str  = Error message if fail"""
    logger.info(
        f"{obs_id} Writing stats for {hyperdrive_solution_filename}..."
    )

    try:
        conv_summary_list = get_convergence_summary(
            hyperdrive_solution_filename
        )

        with open(stats_filename, "w", encoding="UTF-8") as stats:
            for row in conv_summary_list:
                stats.write(f"{row[0]}: {row[1]}\n")

        # Now run hyperdrive again to do some plots
        cmd = (
            f"{hyperdrive_binary_path} solutions-plot -m"
            f" {metafits_filename} {hyperdrive_solution_filename}"
        )

        return_value, _ = run_command_ext(
            logger, cmd, -1, timeout=10, use_shell=False
        )

        logger.debug(
            f"{obs_id} Finished running hyperdrive stats on"
            f" {hyperdrive_solution_filename}. Return={return_value}"
        )
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    try:
        if return_value:
            # Currently, hyperdrive writes the solution files to same dir as
            # the current directory mwax_calvin_processor is run from
            # Move them to the processing_complete dir
            plot_filename_base = os.path.basename(
                f"{os.path.splitext(hyperdrive_solution_filename)[0]}"
            )

            amps_plot_filename = f"{plot_filename_base}_amps.png"
            phase_plot_filename = f"{plot_filename_base}_phases.png"
            shutil.move(
                os.path.join(os.getcwd(), amps_plot_filename),
                os.path.join(stats_path, f"{amps_plot_filename}"),
            )
            shutil.move(
                os.path.join(os.getcwd(), phase_plot_filename),
                os.path.join(stats_path, f"{phase_plot_filename}"),
            )
            logger.debug(f"{obs_id} plots moved successfully to {stats_path}.")

            logger.info(f"{obs_id} write_stats() complete successfully")
        else:
            logger.debug(f"{obs_id} write_stats() failed")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


def write_readme_file(logger, filename, cmd, exit_code, stdout, stderr):
    """This will create a small readme.txt file which will
    hopefully help whoever poor sap is checking why birli
    or hyperdrive did or did not work!"""
    try:
        with open(filename, "w", encoding="UTF-8") as readme:
            if exit_code == 0:
                readme.write(
                    "This run succeded at:"
                    f" {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
                )
            else:
                readme.write(
                    "This run failed at:"
                    f" {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
                )
            readme.write(f"Command: {cmd}")
            readme.write(f"Exit code: {exit_code}\n")
            readme.write(f"stdout: {stdout}\n")
            readme.write(f"stderr: {stderr}\n")

    except Exception:
        logger.warning(
            (
                f"Could not write text file {filename} describing the"
                " problem observation."
            ),
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
    try:
        # Get only data files
        data_files = glob.glob(os.path.join(processing_dir, "*.fits"))
        # Remove the metafits (we specify it seperately)
        data_files.remove(metafits_filename)

        data_file_arg = ""
        for data_file in data_files:
            data_file_arg += f"{data_file} "

        # Run birli
        cmdline = (
            f"{processor.birli_binary_path}"
            f" --metafits {metafits_filename}"
            " --no-draw-progress"
            f" --uvfits-out={uvfits_filename}"
            f" --flag-edge-width={80}"
            f" {data_file_arg}"
        )

        start_time = time.time()

        processor.birli_popen_process = run_command_popen(
            processor.logger, cmdline, -1, False
        )

        exit_code, stdout, stderr = check_popen_finished(
            processor.logger,
            processor.birli_popen_process,
            processor.birli_timeout,
        )

        # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
        elapsed = time.time() - start_time

        if exit_code == 0:
            ## Success!
            processor.logger.info(
                f"{obs_id}: Birli run successful in {elapsed:.3f} seconds"
            )
            birli_success = True
            processor.birli_popen_process = None

            ## Success!
            # Write out a useful file of command line info
            readme_filename = os.path.join(
                processing_dir, f"{obs_id}_birli_readme.txt"
            )
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
                f"{obs_id}: Birli run FAILED: Exit code of {exit_code} in"
                f" {elapsed:.3f} seconds: {stderr}"
            )
    except Exception as hyperdrive_run_exception:
        processor.logger.error(
            f"{obs_id}: hyperdrive run FAILED: Unhandled exception"
            f" {hyperdrive_run_exception} in {elapsed:.3f} seconds:"
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
            error_path = os.path.join(processor.processing_error_path, obs_id)
            processor.logger.info(
                f"{obs_id}: moving failed files to {error_path} for manual"
                " analysis and writing readme_error.txt"
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
) -> int:
    """Runs hyperdrive N times and returns number of successful runs"""
    processor.logger.info(
        f"{obs_id}: {len(uvfits_files)} contiguous bands detected."
        f" Running hyperdrive {len(uvfits_files)} times...."
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
                f" {uvfits_file} {metafits_filename} --num-sources 5"
                " --source-list"
                f" {processor.source_list_filename} --source-list-type"
                f" {processor.source_list_type} --outputs"
                f" {hyperdrive_solution_filename} {bin_solution_filename}"
            )

            start_time = time.time()

            # run hyperdrive
            processor.hyperdrive_popen_process = run_command_popen(
                processor.logger, cmdline, -1, False
            )

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

                ## Success!
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
            error_path = os.path.join(processor.processing_error_path, obs_id)
            processor.logger.info(
                f"{obs_id}: moving failed files to {error_path} for manual"
                " analysis and writing readme_error.txt"
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
            processing_dir,
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
        processor.logger.info(
            f"{obs_id}: All {stats_successful} hyperdrive stats"
            " runs successful"
        )
        return True
    else:
        processor.logger.warning(
            f"{obs_id}: Not all hyperdrive stats runs were successful."
        )
        return False
