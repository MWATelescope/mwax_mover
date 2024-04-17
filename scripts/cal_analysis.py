#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
from argparse import ArgumentParser, FileType
import sys
import shlex

from mwax_mover.mwax_calvin_utils import (
    Metafits, HyperfitsSolution, HyperfitsSolutionGroup, Tile, fit_phase_line, PhaseFitInfo,
    debug_phase_fits
)

def parse_args(argv=None):
    parser = ArgumentParser(
        description="Analyse calibration solutions")

    parser.add_argument( "--metafits", type=str, nargs='+' )
    parser.add_argument( "--solns", type=str, nargs='+' )
    parser.add_argument( "--name", type=str)
    parser.add_argument( "--out-dir", type=str, default='.')
    parser.add_argument( "--plot-residual", default=False, action='store_true')
    parser.add_argument( "--residual-vmax", default=None)
    parser.add_argument( "--phase-diff-path", default=None)
    return parser.parse_args(argv)

def main():
    show = False
    if len(sys.argv) > 1:
        args = parse_args()
    else:
        # is being called directly from nextflow with args ${args}
        args = parse_args(shlex.split('${argstr}'))
    soln_group = HyperfitsSolutionGroup(
        [Metafits(f) for f in args.metafits],
        [HyperfitsSolution(f) for f in args.solns]
    )
    print(vars(args))
    obsids = np.array(soln_group.obsids)

    min_obsid=obsids.min()
    max_obsid=obsids.max()
    if min_obsid != max_obsid:
        title = f"{min_obsid}-{max_obsid}"
    else:
        title = f"{min_obsid}"
    calibrator = soln_group.calibrator
    if calibrator:
        title += f" {calibrator}"
    if args.name:
        title += f" {args.name}"


    tiles = soln_group.metafits_tiles_df
    refant_name = soln_group.refant['name']
    # print(f"{refant=}")
    chanblocks_hz = np.concatenate(soln_group.all_chanblocks_hz)
    # print(f"{len(chanblocks_hz)=}")
    soln_tile_ids, all_xx_solns, all_yy_solns = soln_group.get_solns(refant_name)
    # matrix plot of phase angle vs frequency
    weights = soln_group.weights

    phase_fit_niter = 1

    phase_fits = []
    # by default we don't want to apply any phase rotation.
    phase_diff = np.full((len(chanblocks_hz),), 1.0, dtype=np.complex128)
    if args.phase_diff_path is not None and os.path.exists(args.phase_diff_path):
        # phase_diff_raw is an array, first column is frequency, second column is phase difference
        phase_diff_raw = np.loadtxt(args.phase_diff_path)
        for i, chanblock_hz in enumerate(chanblocks_hz):
            # find the closest frequency in phase_diff_raw
            idx = np.abs(phase_diff_raw[:,0] - chanblock_hz).argmin()
            diff = phase_diff_raw[idx,1]
            phase_diff[i] = np.exp(-1j * diff)
    else:
        print(f"not applying phase correction")

    for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
        tile: Tile = tiles[tiles.id == tile_id].iloc[0]
        for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
            if tile.flavor.endswith("-NI"):
                solns *= phase_diff

            try:
                fit = fit_phase_line(chanblocks_hz, solns, weights, niter=phase_fit_niter)  # type: ignore
            except Exception as exc:
                print(f"{tile_id=:4} ({tile.name}) {pol} {exc}")
                continue
            phase_fits.append([tile_id, soln_idx, pol, *fit])

    phase_fits = pd.DataFrame(phase_fits, columns=["tile_id", "soln_idx", "pol", *PhaseFitInfo._fields])  # type: ignore

    if not len(phase_fits):
        return

    phase_fits = debug_phase_fits(
        phase_fits, tiles, chanblocks_hz, all_xx_solns[0], all_yy_solns[0], weights,
        prefix=f'{args.out_dir}/{title} ', show=show, title=title,
        plot_residual=args.plot_residual, residual_vmax=args.residual_vmax
    )  # type: ignore

if __name__ == '__main__':
    main()