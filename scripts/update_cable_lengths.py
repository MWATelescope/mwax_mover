#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
from argparse import ArgumentParser, FileType
import sys
import shlex
from astropy.io import fits

from mwax_mover.mwax_calvin_utils import (
    Metafits, Tile, fit_phase_line, PhaseFitInfo,
    debug_phase_fits
)

def parse_args(argv=None):
    parser = ArgumentParser(
        description="Analyse calibration solutions")

    parser.add_argument( "--metafits", type=str )
    parser.add_argument( "--name", type=str, default=None, help="Name used to find phase_fits.tsv")
    parser.add_argument( "--tsv", type=str, default=None, help="Direct path to phase_fits.tsv")
    parser.add_argument( "--out-dir", type=str, default='.')
    parser.add_argument( "--mwax", default=False, action='store_true')
    return parser.parse_args(argv)

def main():
    if len(sys.argv) > 1:
        args = parse_args()
    else:
        # is being called directly from nextflow with args ${args}
        args = parse_args(shlex.split('${argstr}'))
    mf = Metafits(args.metafits)
    obsid = mf.obsid
    calibrator = mf.calibrator
    title = f"{obsid}"
    if calibrator:
        title += f" {calibrator}"
    if args.name:
        title += f" {args.name}"

    inputs = mf.inputs_df
    print("inputs\n", inputs)
    if args.tsv:
        phase_fits_tsv = args.tsv
    else:
        phase_fits_tsv = f'{args.out_dir}/{title} phase_fits.tsv'
    if not os.path.exists(phase_fits_tsv):
        raise UserWarning(f"phase_fits_tsv {phase_fits_tsv} does not exist")
    phase_fits = pd.read_csv(phase_fits_tsv, sep='\t')
    print("phase_fits\n", phase_fits)
    with fits.open(args.metafits) as hdus:
        metafits_inputs = hdus['TILEDATA'].data  # type: ignore
        print(f"{type(metafits_inputs)=}")
        print(f"{metafits_inputs.columns=}")
        print("metafits_inputs\n", metafits_inputs["TileName"])
        print("metafits_inputs\n", metafits_inputs["Pol"])
        # xpols = np.where(metafits_inputs["Pol"] == "X")
        # ypols = np.where(metafits_inputs["Pol"] == "X")

        for name, length_xx, length_yy in zip(phase_fits.name, phase_fits.length_xx, phase_fits.length_yy):
            input_x = inputs.iloc[np.where(inputs.name == f"{name}X")[0]]
            input_y = inputs.iloc[np.where(inputs.name == f"{name}Y")[0]]
            id_x = np.logical_and(metafits_inputs["TileName"] == name, metafits_inputs["Pol"] == "X")[0]
            id_y = np.logical_and(metafits_inputs["TileName"] == name, metafits_inputs["Pol"] == "Y")[0]
            # id_x = input_x.id.values[0]
            # id_y = input_y.id.values[0]
            if args.mwax:
                length_x = 0
                length_y = 0
            else:
                length_x = input_x.length.values[0]
                length_y = input_y.length.values[0]
            print(f"{name}\t{length_xx:9.2f}\t{length_yy:9.2f}\t{id_x:3}\t{id_y:3}\t{length_x:9.2f}\t{length_y:9.2f}")
            metafits_inputs['Length'][id_x] = f"EL_{length_xx + length_x:.2f}"
            metafits_inputs['Length'][id_y] = f"EL_{length_yy + length_y:.2f}"

        metafits_path, ext = os.path.splitext(args.metafits)
        hdus.writeto(f"{metafits_path}_fixed{ext}", overwrite=True)

if __name__ == '__main__':
    main()