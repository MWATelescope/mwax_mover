This run succeded at: 14-01-2026 22:40:08
Command: /software/hyperdrive/target/release/hyperdrive di-calibrate --no-progress-bars --data /tmp/calvin/jobs/3692_1452436432/1452436432.uvfits /data/calvin/jobs/3692_1452436432/1452436432_metafits.fits --num-sources 99 --source-list /home/mwa/srclists/GGSM_updated.fits --source-list-type fits --outputs /data/calvin/jobs/3692_1452436432/1452436432_solutions.fits /data/calvin/jobs/3692_1452436432/1452436432_solutions.bin
Exit code: 0
stdout: [2026-01-14T14:38:39Z INFO ] hyperdrive di-calibrate 0.6.1
[2026-01-14T14:38:39Z INFO ] Compiled on git commit hash: e18fdb4
[2026-01-14T14:38:39Z INFO ]             git head ref: refs/heads/main
[2026-01-14T14:38:39Z INFO ]             Tue, 14 Oct 2025 23:26:52 +0000
[2026-01-14T14:38:39Z INFO ]          with compiler rustc 1.89.0 (29483883e 2025-08-04)
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:39Z INFO ] DI calibrating obsid 1452436432
[2026-01-14T14:38:39Z INFO ] ├ from uvfits /tmp/calvin/jobs/3692_1452436432/1452436432.uvfits
[2026-01-14T14:38:39Z INFO ] │ with metafits /data/calvin/jobs/3692_1452436432/1452436432_metafits.fits
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:39Z INFO ] Coordinates
[2026-01-14T14:38:39Z INFO ] ├                    RA        Dec
[2026-01-14T14:38:39Z INFO ] │ Phase centre:       96.7913°  -5.8850° (J2000)
[2026-01-14T14:38:39Z INFO ] │ Pointing centre:    95.9089°  -5.6603°
[2026-01-14T14:38:39Z INFO ] ├ Array position:    116.6708° -26.7033° 377.8270m
[2026-01-14T14:38:39Z INFO ] │                    Longitude Latitude  Height
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:39Z INFO ] Tile info
[2026-01-14T14:38:39Z INFO ] ├ 128 total
[2026-01-14T14:38:39Z INFO ] │ 128 unflagged
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:39Z INFO ] Time info
[2026-01-14T14:38:39Z INFO ] ├ Resolution: 2 s
[2026-01-14T14:38:39Z INFO ] │ No reader averaging
[2026-01-14T14:38:39Z INFO ] ├ First obs timestamp: 2026-01-14T14:33:35 UTC | 1452436433.00
[2026-01-14T14:38:39Z INFO ] ├ Available timesteps: [0..60)
[2026-01-14T14:38:39Z INFO ] │ Unflagged timesteps: [0..60)
[2026-01-14T14:38:39Z INFO ] ├ Using timesteps:     [0..60)
[2026-01-14T14:38:39Z INFO ] │ First timestamp:     2026-01-14T14:33:35 UTC (+0.00s)
[2026-01-14T14:38:39Z INFO ] │ Last timestamp :     2026-01-14T14:35:33 UTC (+118.00s)
[2026-01-14T14:38:39Z INFO ] │ First LMST: 88.876895° (J2000)
[2026-01-14T14:38:39Z INFO ] └ DUT1: 0.0000000000 s
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:39Z INFO ] Channel info
[2026-01-14T14:38:39Z INFO ] ├ Resolution: 40.00 kHz
[2026-01-14T14:38:39Z INFO ] │ No Reader averaging
[2026-01-14T14:38:39Z INFO ] ├ Total number of fine channels:     768
[2026-01-14T14:38:39Z INFO ] │ Number of unflagged fine channels: 768
[2026-01-14T14:38:39Z INFO ] ├ Number of fine chans per coarse channel: 32
[2026-01-14T14:38:39Z INFO ] ├ First fine-channel:           167.040 MHz
[2026-01-14T14:38:39Z INFO ] │ Last fine-channel:            197.720 MHz
[2026-01-14T14:38:39Z INFO ] ├ First unflagged fine-channel: 167.040 MHz
[2026-01-14T14:38:39Z INFO ] │ Last unflagged fine-channel:  197.720 MHz
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:39Z INFO ] Beam info
[2026-01-14T14:38:39Z INFO ] ├ Type: FEE
[2026-01-14T14:38:39Z INFO ] ├ Using dead dipole information (42 tiles affected)
[2026-01-14T14:38:39Z INFO ] ├ Ideal dipole delays: [ 9 10 11 12
[2026-01-14T14:38:39Z INFO ] │                        6  7  8  9
[2026-01-14T14:38:39Z INFO ] │                        3  4  5  6
[2026-01-14T14:38:39Z INFO ] │                        0  1  2  3]
[2026-01-14T14:38:39Z INFO ] └ File: /software/hyperdrive/mwa_full_embedded_element_pattern.h5
[2026-01-14T14:38:39Z INFO ] 
[2026-01-14T14:38:40Z INFO ] Sky- and beam-modelling info
[2026-01-14T14:38:40Z INFO ] ├ Using GPU with double precision
[2026-01-14T14:38:40Z INFO ] │ CUDA device: NVIDIA A40 (capability 8.6, 45490 MiB)
[2026-01-14T14:38:40Z INFO ] │ CUDA driver: 13.1, runtime: 12.9
[2026-01-14T14:38:40Z INFO ] 
[2026-01-14T14:39:12Z INFO ] Sky model info
[2026-01-14T14:39:12Z INFO ] ├ Source list contains 386408 sources
[2026-01-14T14:39:12Z INFO ] │ (386408 components, 293880 points, 92528 Gaussians, 0 shapelets)
[2026-01-14T14:39:12Z INFO ] ├ Using 99 sources with a total of 99 components
[2026-01-14T14:39:12Z INFO ] │ 89 points, 10 Gaussians, 0 shapelets
[2026-01-14T14:39:12Z INFO ] 
[2026-01-14T14:39:12Z INFO ] DI calibration set up
[2026-01-14T14:39:12Z INFO ] ├ 1 calibration timeblocks, 768 calibration chanblocks
[2026-01-14T14:39:12Z INFO ] │ 60 timesteps per timeblock
[2026-01-14T14:39:12Z INFO ] ├ Calibrating with 5917 of 8128 baselines
[2026-01-14T14:39:12Z INFO ] │ Minimum UVW cutoff: 50λ (82.189m)
[2026-01-14T14:39:12Z INFO ] │ Maximum UVW cutoff: ∞
[2026-01-14T14:39:12Z INFO ] │ (Used obs. centroid frequency 182.38 MHz to convert lambdas to metres)
[2026-01-14T14:39:12Z INFO ] ├ Chanblocks will stop iterating
[2026-01-14T14:39:12Z INFO ] │ - when the iteration difference is less than 1e-8 (stop threshold)
[2026-01-14T14:39:12Z INFO ] │ - or after 50 iterations.
[2026-01-14T14:39:12Z INFO ] │ Chanblocks with an iteration diff. less than 1e-4 are considered converged (min. threshold)
[2026-01-14T14:39:12Z INFO ] └ Writing calibration solutions to: /data/calvin/jobs/3692_1452436432/1452436432_solutions.fits, /data/calvin/jobs/3692_1452436432/1452436432_solutions.bin
[2026-01-14T14:39:12Z INFO ] 
[2026-01-14T14:39:15Z INFO ] Reading input data and sky modelling
[2026-01-14T14:39:58Z INFO ] Finished reading input data and sky modelling
Chanblock 732: converged (16): 1e-8 > 6.04061e-9
Chanblock 426: converged (16): 1e-8 > 8.63623e-9
Chanblock 672: converged (16): 1e-8 > 7.22447e-9
Chanblock 720: converged (16): 1e-8 > 5.78698e-9
Chanblock  90: converged (16): 1e-8 > 8.59883e-9
Chanblock 414: converged (16): 1e-8 > 8.83918e-9
Chanblock  24: converged (18): 1e-8 > 4.96980e-9
Chanblock  93: converged (16): 1e-8 > 9.31315e-9
Chanblock  84: converged (16): 1e-8 > 7.45378e-9
Chanblock 423: converged (16): 1e-8 > 7.62324e-9
Chanblock 336: converged (16): 1e-8 > 2.02238e-9
Chanblock 252: converged (16): 1e-8 > 5.24887e-9
Chanblock 420: converged (16): 1e-8 > 2.41143e-9
Chanblock 648: converged (16): 1e-8 > 4.15145e-9
Chanblock 408: converged (16): 1e-8 > 2.79757e-9
Chanblock 164: converged (16): 1e-8 > 5.83897e-9
Chanblock  91: converged (16): 1e-8 > 7.24449e-9
Chanblock 422: converged (16): 1e-8 > 6.03121e-9
Chanblock 144: converged (16): 1e-8 > 4.34809e-9
Chanblock 157: converged (16): 1e-8 > 5.05175e-9
Chanblock 738: converged (16): 1e-8 > 4.91701e-9
Chanblock   3: converged (18): 1e-8 > 6.25087e-9
Chanblock  78: converged (18): 1e-8 > 5.78712e-9
Chanblock   0: converged (18): 1e-8 > 9.79857e-9
Chanblock 264: converged (16): 1e-8 > 1.47967e-9
Chanblock  88: converged (18): 1e-8 > 5.55424e-9
Chanblock   9: converged (18): 1e-8 > 8.87731e-9
Chanblock  49: converged (18): 1e-8 > 6.84906e-9
Chanblock 480: converged (16): 1e-8 > 5.80065e-9
Chanblock 163: converged (16): 1e-8 > 3.76878e-9
Chanblock 216: converged (16): 1e-8 > 3.22002e-9
Chanblock 744: converged (16): 1e-8 > 6.95359e-9
Chanblock 313: converged (16): 1e-8 > 6.62169e-9
Chanblock 327: converged (16): 1e-8 > 4.36070e-9
Chanblock 627: converged (16): 1e-8 > 2.30185e-9
Chanblock 325: converged (16): 1e-8 > 2.80911e-9
Chanblock 168: converged (16): 1e-8 > 7.59014e-9
Chanblock 330: converged (16): 1e-8 > 5.57272e-9
Chanblock 432: converged (16): 1e-8 > 6.09944e-9
Chanblock 630: converged (16): 1e-8 > 2.77876e-9
Chanblock 421: converged (16): 1e-8 > 3.31590e-9
Chanblock  81: converged (16): 1e-8 > 2.64118e-9
Chanblock 636: converged (16): 1e-8 > 4.52969e-9
Chanblock 240: converged (16): 1e-8 > 6.27501e-9
Chanblock 576: converged (16): 1e-8 > 8.28731e-9
Chanblock  15: converged (18): 1e-8 > 7.61137e-9
Chanblock 427: converged (16): 1e-8 > 4.92695e-9
Chanblock 733: converged (16): 1e-8 > 5.44426e-9
Chanblock 384: converged (16): 1e-8 > 5.82099e-9
Chanblock  87: converged (18): 1e-8 > 9.38694e-9
Chanblock 696: converged (18): 1e-8 > 7.07381e-9
Chanblock 324: converged (16): 1e-8 > 5.72281e-9
Chanblock 411: converged (18): 1e-8 > 9.70026e-9
Chanblock  77: converged (18): 1e-8 > 4.97307e-9
Chanblock 624: converged (26): 1e-8 > 4.11709e-9
Chanblock  75: converged (20): 1e-8 > 9.91006e-9
Chanblock 721: converged (16): 1e-8 > 2.62620e-9
Chanblock 415: converged (16): 1e-8 > 3.49799e-9
Chanblock 673: converged (16): 1e-8 > 4.33800e-9
Chanblock 428: converged (16): 1e-8 > 6.10423e-9
Chanblock 734: converged (16): 1e-8 > 4.70558e-9
Chanblock 337: converged (16): 1e-8 > 3.48454e-9
Chanblock 741: converged (16): 1e-8 > 3.78174e-9
Chanblock  85: converged (16): 1e-8 > 7.66957e-9
Chanblock 649: converged (16): 1e-8 > 6.14007e-9
Chanblock 409: converged (16): 1e-8 > 6.66056e-9
Chanblock 424: converged (16): 1e-8 > 5.37343e-9
Chanblock 145: converged (16): 1e-8 > 4.52541e-9
Chanblock 684: converged (16): 1e-8 > 4.93686e-9
Chanblock  25: converged (18): 1e-8 > 4.58803e-9
Chanblock 739: converged (16): 1e-8 > 3.15986e-9
Chanblock 756: converged (18): 1e-8 > 5.12819e-9
Chanblock 396: converged (16): 1e-8 > 2.55536e-9
Chanblock  10: converged (18): 1e-8 > 3.18338e-9
Chanblock 631: converged (16): 1e-8 > 3.47949e-9
Chanblock 265: converged (16): 1e-8 > 2.19216e-9
Chanblock  89: converged (18): 1e-8 > 5.86395e-9
Chanblock  95: converged (18): 1e-8 > 9.55843e-9
Chanblock 735: converged (16): 1e-8 > 1.40383e-9
Chanblock 481: converged (16): 1e-8 > 3.36040e-9
Chanblock 326: converged (16): 1e-8 > 3.37589e-9
Chanblock 433: converged (16): 1e-8 > 2.68566e-9
Chanblock  16: converged (18): 1e-8 > 9.40375e-9
Chanblock 637: converged (16): 1e-8 > 3.28767e-9
Chanblock 577: converged (16): 1e-8 > 6.14331e-9
Chanblock 722: converged (16): 1e-8 > 4.06419e-9
Chanblock 416: converged (16): 1e-8 > 4.42820e-9
Chanblock 314: converged (16): 1e-8 > 7.88768e-9
Chanblock 745: converged (16): 1e-8 > 4.05877e-9
Chanblock 328: converged (16): 1e-8 > 3.65828e-9
Chanblock 331: converged (16): 1e-8 > 3.23815e-9
Chanblock 169: converged (16): 1e-8 > 7.12756e-9
Chanblock 697: converged (16): 1e-8 > 5.52728e-9
Chanblock 385: converged (16): 1e-8 > 6.17381e-9
Chanblock 412: converged (16): 1e-8 > 2.68519e-9
Chanblock  57: converged (50): 1e-4 > 7.75882e-8 > 1e-8
Chanblock 120: converged (50): 1e-4 > 1.11455e-8 > 1e-8
Chanblock 318: converged (50): 1e-4 > 1.38251e-8 > 1e-8
Chanblock 159: converged (50): 1e-4 > 2.29837e-8 > 1e-8
Chanblock  50: converged (50): 1e-4 > 3.24050e-8 > 1e-8
Chanblock  82: converged (18): 1e-8 > 4.66125e-9
Chanblock  60: converged (50): 1e-4 > 2.76934e-8 > 1e-8
Chanblock 742: converged (16): 1e-8 > 6.05580e-9
Chanblock 429: converged (16): 1e-8 > 3.62098e-9
Chanblock 417: converged (16): 1e-8 > 3.22810e-9
Chanblock 743: converged (16): 1e-8 > 8.22994e-9
Chanblock 217: converged (20): 1e-8 > 9.84126e-9
Chanblock 333: converged (16): 1e-8 > 1.35836e-9
Chanblock 674: converged (16): 1e-8 > 2.82606e-9
Chanblock 628: converged (24): 1e-8 > 6.95636e-9
Chanblock 625: converged (26): 1e-8 > 3.61276e-9
Chanblock 165: converged (50): 1e-4 > 2.80228e-8 > 1e-8
Chanblock  72: converged (50): 1e-4 > 1.49681e-8 > 1e-8
Chanblock 410: converged (16): 1e-8 > 3.53295e-9
Chanblock 650: converged (16): 1e-8 > 2.59128e-9
Chanblock 146: converged (16): 1e-8 > 9.87556e-9
Chanblock 204: converged (50): 1e-4 > 1.11682e-8 > 1e-8
Chanblock 685: converged (16): 1e-8 > 7.40705e-9
Chanblock 338: converged (16): 1e-8 > 4.79290e-9
Chanblock 740: converged (16): 1e-8 > 3.20913e-9
Chanblock 162: converged (50): 1e-4 > 2.47542e-8 > 1e-8
Chanblock 425: converged (16): 1e-8 > 2.65730e-9
Chanblock  54: converged (50): 1e-4 > 5.69782e-8 > 1e-8
Chanblock 290: converged (50): 1e-4 > 1.22432e-8 > 1e-8
Chanblock 397: converged (16): 1e-8 > 2.57292e-9
Chanblock 156: converged (50): 1e-4 > 1.10524e-8 > 1e-8
Chanblock  18: converged (50): 1e-4 > 1.07721e-8 > 1e-8
Chanblock 288: converged (50): 1e-4 > 1.08422e-8 > 1e-8
Chanblock  26: converged (18): 1e-8 > 4.40038e-9
Chanblock 294: converged (50): 1e-4 > 2.40904e-8 > 1e-8
Chanblock 360: converged (16): 1e-8 > 2.68835e-9
Chanblock 757: converged (16): 1e-8 > 5.25193e-9
Chanblock  86: converged (18): 1e-8 > 9.80971e-9
Chanblock 632: converged (16): 1e-8 > 2.98222e-9
Chanblock 266: converged (16): 1e-8 > 5.82684e-9
Chanblock 736: converged (16): 1e-8 > 7.41949e-9
Chanblock 660: converged (16): 1e-8 > 4.53884e-9
Chanblock 482: converged (16): 1e-8 > 4.75219e-9
Chanblock  51: converged (50): 1e-4 > 4.95857e-8 > 1e-8
Chanblock 434: converged (16): 1e-8 > 4.34255e-9
Chanblock 578: converged (16): 1e-8 > 4.30400e-9
Chanblock  12: converged (50): 1e-4 > 2.59109e-8 > 1e-8
Chanblock 638: converged (16): 1e-8 > 1.84082e-9
Chanblock 413: converged (16): 1e-8 > 6.56884e-9
Chanblock  79: converged (50): 1e-4 > 1.47155e-8 > 1e-8
Chanblock 291: converged (50): 1e-4 > 2.49483e-8 > 1e-8
Chanblock   7: converged (50): 1e-4 > 1.38711e-8 > 1e-8
Chanblock  21: converged (18): 1e-8 > 6.78836e-9
Chanblock 723: converged (16): 1e-8 > 4.78477e-9
Chanblock  74: converged (50): 1e-4 > 1.00756e-8 > 1e-8
Chanblock  48: converged (50): 1e-4 > 3.42580e-8 > 1e-8
Chanblock 321: converged (16): 1e-8 > 5.61320e-9
Chanblock 300: converged (50): 1e-4 > 1.56135e-8 > 1e-8
Chanblock 698: converged (16): 1e-8 > 6.63951e-9
Chanblock 312: converged (50): 1e-4 > 1.11253e-8 > 1e-8
Chanblock  73: converged (50): 1e-4 > 2.17647e-8 > 1e-8
Chanblock 329: converged (16): 1e-8 > 7.68242e-9
Chanblock  76: converged (50): 1e-4 > 1.16821e-8 > 1e-8
Chanblock  96: converged (50): 1e-4 > 4.47403e-8 > 1e-8
Chanblock 332: converged (16): 1e-8 > 2.49486e-9
Chanblock 170: converged (16): 1e-8 > 5.17770e-9
Chanblock 192: converged (50): 1e-4 > 1.32087e-8 > 1e-8
Chanblock 386: converged (16): 1e-8 > 4.82562e-9
Chanblock   1: converged (50): 1e-4 > 2.42047e-8 > 1e-8
Chanblock 121: converged (16): 1e-8 > 4.25141e-9
Chanblock  92: converged (50): 1e-4 > 1.61495e-8 > 1e-8
Chanblock 319: converged (16): 1e-8 > 8.40987e-9
Chanblock 289: converged (50): 1e-4 > 2.16958e-8 > 1e-8
Chanblock  55: converged (50): 1e-4 > 1.15483e-7 > 1e-8
Chanblock 166: converged (50): 1e-4 > 1.68534e-8 > 1e-8
Chanblock 292: converged (50): 1e-4 > 1.38701e-8 > 1e-8
Chanblock   6: converged (50): 1e-4 > 1.37254e-8 > 1e-8
Chanblock 315: converged (50): 1e-4 > 1.56203e-8 > 1e-8
Chanblock 430: converged (16): 1e-8 > 6.10497e-9
Chanblock 418: converged (16): 1e-8 > 2.29269e-9
Chanblock  83: converged (18): 1e-8 > 4.52805e-9
Chanblock  94: converged (50): 1e-4 > 1.05424e-8 > 1e-8
Chanblock 361: converged (16): 1e-8 > 4.04896e-9
Chanblock 629: converged (16): 1e-8 > 2.43320e-9
Chanblock 675: converged (16): 1e-8 > 5.09961e-9
Chanblock 678: converged (16): 1e-8 > 4.78565e-9
Chanblock 322: converged (16): 1e-8 > 4.22987e-9
Chanblock  13: converged (50): 1e-4 > 2.20157e-8 > 1e-8
Chanblock 686: converged (16): 1e-8 > 8.08093e-9
Chanblock 651: converged (16): 1e-8 > 5.62335e-9
Chanblock 323: converged (16): 1e-8 > 4.18194e-9
Chanblock 600: converged (16): 1e-8 > 2.15015e-9
Chanblock 205: converged (18): 1e-8 > 7.69218e-9
Chanblock 339: converged (16): 1e-8 > 6.65016e-9
Chanblock 398: converged (16): 1e-8 > 4.42977e-9
Chanblock 158: converged (50): 1e-4 > 1.38923e-8 > 1e-8
Chanblock 147: converged (18): 1e-8 > 7.33989e-9
Chanblock 334: converged (16): 1e-8 > 1.93996e-9
Chanblock 676: converged (16): 1e-8 > 4.72032e-9
Chanblock 186: converged (16): 1e-8 > 5.54941e-9
Chanblock 633: converged (16): 1e-8 > 2.65300e-9
Chanblock 758: converged (16): 1e-8 > 6.74394e-9
Chanblock 348: converged (16): 1e-8 > 1.39772e-9
Chanblock 267: converged (16): 1e-8 > 4.06518e-9
Chanblock  27: converged (18): 1e-8 > 7.23048e-9
Chanblock 737: converged (16): 1e-8 > 4.03561e-9
Chanblock 661: converged (16): 1e-8 > 4.27904e-9
Chanblock 483: converged (16): 1e-8 > 5.07991e-9
Chanblock 435: converged (16): 1e-8 > 1.91444e-9
Chanblock  20: converged (18): 1e-8 > 5.33965e-9
Chanblock  19: converged (18): 1e-8 > 9.42026e-9
Chanblock 579: converged (16): 1e-8 > 7.71464e-9
Chanblock   4: converged (50): 1e-4 > 1.65038e-8 > 1e-8
Chanblock 362: converged (16): 1e-8 > 3.76568e-9
Chanblock 390: converged (16): 1e-8 > 2.27639e-9
Chanblock 639: converged (16): 1e-8 > 5.02780e-9
Chanblock 160: converged (50): 1e-4 > 1.01871e-8 > 1e-8
Chanblock 241: converged (50): 1e-4 > 2.25971e-8 > 1e-8
Chanblock 724: converged (16): 1e-8 > 4.54737e-9
Chanblock 171: converged (16): 1e-8 > 5.05241e-9
Chanblock 699: converged (16): 1e-8 > 7.01367e-9
Chanblock 317: converged (16): 1e-8 > 8.01676e-9
Chanblock 222: converged (16): 1e-8 > 4.61566e-9
Chanblock 690: converged (16): 1e-8 > 7.02965e-9
Chanblock   8: converged (18): 1e-8 > 5.75434e-9
Chanblock 180: converged (50): 1e-4 > 1.53329e-8 > 1e-8
Chanblock 387: converged (16): 1e-8 > 7.47851e-9
Chanblock  71: converged (18): 1e-8 > 9.95135e-9
Chanblock 626: converged (26): 1e-8 > 6.14364e-9
Chanblock 122: converged (16): 1e-8 > 8.31133e-9
Chanblock  70: converged (20): 1e-8 > 5.89519e-9
Chanblock 708: converged (16): 1e-8 > 6.73800e-9
Chanblock 750: converged (16): 1e-8 > 4.88003e-9
Chanblock 753: converged (16): 1e-8 > 3.12978e-9
Chanblock 687: converged (16): 1e-8 > 8.27276e-9
Chanblock 293: converged (16): 1e-8 > 8.93341e-9
Chanblock 303: converged (18): 1e-8 > 9.86709e-9
Chanblock  22: converged (20): 1e-8 > 6.38348e-9
Chanblock 297: converged (16): 1e-8 > 5.83690e-9
Chanblock 246: converged (16): 1e-8 > 1.83214e-9
Chanblock 431: converged (16): 1e-8 > 2.44956e-9
Chanblock 419: converged (16): 1e-8 > 8.49511e-9
Chanblock 677: converged (16): 1e-8 > 5.10726e-9
Chanblock 681: converged (16): 1e-8 > 3.81935e-9
Chanblock 642: converged (16): 1e-8 > 7.48555e-9
Chanblock 228: converged (50): 1e-4 > 2.43434e-8 > 1e-8
Chanblock 679: converged (16): 1e-8 > 4.88701e-9
Chanblock 354: converged (16): 1e-8 > 4.84519e-9
Chanblock 234: converged (16): 1e-8 > 3.50145e-9
Chanblock 693: converged (16): 1e-8 > 3.43899e-9
Chanblock 210: converged (16): 1e-8 > 9.29959e-9
Chanblock 652: converged (16): 1e-8 > 1.58819e-9
Chanblock 601: converged (16): 1e-8 > 6.15050e-9
Chanblock 363: converged (16): 1e-8 > 5.77008e-9
Chanblock 206: converged (16): 1e-8 > 4.13874e-9
Chanblock 612: converged (26): 1e-8 > 2.29057e-9
Chanblock 399: converged (16): 1e-8 > 5.96472e-9
Chanblock 340: converged (16): 1e-8 > 2.09044e-9
Chanblock 634: converged (16): 1e-8 > 3.46813e-9
Chanblock  14: converged (20): 1e-8 > 9.85808e-9
Chanblock 148: converged (16): 1e-8 > 9.06480e-9
Chanblock 606: converged (16): 1e-8 > 2.02341e-9
Chanblock 167: converged (50): 1e-4 > 1.02868e-8 > 1e-8
Chanblock 635: converged (16): 1e-8 > 5.23491e-9
Chanblock 662: converged (16): 1e-8 > 3.21761e-9
Chanblock 759: converged (16): 1e-8 > 5.61893e-9
Chanblock 484: converged (16): 1e-8 > 4.32520e-9
Chanblock 349: converged (16): 1e-8 > 4.88120e-9
Chanblock 436: converged (16): 1e-8 > 4.52181e-9
Chanblock  66: converged (50): 1e-4 > 4.97033e-8 > 1e-8
Chanblock 747: converged (16): 1e-8 > 8.12004e-9
Chanblock 161: converged (16): 1e-8 > 4.59774e-9
Chanblock 746: converged (50): 1e-4 > 1.00014e-8 > 1e-8
Chanblock 640: converged (16): 1e-8 > 3.62774e-9
Chanblock 181: converged (16): 1e-8 > 9.41124e-9
Chanblock 342: converged (16): 1e-8 > 1.56054e-9
Chanblock   2: converged (50): 1e-4 > 1.02551e-8 > 1e-8
Chanblock   5: converged (18): 1e-8 > 7.68709e-9
Chanblock  11: converged (50): 1e-4 > 1.72582e-8 > 1e-8
Chanblock 725: converged (16): 1e-8 > 5.00660e-9
Chanblock 700: converged (16): 1e-8 > 5.50480e-9
Chanblock 253: converged (50): 1e-4 > 1.01636e-8 > 1e-8
Chanblock 641: converged (16): 1e-8 > 3.06955e-9
Chanblock 223: converged (16): 1e-8 > 8.86945e-9
Chanblock 691: converged (16): 1e-8 > 5.57468e-9
Chanblock 388: converged (16): 1e-8 > 9.24635e-9
Chanblock  17: converged (50): 1e-4 > 1.17955e-8 > 1e-8
Chanblock 528: converged (20): 1e-8 > 4.89862e-9
Chanblock 351: converged (16): 1e-8 > 2.25131e-9
Chanblock 172: converged (18): 1e-8 > 9.40084e-9
Chanblock 335: converged (18): 1e-8 > 8.90993e-9
Chanblock 709: converged (16): 1e-8 > 8.97625e-9
Chanblock 123: converged (16): 1e-8 > 3.26913e-9
Chanblock 751: converged (16): 1e-8 > 7.09986e-9
Chanblock 754: converged (16): 1e-8 > 5.02882e-9
Chanblock 688: converged (16): 1e-8 > 4.76415e-9
Chanblock 748: converged (16): 1e-8 > 6.13148e-9
Chanblock  23: converged (18): 1e-8 > 7.58795e-9
Chanblock 618: converged (16): 1e-8 > 3.69457e-9
Chanblock 682: converged (16): 1e-8 > 4.55973e-9
Chanblock 680: converged (16): 1e-8 > 4.55909e-9
Chanblock  58: converged (50): 1e-4 > 7.23899e-8 > 1e-8
Chanblock 316: converged (50): 1e-4 > 1.40057e-8 > 1e-8
Chanblock 504: converged (16): 1e-8 > 3.00346e-9
Chanblock  61: converged (50): 1e-4 > 1.79228e-8 > 1e-8
Chanblock 244: converged (16): 1e-8 > 6.55547e-9
Chanblock 355: converged (16): 1e-8 > 5.20448e-9
Chanblock 643: converged (16): 1e-8 > 5.21950e-9
Chanblock 229: converged (16): 1e-8 > 9.31719e-9
Chanblock 694: converged (16): 1e-8 > 6.36580e-9
Chanblock 235: converged (16): 1e-8 > 4.24382e-9
Chanblock 174: converged (50): 1e-4 > 1.68953e-8 > 1e-8
Chanblock 218: converged (50): 1e-4 > 1.09900e-8 > 1e-8
Chanblock 602: converged (16): 1e-8 > 2.07698e-9
Chanblock 207: converged (16): 1e-8 > 6.07932e-9
Chanblock 400: converged (16): 1e-8 > 4.61976e-9
Chanblock 580: converged (28): 1e-8 > 4.37555e-9
Chanblock 653: converged (16): 1e-8 > 5.75739e-9
Chanblock 402: converged (16): 1e-8 > 6.10078e-9
Chanblock 341: converged (16): 1e-8 > 2.33818e-9
Chanblock  36: converged (50): 1e-4 > 2.50004e-8 > 1e-8
Chanblock  59: converged (50): 1e-4 > 5.98998e-8 > 1e-8
Chanblock 695: converged (16): 1e-8 > 4.98141e-9
Chanblock 149: converged (16): 1e-8 > 7.00840e-9
Chanblock 607: converged (16): 1e-8 > 1.75700e-9
Chanblock 295: converged (50): 1e-4 > 1.83445e-8 > 1e-8
Chanblock 663: converged (16): 1e-8 > 2.48045e-9
Chanblock 485: converged (16): 1e-8 > 2.79102e-9
Chanblock 357: converged (16): 1e-8 > 2.39388e-9
Chanblock 437: converged (16): 1e-8 > 3.53851e-9
Chanblock 760: converged (16): 1e-8 > 6.14672e-9
Chanblock 350: converged (16): 1e-8 > 3.00065e-9
Chanblock 306: converged (50): 1e-4 > 1.70775e-8 > 1e-8
Chanblock 645: converged (16): 1e-8 > 2.78099e-9
Chanblock  52: converged (50): 1e-4 > 5.03687e-8 > 1e-8
Chanblock 213: converged (16): 1e-8 > 4.08346e-9
Chanblock 320: converged (50): 1e-4 > 1.55547e-8 > 1e-8
Chanblock  80: converged (50): 1e-4 > 1.22935e-8 > 1e-8
Chanblock 752: converged (16): 1e-8 > 6.32014e-9
Chanblock 182: converged (16): 1e-8 > 8.86516e-9
Chanblock 654: converged (16): 1e-8 > 3.35165e-9
Chanblock 301: converged (50): 1e-4 > 4.24664e-8 > 1e-8
Chanblock  67: converged (50): 1e-4 > 2.79474e-8 > 1e-8
Chanblock  56: converged (50): 1e-4 > 6.27395e-8 > 1e-8
Chanblock  97: converged (50): 1e-4 > 1.13995e-8 > 1e-8
Chanblock 613: converged (24): 1e-8 > 9.93975e-9
Chanblock 343: converged (16): 1e-8 > 3.39395e-9
Chanblock 193: converged (50): 1e-4 > 1.90716e-8 > 1e-8
Chanblock 702: converged (16): 1e-8 > 4.68131e-9
Chanblock 726: converged (16): 1e-8 > 5.16649e-9
Chanblock 701: converged (16): 1e-8 > 8.73446e-9
Chanblock 224: converged (16): 1e-8 > 4.72321e-9
Chanblock 175: converged (16): 1e-8 > 5.81721e-9
Chanblock 692: converged (16): 1e-8 > 9.00452e-9
Chanblock 389: converged (16): 1e-8 > 3.97437e-9
Chanblock 714: converged (16): 1e-8 > 3.13582e-9
Chanblock 352: converged (16): 1e-8 > 4.86660e-9
Chanblock 173: converged (16): 1e-8 > 8.09584e-9
Chanblock 710: converged (16): 1e-8 > 2.90457e-9
Chanblock 124: converged (16): 1e-8 > 7.57932e-9
Chanblock 621: converged (16): 1e-8 > 2.09351e-9
Chanblock 755: converged (16): 1e-8 > 7.86452e-9
Chanblock 689: converged (16): 1e-8 > 4.65253e-9
Chanblock 749: converged (16): 1e-8 > 3.92726e-9
Chanblock 198: converged (16): 1e-8 > 6.43908e-9
Chanblock 683: converged (16): 1e-8 > 6.31347e-9
Chanblock 302: converged (50): 1e-4 > 2.50306e-8 > 1e-8
Chanblock 254: converged (16): 1e-8 > 4.71222e-9
Chanblock  69: converged (50): 1e-4 > 1.03471e-8 > 1e-8
Chanblock 619: converged (16): 1e-8 > 2.27583e-9
Chanblock 620: converged (16): 1e-8 > 2.33925e-9
Chanblock 730: converged (16): 1e-8 > 5.58297e-9
Chanblock 356: converged (16): 1e-8 > 5.16516e-9
Chanblock 729: converged (16): 1e-8 > 4.10765e-9
Chanblock 505: converged (16): 1e-8 > 2.79287e-9
Chanblock 189: converged (50): 1e-4 > 1.04958e-8 > 1e-8
Chanblock 588: converged (24): 1e-8 > 6.42339e-9
Chanblock 230: converged (16): 1e-8 > 7.81466e-9
Chanblock 364: converged (50): 1e-4 > 1.13590e-8 > 1e-8
Chanblock 603: converged (16): 1e-8 > 3.68567e-9
Chanblock 236: converged (16): 1e-8 > 3.11146e-9
Chanblock 208: converged (16): 1e-8 > 6.14696e-9
Chanblock 219: converged (16): 1e-8 > 7.66158e-9
Chanblock 401: converged (16): 1e-8 > 4.96078e-9
Chanblock 150: converged (18): 1e-8 > 3.90650e-9
Chanblock 644: converged (18): 1e-8 > 3.40074e-9
Chanblock 529: converged (22): 1e-8 > 2.02711e-9
Chanblock 403: converged (16): 1e-8 > 3.64986e-9
Chanblock 177: converged (50): 1e-4 > 3.29819e-8 > 1e-8
Chanblock 225: converged (16): 1e-8 > 3.66591e-9
Chanblock 486: converged (16): 1e-8 > 2.26940e-9
Chanblock 231: converged (16): 1e-8 > 8.29286e-9
Chanblock 438: converged (16): 1e-8 > 2.49358e-9
Chanblock 664: converged (16): 1e-8 > 4.82411e-9
Chanblock 268: converged (50): 1e-4 > 1.60548e-8 > 1e-8
Chanblock 358: converged (16): 1e-8 > 4.92902e-9
Chanblock  28: converged (50): 1e-4 > 1.88622e-8 > 1e-8
Chanblock 761: converged (16): 1e-8 > 9.32392e-9
Chanblock 201: converged (16): 1e-8 > 5.37224e-9
Chanblock 307: converged (16): 1e-8 > 9.26499e-9
Chanblock 727: converged (16): 1e-8 > 7.12589e-9
Chanblock 187: converged (50): 1e-4 > 1.80910e-8 > 1e-8
Chanblock 214: converged (16): 1e-8 > 2.88210e-9
Chanblock 220: converged (16): 1e-8 > 4.86417e-9
Chanblock 391: converged (50): 1e-4 > 1.34896e-8 > 1e-8
Chanblock 393: converged (16): 1e-8 > 3.34927e-9
Chanblock 655: converged (16): 1e-8 > 2.26974e-9
Chanblock 646: converged (16): 1e-8 > 8.10358e-9
Chanblock 731: converged (16): 1e-8 > 9.71932e-9
Chanblock  98: converged (16): 1e-8 > 8.70163e-9
Chanblock 615: converged (24): 1e-8 > 9.19801e-9
Chanblock 242: converged (50): 1e-4 > 1.01727e-8 > 1e-8
Chanblock 344: converged (16): 1e-8 > 4.15114e-9
Chanblock 703: converged (16): 1e-8 > 6.34246e-9
Chanblock 715: converged (16): 1e-8 > 8.24422e-9
Chanblock 717: converged (16): 1e-8 > 4.75591e-9
Chanblock 184: converged (16): 1e-8 > 7.48996e-9
Chanblock 153: converged (18): 1e-8 > 9.57562e-9
Chanblock 345: converged (50): 1e-4 > 1.66450e-8 > 1e-8
Chanblock 176: converged (16): 1e-8 > 5.95888e-9
Chanblock 608: converged (22): 1e-8 > 3.35422e-9
Chanblock 151: converged (16): 1e-8 > 5.63033e-9
Chanblock 195: converged (16): 1e-8 > 4.93925e-9
Chanblock 353: converged (16): 1e-8 > 4.92361e-9
Chanblock 711: converged (16): 1e-8 > 6.93454e-9
Chanblock 622: converged (16): 1e-8 > 2.18014e-9
Chanblock 712: converged (16): 1e-8 > 9.48728e-9
Chanblock 255: converged (16): 1e-8 > 6.12764e-9
Chanblock 346: converged (16): 1e-8 > 7.82179e-9
Chanblock 249: converged (16): 1e-8 > 9.52765e-9
Chanblock 706: converged (16): 1e-8 > 8.55326e-9
Chanblock 705: converged (18): 1e-8 > 6.67652e-9
Chanblock 392: converged (16): 1e-8 > 4.42731e-9
Chanblock 581: converged (30): 1e-8 > 5.63975e-9
Chanblock 582: converged (36): 1e-8 > 7.40355e-9
Chanblock 589: converged (16): 1e-8 > 2.37056e-9
Chanblock 594: converged (16): 1e-8 > 3.95812e-9
Chanblock 506: converged (16): 1e-8 > 7.19410e-9
Chanblock 304: converged (50): 1e-4 > 2.87164e-8 > 1e-8
Chanblock 614: converged (26): 1e-8 > 3.07081e-9
Chanblock 298: converged (50): 1e-4 > 2.87616e-8 > 1e-8
Chanblock 247: converged (50): 1e-4 > 1.11642e-8 > 1e-8
Chanblock 604: converged (16): 1e-8 > 7.41641e-9
Chanblock 657: converged (16): 1e-8 > 5.51962e-9
Chanblock 196: converged (16): 1e-8 > 2.73285e-9
Chanblock 237: converged (16): 1e-8 > 9.73887e-9
Chanblock 211: converged (50): 1e-4 > 1.72945e-8 > 1e-8
Chanblock 200: converged (16): 1e-8 > 9.07945e-9
Chanblock 404: converged (16): 1e-8 > 4.01986e-9
Chanblock 372: converged (18): 1e-8 > 9.58621e-9
Chanblock 487: converged (16): 1e-8 > 6.28121e-9
Chanblock 665: converged (16): 1e-8 > 2.24671e-9
Chanblock 359: converged (14): 1e-8 > 9.67251e-9
Chanblock 188: converged (50): 1e-4 > 1.73977e-8 > 1e-8
Chanblock 658: converged (16): 1e-8 > 4.21282e-9
Chanblock 439: converged (16): 1e-8 > 3.23593e-9
Chanblock 269: converged (16): 1e-8 > 4.35463e-9
Chanblock 728: converged (16): 1e-8 > 3.13974e-9
Chanblock 202: converged (16): 1e-8 > 5.72196e-9
Chanblock 762: converged (16): 1e-8 > 5.27487e-9
Chanblock  29: converged (18): 1e-8 > 8.49147e-9
Chanblock 394: converged (16): 1e-8 > 5.46668e-9
Chanblock 656: converged (16): 1e-8 > 4.66046e-9
Chanblock 378: converged (16): 1e-8 > 4.10804e-9
Chanblock 530: converged (22): 1e-8 > 5.84361e-9
Chanblock 215: converged (16): 1e-8 > 8.23148e-9
Chanblock 456: converged (16): 1e-8 > 2.75217e-9
Chanblock 243: converged (50): 1e-4 > 1.12291e-8 > 1e-8
Chanblock 647: converged (16): 1e-8 > 5.21398e-9
Chanblock 154: converged (16): 1e-8 > 5.96154e-9
Chanblock 381: converged (16): 1e-8 > 4.94688e-9
Chanblock 138: converged (18): 1e-8 > 5.10228e-9
Chanblock 704: converged (16): 1e-8 > 5.69795e-9
Chanblock 347: converged (16): 1e-8 > 2.72081e-9
Chanblock 718: converged (16): 1e-8 > 7.20786e-9
Chanblock 152: converged (16): 1e-8 > 5.05807e-9
Chanblock 256: converged (16): 1e-8 > 8.01570e-9
Chanblock 248: converged (16): 1e-8 > 4.24029e-9
Chanblock 183: converged (50): 1e-4 > 2.33713e-8 > 1e-8
Chanblock 716: converged (18): 1e-8 > 4.98970e-9
Chanblock 444: converged (16): 1e-8 > 4.80153e-9
Chanblock 713: converged (16): 1e-8 > 5.92736e-9
Chanblock 765: converged (16): 1e-8 > 8.35858e-9
Chanblock 468: converged (16): 1e-8 > 3.52867e-9
Chanblock 590: converged (16): 1e-8 > 3.71285e-9
Chanblock 707: converged (16): 1e-8 > 5.94442e-9
Chanblock 245: converged (50): 1e-4 > 1.20821e-8 > 1e-8
Chanblock 250: converged (16): 1e-8 > 9.53195e-9
Chanblock 595: converged (16): 1e-8 > 8.90225e-9
Chanblock 605: converged (16): 1e-8 > 3.10286e-9
Chanblock 617: converged (16): 1e-8 > 6.03981e-9
Chanblock 719: converged (16): 1e-8 > 6.49601e-9
Chanblock 659: converged (16): 1e-8 > 4.22563e-9
Chanblock  42: converged (50): 1e-4 > 1.32337e-8 > 1e-8
Chanblock  30: converged (18): 1e-8 > 6.68406e-9
Chanblock 212: converged (16): 1e-8 > 3.89443e-9
Chanblock 507: converged (18): 1e-8 > 9.83929e-9
Chanblock 405: converged (16): 1e-8 > 6.15939e-9
Chanblock  62: converged (50): 1e-4 > 7.90016e-8 > 1e-8
Chanblock 197: converged (18): 1e-8 > 5.74976e-9
Chanblock 135: converged (18): 1e-8 > 7.07583e-9
Chanblock 373: converged (16): 1e-8 > 8.35895e-9
Chanblock 766: converged (16): 1e-8 > 8.36141e-9
Chanblock 488: converged (16): 1e-8 > 6.10076e-9
Chanblock 616: converged (26): 1e-8 > 2.75849e-9
Chanblock 239: converged (16): 1e-8 > 8.99444e-9
Chanblock 666: converged (16): 1e-8 > 3.92384e-9
Chanblock 440: converged (16): 1e-8 > 4.45637e-9
Chanblock 609: converged (24): 1e-8 > 2.63269e-9
Chanblock  37: converged (50): 1e-4 > 2.13620e-8 > 1e-8
Chanblock 763: converged (16): 1e-8 > 6.20359e-9
Chanblock 270: converged (16): 1e-8 > 2.89818e-9
Chanblock 585: converged (34): 1e-8 > 6.19397e-9
Chanblock 597: converged (16): 1e-8 > 5.08313e-9
Chanblock 296: converged (50): 1e-4 > 2.89910e-8 > 1e-8
Chanblock 309: converged (50): 1e-4 > 1.72479e-8 > 1e-8
Chanblock 369: converged (16): 1e-8 > 5.13448e-9
Chanblock 203: converged (16): 1e-8 > 7.25169e-9
Chanblock  53: converged (50): 1e-4 > 6.13254e-8 > 1e-8
Chanblock 395: converged (16): 1e-8 > 8.04272e-9
Chanblock 379: converged (16): 1e-8 > 5.20257e-9
Chanblock 623: converged (26): 1e-8 > 6.56357e-9
Chanblock 457: converged (16): 1e-8 > 5.73053e-9
Chanblock 179: converged (16): 1e-8 > 4.62060e-9
Chanblock  45: converged (50): 1e-4 > 4.00546e-8 > 1e-8
Chanblock 137: converged (18): 1e-8 > 8.94504e-9
Chanblock 178: converged (50): 1e-4 > 1.28464e-8 > 1e-8
Chanblock  68: converged (50): 1e-4 > 2.47033e-8 > 1e-8
Chanblock 155: converged (16): 1e-8 > 4.41096e-9
Chanblock 382: converged (16): 1e-8 > 2.07939e-9
Chanblock 194: converged (50): 1e-4 > 1.10157e-8 > 1e-8
Chanblock 441: converged (16): 1e-8 > 5.11297e-9
Chanblock 366: converged (50): 1e-4 > 1.00552e-8 > 1e-8
Chanblock 257: converged (16): 1e-8 > 5.08289e-9
Chanblock 598: converged (16): 1e-8 > 2.28062e-9
Chanblock 141: converged (20): 1e-8 > 9.58519e-9
Chanblock 199: converged (50): 1e-4 > 1.22982e-8 > 1e-8
Chanblock 276: converged (16): 1e-8 > 7.32585e-9
Chanblock 445: converged (16): 1e-8 > 4.30481e-9
Chanblock 469: converged (16): 1e-8 > 4.32783e-9
Chanblock  40: converged (50): 1e-4 > 1.99767e-8 > 1e-8
Chanblock 221: converged (50): 1e-4 > 2.19364e-8 > 1e-8
Chanblock 591: converged (16): 1e-8 > 3.99384e-9
Chanblock  39: converged (50): 1e-4 > 3.99160e-8 > 1e-8
Chanblock 126: converged (16): 1e-8 > 9.77862e-9
Chanblock 143: converged (16): 1e-8 > 4.00774e-9
Chanblock  43: converged (50): 1e-4 > 2.63586e-8 > 1e-8
Chanblock 531: converged (22): 1e-8 > 9.30717e-9
Chanblock 383: converged (16): 1e-8 > 9.03078e-9
Chanblock 226: converged (50): 1e-4 > 1.20882e-8 > 1e-8
Chanblock 251: converged (16): 1e-8 > 3.62289e-9
Chanblock 583: converged (32): 1e-8 > 6.05876e-9
Chanblock 132: converged (50): 1e-4 > 1.54847e-8 > 1e-8
Chanblock 596: converged (16): 1e-8 > 3.90934e-9
Chanblock 125: converged (50): 1e-4 > 1.76511e-8 > 1e-8
Chanblock 370: converged (16): 1e-8 > 3.74292e-9
Chanblock 140: converged (16): 1e-8 > 4.91592e-9
Chanblock 492: converged (16): 1e-8 > 4.06463e-9
Chanblock 516: converged (16): 1e-8 > 4.00285e-9
Chanblock 365: converged (50): 1e-4 > 1.11609e-8 > 1e-8
Chanblock 406: converged (16): 1e-8 > 3.17504e-9
Chanblock 209: converged (50): 1e-4 > 2.35211e-8 > 1e-8
Chanblock 669: converged (16): 1e-8 > 5.88770e-9
Chanblock 508: converged (16): 1e-8 > 7.26035e-9
Chanblock 374: converged (16): 1e-8 > 9.71888e-9
Chanblock 190: converged (50): 1e-4 > 1.46864e-8 > 1e-8
Chanblock 489: converged (16): 1e-8 > 5.87366e-9
Chanblock 767: converged (16): 1e-8 > 9.49381e-9
Chanblock 667: converged (16): 1e-8 > 5.26841e-9
Chanblock 498: converged (16): 1e-8 > 2.22469e-9
Chanblock 271: converged (16): 1e-8 > 3.64766e-9
Chanblock 670: converged (16): 1e-8 > 4.64738e-9
Chanblock 227: converged (50): 1e-4 > 1.07543e-8 > 1e-8
Chanblock 764: converged (16): 1e-8 > 6.70204e-9
Chanblock 142: converged (16): 1e-8 > 6.61302e-9
Chanblock 442: converged (16): 1e-8 > 4.80294e-9
Chanblock 380: converged (16): 1e-8 > 4.97771e-9
Chanblock 510: converged (16): 1e-8 > 4.41867e-9
Chanblock 671: converged (16): 1e-8 > 4.82518e-9
Chanblock 586: converged (32): 1e-8 > 8.84794e-9
Chanblock 232: converged (50): 1e-4 > 2.47693e-8 > 1e-8
Chanblock 458: converged (16): 1e-8 > 3.58800e-9
Chanblock 114: converged (16): 1e-8 > 5.35594e-9
Chanblock 299: converged (50): 1e-4 > 2.76344e-8 > 1e-8
Chanblock 668: converged (16): 1e-8 > 5.45463e-9
Chanblock  99: converged (50): 1e-4 > 3.27441e-8 > 1e-8
Chanblock  38: converged (50): 1e-4 > 1.07959e-8 > 1e-8
Chanblock 570: converged (16): 1e-8 > 6.24775e-9
Chanblock 564: converged (16): 1e-8 > 7.81124e-9
Chanblock 308: converged (50): 1e-4 > 1.10206e-8 > 1e-8
Chanblock 587: converged (30): 1e-8 > 7.67252e-9
Chanblock 522: converged (18): 1e-8 > 3.82628e-9
Chanblock 610: converged (22): 1e-8 > 5.95819e-9
Chanblock 258: converged (16): 1e-8 > 2.65506e-9
Chanblock 558: converged (16): 1e-8 > 4.82417e-9
Chanblock 470: converged (16): 1e-8 > 5.74821e-9
Chanblock 599: converged (16): 1e-8 > 2.27916e-9
Chanblock 592: converged (16): 1e-8 > 2.47053e-9
Chanblock 277: converged (16): 1e-8 > 1.63009e-9
Chanblock 611: converged (24): 1e-8 > 4.72638e-9
Chanblock 185: converged (50): 1e-4 > 1.01509e-8 > 1e-8
Chanblock 133: converged (18): 1e-8 > 5.28283e-9
Chanblock 446: converged (16): 1e-8 > 5.56624e-9
Chanblock 568: converged (16): 1e-8 > 6.02946e-9
Chanblock 567: converged (16): 1e-8 > 3.39207e-9
Chanblock 191: converged (50): 1e-4 > 1.46292e-8 > 1e-8
Chanblock 490: converged (16): 1e-8 > 5.77365e-9
Chanblock 127: converged (16): 1e-8 > 9.43517e-9
Chanblock 238: converged (50): 1e-4 > 2.37776e-8 > 1e-8
Chanblock 443: converged (16): 1e-8 > 3.86968e-9
Chanblock 565: converged (16): 1e-8 > 3.83311e-9
Chanblock 566: converged (16): 1e-8 > 6.05673e-9
Chanblock 519: converged (16): 1e-8 > 3.31085e-9
Chanblock 517: converged (16): 1e-8 > 3.19485e-9
Chanblock  44: converged (50): 1e-4 > 2.73195e-8 > 1e-8
Chanblock 501: converged (16): 1e-8 > 3.03818e-9
Chanblock 493: converged (16): 1e-8 > 4.01796e-9
Chanblock 103: converged (16): 1e-8 > 9.22815e-9
Chanblock 407: converged (16): 1e-8 > 5.32814e-9
Chanblock 525: converged (16): 1e-8 > 4.44983e-9
Chanblock 491: converged (16): 1e-8 > 3.04294e-9
Chanblock 520: converged (16): 1e-8 > 8.15565e-9
Chanblock 509: converged (16): 1e-8 > 7.29043e-9
Chanblock 305: converged (50): 1e-4 > 1.22305e-8 > 1e-8
Chanblock 573: converged (16): 1e-8 > 5.46709e-9
Chanblock 523: converged (18): 1e-8 > 4.50724e-9
Chanblock 502: converged (16): 1e-8 > 5.10467e-9
Chanblock 499: converged (16): 1e-8 > 5.49094e-9
Chanblock 272: converged (16): 1e-8 > 8.97710e-9
Chanblock 546: converged (40): 1e-8 > 9.37420e-9
Chanblock 513: converged (16): 1e-8 > 6.32011e-9
Chanblock 136: converged (50): 1e-4 > 1.10035e-8 > 1e-8
Chanblock 561: converged (16): 1e-8 > 2.77878e-9
Chanblock 273: converged (16): 1e-8 > 4.42826e-9
Chanblock 511: converged (16): 1e-8 > 4.97441e-9
Chanblock 447: converged (16): 1e-8 > 8.03329e-9
Chanblock 532: converged (24): 1e-8 > 3.61803e-9
Chanblock  33: converged (18): 1e-8 > 7.99981e-9
Chanblock 459: converged (16): 1e-8 > 3.91406e-9
Chanblock 310: converged (50): 1e-4 > 1.70707e-8 > 1e-8
Chanblock 540: converged (34): 1e-8 > 3.92198e-9
Chanblock 571: converged (16): 1e-8 > 2.48623e-9
Chanblock 514: converged (16): 1e-8 > 6.69941e-9
Chanblock  41: converged (50): 1e-4 > 1.26201e-8 > 1e-8
Chanblock 521: converged (18): 1e-8 > 4.54712e-9
Chanblock 495: converged (16): 1e-8 > 7.80941e-9
Chanblock 259: converged (16): 1e-8 > 9.41911e-9
Chanblock 139: converged (50): 1e-4 > 1.03476e-8 > 1e-8
Chanblock 101: converged (16): 1e-8 > 7.93530e-9
Chanblock  32: converged (20): 1e-8 > 7.94218e-9
Chanblock 526: converged (20): 1e-8 > 2.75286e-9
Chanblock 559: converged (16): 1e-8 > 3.93972e-9
Chanblock 471: converged (16): 1e-8 > 5.33981e-9
Chanblock 593: converged (16): 1e-8 > 2.71431e-9
Chanblock 569: converged (16): 1e-8 > 2.89066e-9
Chanblock 494: converged (16): 1e-8 > 5.20718e-9
Chanblock 278: converged (16): 1e-8 > 4.96959e-9
Chanblock 503: converged (16): 1e-8 > 3.91501e-9
Chanblock 574: converged (16): 1e-8 > 2.65750e-9
Chanblock 282: converged (16): 1e-8 > 4.08760e-9
Chanblock 109: converged (16): 1e-8 > 4.31367e-9
Chanblock 474: converged (16): 1e-8 > 8.45189e-9
Chanblock 541: converged (36): 1e-8 > 4.07197e-9
Chanblock 512: converged (16): 1e-8 > 5.87240e-9
Chanblock 233: converged (50): 1e-4 > 2.12426e-8 > 1e-8
Chanblock  64: converged (50): 1e-4 > 1.21712e-8 > 1e-8
Chanblock 105: converged (40): 1e-8 > 9.99566e-9
Chanblock 500: converged (16): 1e-8 > 3.16147e-9
Chanblock 557: converged (16): 1e-8 > 2.56903e-9
Chanblock 107: converged (16): 1e-8 > 4.36701e-9
Chanblock  63: converged (50): 1e-4 > 7.62531e-8 > 1e-8
Chanblock 534: converged (24): 1e-8 > 9.14927e-9
Chanblock 518: converged (16): 1e-8 > 5.78719e-9
Chanblock 128: converged (18): 1e-8 > 5.71314e-9
Chanblock 462: converged (16): 1e-8 > 5.11030e-9
Chanblock 448: converged (16): 1e-8 > 1.93299e-9
Chanblock 575: converged (16): 1e-8 > 3.14258e-9
Chanblock 572: converged (16): 1e-8 > 3.62336e-9
Chanblock 129: converged (16): 1e-8 > 5.54170e-9
Chanblock 478: converged (16): 1e-8 > 2.76264e-9
Chanblock 450: converged (50): 1e-4 > 1.46955e-8 > 1e-8
Chanblock 551: converged (16): 1e-8 > 3.48956e-9
Chanblock 584: converged (34): 1e-8 > 9.14511e-9
Chanblock 524: converged (18): 1e-8 > 5.15838e-9
Chanblock 453: converged (16): 1e-8 > 6.69184e-9
Chanblock 477: converged (16): 1e-8 > 4.29126e-9
Chanblock 496: converged (16): 1e-8 > 6.32941e-9
Chanblock 110: converged (18): 1e-8 > 8.74342e-9
Chanblock 543: converged (46): 1e-8 > 7.94247e-9
Chanblock 451: converged (16): 1e-8 > 6.13563e-9
Chanblock  65: converged (50): 1e-4 > 4.47210e-8 > 1e-8
Chanblock 562: converged (16): 1e-8 > 4.12131e-9
Chanblock 102: converged (50): 1e-4 > 1.02646e-8 > 1e-8
Chanblock  31: converged (50): 1e-4 > 2.82300e-8 > 1e-8
Chanblock 274: converged (16): 1e-8 > 5.74471e-9
Chanblock 479: converged (16): 1e-8 > 2.42470e-9
Chanblock 552: converged (50): 1e-4 > 1.03323e-8 > 1e-8
Chanblock 108: converged (50): 1e-4 > 2.40691e-8 > 1e-8
Chanblock 497: converged (16): 1e-8 > 2.97765e-9
Chanblock 465: converged (16): 1e-8 > 3.82540e-9
Chanblock 100: converged (50): 1e-4 > 1.26286e-8 > 1e-8
Chanblock 452: converged (16): 1e-8 > 3.65944e-9
Chanblock 466: converged (16): 1e-8 > 5.26577e-9
Chanblock  46: converged (50): 1e-4 > 6.79630e-8 > 1e-8
Chanblock 544: converged (44): 1e-8 > 4.59091e-9
Chanblock 460: converged (16): 1e-8 > 2.27415e-9
Chanblock 275: converged (16): 1e-8 > 2.80214e-9
Chanblock 537: converged (16): 1e-8 > 2.79530e-9
Chanblock 515: converged (16): 1e-8 > 3.96371e-9
Chanblock 549: converged (46): 1e-8 > 7.80295e-9
Chanblock 563: converged (16): 1e-8 > 2.65455e-9
Chanblock 560: converged (16): 1e-8 > 4.66641e-9
Chanblock 542: converged (16): 1e-8 > 3.60201e-9
Chanblock 533: converged (24): 1e-8 > 5.47664e-9
Chanblock 461: converged (16): 1e-8 > 3.13685e-9
Chanblock 554: converged (16): 1e-8 > 2.72293e-9
Chanblock 375: converged (50): 1e-4 > 1.25323e-8 > 1e-8
Chanblock 527: converged (20): 1e-8 > 6.68723e-9
Chanblock 463: converged (16): 1e-8 > 6.49355e-9
Chanblock 367: converged (50): 1e-4 > 1.10477e-8 > 1e-8
Chanblock 260: converged (16): 1e-8 > 4.46408e-9
Chanblock 280: converged (16): 1e-8 > 4.40008e-9
Chanblock 472: converged (16): 1e-8 > 3.32491e-9
Chanblock 286: converged (16): 1e-8 > 7.33935e-9
Chanblock 279: converged (16): 1e-8 > 5.76508e-9
Chanblock 376: converged (16): 1e-8 > 7.35060e-9
Chanblock 368: converged (50): 1e-4 > 2.24378e-8 > 1e-8
Chanblock 116: converged (16): 1e-8 > 3.66992e-9
Chanblock 473: converged (16): 1e-8 > 2.93676e-9
Chanblock 287: converged (18): 1e-8 > 9.78072e-9
Chanblock 113: converged (16): 1e-8 > 7.50583e-9
Chanblock 449: converged (16): 1e-8 > 4.07568e-9
Chanblock 111: converged (50): 1e-4 > 3.08811e-8 > 1e-8
Chanblock 535: converged (26): 1e-8 > 2.63143e-9
Chanblock 475: converged (16): 1e-8 > 4.34613e-9
Chanblock 545: converged (42): 1e-8 > 6.64117e-9
Chanblock 285: converged (16): 1e-8 > 9.19082e-9
Chanblock 454: converged (16): 1e-8 > 3.99894e-9
Chanblock 262: converged (16): 1e-8 > 3.51246e-9
Chanblock 371: converged (50): 1e-4 > 1.35846e-8 > 1e-8
Chanblock 377: converged (16): 1e-8 > 6.44159e-9
Chanblock 476: converged (16): 1e-8 > 5.04016e-9
Chanblock 106: converged (50): 1e-4 > 2.41079e-8 > 1e-8
Chanblock 281: converged (16): 1e-8 > 3.72821e-9
Chanblock 467: converged (16): 1e-8 > 6.67128e-9
Chanblock 263: converged (16): 1e-8 > 3.56014e-9
Chanblock 115: converged (50): 1e-4 > 2.14504e-8 > 1e-8
Chanblock 284: converged (16): 1e-8 > 8.01001e-9
Chanblock 555: converged (50): 1e-4 > 3.00013e-8 > 1e-8
Chanblock 119: converged (18): 1e-8 > 9.98704e-9
Chanblock 464: converged (16): 1e-8 > 8.64467e-9
Chanblock 538: converged (30): 1e-8 > 5.04125e-9
Chanblock 311: converged (50): 1e-4 > 1.48815e-8 > 1e-8
Chanblock 117: converged (50): 1e-4 > 2.46452e-8 > 1e-8
Chanblock 118: converged (16): 1e-8 > 7.38420e-9
Chanblock 112: converged (50): 1e-4 > 3.28882e-8 > 1e-8
Chanblock 455: converged (16): 1e-8 > 4.23591e-9
Chanblock 131: converged (18): 1e-8 > 6.75125e-9
Chanblock  47: converged (50): 1e-4 > 3.50758e-8 > 1e-8
Chanblock 548: converged (26): 1e-8 > 9.50463e-9
Chanblock 556: converged (50): 1e-4 > 4.62291e-8 > 1e-8
Chanblock  34: converged (50): 1e-4 > 1.58317e-8 > 1e-8
Chanblock 134: converged (50): 1e-4 > 1.21811e-8 > 1e-8
Chanblock  35: converged (50): 1e-4 > 1.13254e-8 > 1e-8
Chanblock 536: converged (28): 1e-8 > 6.39210e-9
Chanblock 550: converged (44): 1e-8 > 6.31861e-9
Chanblock 539: converged (28): 1e-8 > 8.73202e-9
Chanblock 547: converged (42): 1e-8 > 5.54283e-9
Chanblock 104: converged (50): 1e-4 > 3.35963e-8 > 1e-8
Chanblock 553: converged (50): 1e-4 > 1.23465e-8 > 1e-8
Chanblock 130: converged (50): 1e-4 > 1.83937e-8 > 1e-8
Chanblock 283: converged (50): 1e-4 > 2.31819e-8 > 1e-8
Chanblock 261: converged (50): 1e-4 > 1.53148e-8 > 1e-8
[2026-01-14T14:40:07Z INFO ] All timesteps: 768/768 (100%) chanblocks converged
[2026-01-14T14:40:07Z INFO ] Calibration solutions written to:
[2026-01-14T14:40:07Z INFO ]   /data/calvin/jobs/3692_1452436432/1452436432_solutions.fits
[2026-01-14T14:40:07Z INFO ]   /data/calvin/jobs/3692_1452436432/1452436432_solutions.bin
[2026-01-14T14:40:07Z INFO ] hyperdrive di-calibrate complete.

stderr: 
