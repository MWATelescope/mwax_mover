"""Regression test for calvin.plots.phases.plot_debug_phase_fits: byte-order
handling.

Split out of the former test023_calvin_plots.py (docs/RESTRUCTURE.md
test-tree reorg). The matplotlib-heavy plotting functions this module
exercises are otherwise covered indirectly via tests/calvin/test_pipeline.py's
real-fixture integration tests rather than duplicated here as isolated unit
tests -- rendering full figures against synthetic data adds little beyond
what those integration tests already cover, and would be slow to run
per-test. This is the one exception: tests/calvin/test_pipeline.py and
tests/calvin/test_hyperdrive.py's integration tests all patch
plot_debug_phase_fits out entirely, so nothing else in the suite actually
calls it with byte-swapped (e.g. real FITS-derived, big-endian) input --
exactly the input that triggered a numpy-2.0-incompatible
ndarray.newbyteorder() call in a since-removed local duplicate of
ensure_system_byte_order.
"""

import numpy as np
import pandas as pd

from mwax_mover.calvin.plots.phases import plot_debug_phase_fits


def test_plot_debug_phase_fits_handles_byteswapped_input():
    """plot_debug_phase_fits must not crash on non-native byte order.

    Regression test: FITS data (freqs/solutions/weights read straight off
    disk) is always big-endian per the FITS standard, so on a
    little-endian machine this is the realistic input shape, not an edge
    case. A previous local duplicate of ensure_system_byte_order in this
    module used ndarray.newbyteorder(), which numpy removed in 2.0 --
    silently never caught because every other test exercising this
    function mocks it out entirely.
    """
    n_chan = 4
    tile_ids = np.array([1, 2])
    tiles = pd.DataFrame(
        {
            "id": tile_ids,
            "name": [f"Tile{i:03d}" for i in tile_ids],
            "rx": [1, 1],
            "slot": [1, 2],
            "flavor": ["RRI", "RRI"],
            "flag": [False, False],
        }
    )

    rows = []
    for tile_id in tile_ids:
        for pol in ["XX", "YY"]:
            rows.append(
                {
                    "tile_id": tile_id,
                    "soln_idx": int(tile_id) - 1,
                    "pol": pol,
                    "name": f"Tile{tile_id:03d}",
                    "id": tile_id,
                    "flavor": "RRI",
                    "rx": 1,
                    "length": 1.0,
                    "intercept": 0.0,
                    "sigma_resid": 0.01,
                    "chi2dof": 1.0,
                    "quality": 1.0,
                    "stderr": 0.001,
                    "outlier": False,
                }
            )
    phase_fits = pd.DataFrame(rows)

    freqs = np.linspace(1.5e8, 1.6e8, n_chan).astype(">f8")
    soln_xx = np.ones((len(tile_ids), n_chan), dtype=">c16")
    soln_yy = np.ones((len(tile_ids), n_chan), dtype=">c16")
    weights = np.ones(n_chan, dtype=">f8")

    assert freqs.dtype.byteorder == ">"
    assert soln_xx.dtype.byteorder == ">"

    # Must not raise (in particular, no AttributeError from a stale
    # ndarray.newbyteorder() call) and must return a non-empty pivot.
    result = plot_debug_phase_fits(
        phase_fits,
        tiles,
        freqs,
        soln_xx,
        soln_yy,
        weights,
        prefix="",
        show=False,
    )

    assert result is not None
    assert len(result) > 0
