"""Tests for calvin.pipeline.process_solutions_files(): processing solutions on disk, with no database."""

import os
import shutil
from unittest.mock import patch

import pytest

from mwax_mover.calvin.pipeline import ProcessedSolutions, process_solutions_files

OBS_ID = 1365977896
DATA_DIR = os.path.join("tests", "data", str(OBS_ID))
METAFITS_PATH = os.path.join(DATA_DIR, f"{OBS_ID}_metafits.fits")
SOLUTIONS_PATH = os.path.join(DATA_DIR, f"{OBS_ID}_solutions.fits")


def _process(path: str):
    return process_solutions_files(
        obs_id=OBS_ID,
        input_data_path=path,
        output_data_path=path,
        phase_fit_niter=1,
        gain_max_cutoff=None,
        gain_outlier_poly_degree=2,
        gain_outlier_mad_residual_threshold=10,
        gain_outlier_plot_n_tiles_per_page=16,
        tile_bad_channel_fraction=0.5,
        hyperdrive_binary_path="/nonexistent/hyperdrive",
    )


@pytest.fixture
def work_dir(tmp_path):
    """A directory with the real metafits and solutions, or skip the test if they are missing."""
    for required in (METAFITS_PATH, SOLUTIONS_PATH):
        if not os.path.exists(required):
            pytest.skip(f"Required test data file not found: {required}")
    shutil.copy(METAFITS_PATH, tmp_path)
    shutil.copy(SOLUTIONS_PATH, tmp_path)
    return tmp_path


def test_process_solutions_files_returns_the_fits_the_database_insert_needs(work_dir):
    with (
        # See the note in test_pipeline.py on why these mocks are safe: only the plots and the
        # second phase fit are skipped, not the data that is returned.
        patch("mwax_mover.calvin.plots.hyperdrive.generate_plots_for_files", return_value=iter([])),
        patch("mwax_mover.calvin.pipeline.plot_outlier_gains", return_value=None),
        patch(
            "mwax_mover.calvin.pipeline.write_before_after_stats",
            side_effect=lambda group, obs_id, stats_fd, phase_outlier_nstd=3.0: group.phase_fits,
        ),
        patch("mwax_mover.calvin.pipeline.write_debug_phase_fit_plots", return_value=None),
    ):
        processed = _process(str(work_dir))

    assert isinstance(processed, ProcessedSolutions)
    assert len(processed.soln_tile_ids) > 0
    assert processed.n_metafits_coarse >= 1
    assert len(processed.all_metafits_coarse_chans) == processed.n_metafits_coarse
    assert len(processed.solution_coarse_chans) >= 1
    assert len(processed.gain_fits) > 0
    assert len(processed.phase_fits) > 0
    # The files are written, with no database
    assert (work_dir / f"{OBS_ID}_stats.txt").exists()
    assert (work_dir / f"{OBS_ID}_solutions.fits").exists()


def test_process_solutions_files_returns_none_if_all_tiles_are_flagged(tmp_path):
    if not os.path.exists(METAFITS_PATH):
        pytest.skip(f"Required metafits file not found: {METAFITS_PATH}")

    from astropy.io import fits

    with fits.open(METAFITS_PATH) as hdus:
        new_hdus = hdus.copy()
        new_hdus["TILEDATA"].data["Flag"][:] = 1
        new_hdus.writeto(str(tmp_path / f"{OBS_ID}_metafits.fits"), overwrite=True)
    shutil.copy(SOLUTIONS_PATH, str(tmp_path))

    assert _process(str(tmp_path)) is None
    assert not (tmp_path / f"{OBS_ID}_stats.txt").exists()


def test_process_solutions_files_raises_if_there_are_no_solution_files(tmp_path):
    if not os.path.exists(METAFITS_PATH):
        pytest.skip(f"Required metafits file not found: {METAFITS_PATH}")
    shutil.copy(METAFITS_PATH, str(tmp_path))

    with pytest.raises(Exception):  # noqa: B017, PT011 - any error must reach the caller; the type is not part of the contract
        _process(str(tmp_path))
