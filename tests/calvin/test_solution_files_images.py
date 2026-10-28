"""Tests for publishing the imaging outputs (calvin.solution_files.upload_image_files)."""

import json
from pathlib import Path

from mwax_mover.calvin.solution_files import find_image_files, reap_orphaned_staging_dirs, upload_image_files

OBS = "1234567890"


def _make_job_output(tmp_path: Path) -> Path:
    job = tmp_path / "job"
    job.mkdir()
    for name in [
        f"{OBS}_wsclean-image.fits",
        f"{OBS}_wsclean-dirty.fits",
        f"{OBS}_wsclean_readme.txt",
        f"{OBS}_ch75-76_solutions_apply_readme.txt",
        f"{OBS}_ch159_solutions_apply_readme.txt",
        # Not imaging outputs
        f"{OBS}_ch75-76_solutions.fits",
        f"{OBS}_metafits.fits",
        f"{OBS}_birli_readme.txt",
    ]:
        (job / name).write_text(name)
    return job


def test_find_image_files_lists_only_imaging_outputs(tmp_path):
    job = _make_job_output(tmp_path)

    assert [Path(f).name for f in find_image_files(str(job))] == [
        f"{OBS}_ch159_solutions_apply_readme.txt",
        f"{OBS}_ch75-76_solutions_apply_readme.txt",
        f"{OBS}_wsclean-dirty.fits",
        f"{OBS}_wsclean-image.fits",
        f"{OBS}_wsclean_readme.txt",
    ]


def test_upload_image_files_publishes_atomically_copying_fits_and_moving_readmes(tmp_path):
    job = _make_job_output(tmp_path)
    plots = tmp_path / "plots"
    plots.mkdir()
    upload_path = plots / "1768401673707300_images"
    index = {"version": 2, "files": [{"filename": f"{OBS}_wsclean-image.fits"}]}

    assert upload_image_files(str(job), str(upload_path), index) is True

    assert sorted(p.name for p in upload_path.iterdir()) == sorted(
        [
            f"{OBS}_ch159_solutions_apply_readme.txt",
            f"{OBS}_ch75-76_solutions_apply_readme.txt",
            f"{OBS}_wsclean-dirty.fits",
            f"{OBS}_wsclean-image.fits",
            f"{OBS}_wsclean_readme.txt",
            "index.json",
        ]
    )
    assert json.loads((upload_path / "index.json").read_text()) == index
    # FITS are copied (originals stay), readmes are moved
    assert (job / f"{OBS}_wsclean-image.fits").exists()
    assert (job / f"{OBS}_wsclean-dirty.fits").exists()
    assert not (job / f"{OBS}_wsclean_readme.txt").exists()
    assert not (job / f"{OBS}_ch75-76_solutions_apply_readme.txt").exists()
    # Other job files are untouched
    assert (job / f"{OBS}_ch75-76_solutions.fits").exists()
    assert (job / f"{OBS}_birli_readme.txt").exists()
    # No staging dir is left
    assert [p.name for p in plots.iterdir() if p.name.startswith(".")] == []


def test_upload_image_files_does_not_overwrite_an_existing_dir(tmp_path):
    job = _make_job_output(tmp_path)
    upload_path = tmp_path / "plots" / "1768401673707300_images"
    upload_path.mkdir(parents=True)

    assert upload_image_files(str(job), str(upload_path), {}) is False

    assert list(upload_path.iterdir()) == []
    assert (job / f"{OBS}_wsclean_readme.txt").exists()  # nothing was moved


def test_upload_image_files_with_no_wsclean_fits_publishes_nothing(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    (job / f"{OBS}_wsclean_readme.txt").write_text("failed run")
    upload_path = tmp_path / "plots" / "1768401673707300_images"
    (tmp_path / "plots").mkdir()

    assert upload_image_files(str(job), str(upload_path), {}) is False

    assert not upload_path.exists()
    assert (job / f"{OBS}_wsclean_readme.txt").exists()


def test_upload_image_files_replaces_a_stale_staging_dir(tmp_path):
    job = _make_job_output(tmp_path)
    plots = tmp_path / "plots"
    stale = plots / ".staging-1768401673707300_images"
    stale.mkdir(parents=True)
    (stale / "old.fits").write_text("old")
    upload_path = plots / "1768401673707300_images"

    assert upload_image_files(str(job), str(upload_path), {"files": []}) is True

    assert not (upload_path / "old.fits").exists()


def test_reap_orphaned_staging_dirs_also_removes_an_orphaned_images_staging_dir(tmp_path):
    orphan = tmp_path / ".staging-1768401673707300_images"
    orphan.mkdir()
    (orphan / "x.fits").write_text("x")

    assert reap_orphaned_staging_dirs(str(tmp_path), max_age_hours=0) == [str(orphan)]
    assert not orphan.exists()
