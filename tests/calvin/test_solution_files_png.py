"""The preview PNG is published with the other imaging outputs."""

import base64
import json
from pathlib import Path

from mwax_mover.calvin.plots.index import extend_plot_index, populate_index_json_entry
from mwax_mover.calvin.solution_files import find_image_files, upload_image_files

OBS = "1234567890"
FIT_ID = 1768401673707300
URL = "https://somes3url"


def _png_bytes() -> bytes:
    """The smallest valid PNG (1x1 pixel), so the index can read its size."""
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
    )


def _make_job(tmp_path: Path) -> Path:
    job = tmp_path / "job"
    job.mkdir()
    (job / f"{OBS}_wsclean-MFS-image.fits").write_text("fits")
    (job / f"{OBS}_wsclean-MFS-image.png").write_bytes(_png_bytes())
    (job / f"{OBS}_wsclean_readme.txt").write_text("readme")
    (job / "phase_fits_xx.png").write_bytes(_png_bytes())  # an ordinary plot, not an imaging output
    return job


def test_find_image_files_includes_the_preview_png_but_not_other_plots(tmp_path):
    job = _make_job(tmp_path)

    assert [Path(f).name for f in find_image_files(str(job))] == [
        f"{OBS}_wsclean-MFS-image.fits",
        f"{OBS}_wsclean-MFS-image.png",
        f"{OBS}_wsclean_readme.txt",
    ]


def test_the_preview_png_is_in_the_index_with_its_size_and_a_description(tmp_path):
    job = _make_job(tmp_path)

    entry = populate_index_json_entry(job / f"{OBS}_wsclean-MFS-image.png", FIT_ID, URL)

    assert entry is not None
    assert entry["content_type"] == "image/png"
    assert entry["image_width"] == 1
    assert entry["image_height"] == 1
    assert entry["description"].startswith("PNG preview of the main WSClean image")
    assert entry["url"] == f"{URL}/{FIT_ID}/{OBS}_wsclean-MFS-image.png"


def test_the_preview_png_is_published_in_the_images_dir_and_the_index(tmp_path):
    job = _make_job(tmp_path)
    plots = tmp_path / "plots"
    plots.mkdir()
    index = extend_plot_index({"version": 2, "generated_at": "", "files": []}, find_image_files(str(job)), FIT_ID, URL)

    assert upload_image_files(str(job), str(plots / f"{FIT_ID}_images"), index) is True

    images_dir = plots / f"{FIT_ID}_images"
    assert (images_dir / f"{OBS}_wsclean-MFS-image.png").exists()
    # The PNG is moved (it is small and has no other use). The FITS file is copied.
    assert not (job / f"{OBS}_wsclean-MFS-image.png").exists()
    assert (job / f"{OBS}_wsclean-MFS-image.fits").exists()
    # An ordinary plot is not touched
    assert (job / "phase_fits_xx.png").exists()
    published = json.loads((images_dir / "index.json").read_text())
    assert [f["filename"] for f in published["files"]] == [
        f"{OBS}_wsclean-MFS-image.fits",
        f"{OBS}_wsclean-MFS-image.png",
        f"{OBS}_wsclean_readme.txt",
    ]
