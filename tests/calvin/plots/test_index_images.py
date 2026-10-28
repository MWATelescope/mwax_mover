"""Tests for the WSClean entries of calvin.plots.index."""

import pytest

from mwax_mover.calvin.plots.index import extend_plot_index, get_file_description, populate_index_json_entry

FIT_ID = 1768401673707300
URL = "https://somes3url"


def _touch(tmp_path, name: str, content: str = "x") -> str:
    path = tmp_path / name
    path.write_text(content)
    return str(path)


@pytest.mark.parametrize(
    ("suffix", "description"),
    [
        ("-image.fits", "WSClean restored image"),
        ("-dirty.fits", "WSClean dirty image"),
        ("-psf.fits", "WSClean point spread function (PSF) image"),
        ("-residual.fits", "WSClean residual image"),
        ("-model.fits", "WSClean model image"),
        ("-MFS-image.fits", "WSClean restored image"),
        ("-0001-image.fits", "WSClean restored image"),
        ("-beam.fits", "WSClean output image"),
    ],
)
def test_populate_index_json_entry_accepts_wsclean_outputs(tmp_path, suffix, description):
    entry = populate_index_json_entry(_touch(tmp_path, f"1234567890_wsclean{suffix}"), FIT_ID, URL)

    assert entry is not None
    assert entry["filename"] == f"1234567890_wsclean{suffix}"
    assert entry["url"] == f"{URL}/{FIT_ID}/1234567890_wsclean{suffix}"
    assert entry["description"].startswith(description)


def test_populate_index_json_entry_still_ignores_other_fits(tmp_path):
    assert populate_index_json_entry(_touch(tmp_path, "1234567890_metafits.fits"), FIT_ID, URL) is None
    assert (
        populate_index_json_entry(_touch(tmp_path, "1234567890_20260101000000_gpubox01_00.fits"), FIT_ID, URL) is None
    )


def test_get_file_description_for_the_imaging_readmes():
    assert get_file_description("1234567890_wsclean_readme.txt").startswith("Full log output of the WSClean run")
    assert get_file_description("1234567890_ch75-76_solutions_apply_readme.txt").startswith(
        "Full log output of the Hyperdrive solutions-apply run"
    )
    assert get_file_description("1234567890_ch75-76_solutions_apply_readme.txt").endswith("(96.000 - 97.280 MHz)")


def test_extend_plot_index_adds_entries_and_leaves_the_original_unchanged(tmp_path):
    index = {
        "version": 2,
        "generated_at": "2020-01-01T00:00:00Z",
        "base_url": URL,
        "path": str(FIT_ID),
        "files": [{"filename": "phase_fits.png"}],
    }
    files = [
        _touch(tmp_path, "1234567890_wsclean-psf.fits"),
        _touch(tmp_path, "1234567890_wsclean-image.fits"),
        _touch(tmp_path, "1234567890_wsclean_readme.txt"),
    ]

    extended = extend_plot_index(index, files, FIT_ID, URL)

    assert [f["filename"] for f in extended["files"]] == [
        "phase_fits.png",
        "1234567890_wsclean-image.fits",
        "1234567890_wsclean-psf.fits",
        "1234567890_wsclean_readme.txt",
    ]
    assert extended["generated_at"] != "2020-01-01T00:00:00Z"
    assert extended["path"] == str(FIT_ID)
    assert extended["base_url"] == URL
    assert index["files"] == [{"filename": "phase_fits.png"}]
    assert index["generated_at"] == "2020-01-01T00:00:00Z"


def test_extend_plot_index_replaces_an_entry_with_the_same_filename(tmp_path):
    index = {"files": [{"filename": "1234567890_wsclean-image.fits", "size_bytes": 1}], "generated_at": ""}
    path = _touch(tmp_path, "1234567890_wsclean-image.fits", "0123456789")

    extended = extend_plot_index(index, [path], FIT_ID, URL)

    assert len(extended["files"]) == 1
    assert extended["files"][0]["size_bytes"] == 10


def test_extend_plot_index_skips_files_that_cannot_be_indexed(tmp_path):
    index = {"files": [], "generated_at": ""}

    extended = extend_plot_index(index, [_touch(tmp_path, "something.bin")], FIT_ID, URL)

    assert extended["files"] == []
