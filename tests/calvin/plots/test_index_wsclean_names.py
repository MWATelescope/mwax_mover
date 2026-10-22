"""The index must accept the file names WSClean makes with 4 output channels and 2 polarisations."""

import pytest

from mwax_mover.calvin.plots.index import populate_index_json_entry

NAMES = [
    "1234567890_wsclean-MFS-XX-image.fits",
    "1234567890_wsclean-MFS-YY-image.fits",
    "1234567890_wsclean-0000-XX-image.fits",
    "1234567890_wsclean-0003-YY-dirty.fits",
    "1234567890_wsclean-0001-XX-psf.fits",
    "1234567890_wsclean-MFS-XX-residual.fits",
    "1234567890_wsclean-0002-YY-model.fits",
]


@pytest.mark.parametrize("name", NAMES)
def test_index_accepts_multi_channel_multi_polarisation_names(tmp_path, name):
    path = tmp_path / name
    path.write_text("x")

    entry = populate_index_json_entry(path, 1768401673707300, "https://somes3url")

    assert entry is not None
    assert entry["description"].startswith("WSClean ")
    assert entry["description"] != "WSClean output image"
