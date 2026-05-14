from mwax_mover.mwax_calvin_utils import generate_plot_index_file
import logging
import os

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


def test_generate_plot_file():
    fit_path = "tests/data/test021/1768401673707300"
    out_filename = os.path.join(fit_path, "index.json")

    # ensure out_filename is removed before we test
    if os.path.exists(out_filename):
        os.remove(out_filename)

    success, index = generate_plot_index_file(
        fit_id=1768401673707300,
        plot_front_end_url="https://somes3url",
        fit_dir=fit_path,
        output_filename=out_filename,
    )

    assert success
    assert os.path.exists(out_filename)

    # check that the number of files == the json number of files!
    files = [f for f in os.scandir(fit_path)]
    assert len(index["files"]) == len(files) - 1  # minus one due to the index.json we just created!
