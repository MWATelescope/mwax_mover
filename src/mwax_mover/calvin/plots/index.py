"""index.json manifest generation for a calibration fit's uploaded files.

generate_plot_index_file() scans a fit directory and writes an index.json
manifest describing each file, suitable for upload to S3 alongside the
files themselves. populate_index_json_entry() builds one file's entry
(size, modification time, MIME type, PNG dimensions where applicable);
get_file_description() supplies its human-readable description field.
"""

import copy
import datetime
import fnmatch
import json
import logging
import mimetypes
import os
from pathlib import Path

from mwax_mover.constants import (
    EXT_FITS,
    INDEX_JSON_FILENAME,
    SOLUTIONS_APPLY_README_SUFFIX,
    SOLUTIONS_FITS_SUFFIX,
    SOLUTIONS_ORIGINAL_FITS_SUFFIX,
    WSCLEAN_FITS_GLOB,
    WSCLEAN_PNG_GLOB,
    WSCLEAN_README_SUFFIX,
)
from mwax_mover.filesystem.files import get_png_dimensions
from mwax_mover.filesystem.naming import extract_channels_from_filename

logger = logging.getLogger(__name__)

# Description of each known WSClean FITS output, by the end of its filename.
# Any other WSClean FITS output (e.g. from extra options) gets a generic description.
WSCLEAN_OUTPUT_DESCRIPTIONS = {
    "-image.fits": "WSClean restored image",
    "-dirty.fits": "WSClean dirty image",
    "-psf.fits": "WSClean point spread function (PSF) image",
    "-residual.fits": "WSClean residual image",
    "-model.fits": "WSClean model image",
}
WSCLEAN_OTHER_OUTPUT_DESCRIPTION = "WSClean output image"


def get_file_description(filename: str) -> str:
    """Given a filename, attempt to generate a description of it
    Args:
        filename: The filename to be described

    Returns:
        A human-readable description of the file, with a channel-range
        suffix appended where the filename identifies one, or "Miscellaneous
        file" if the filename doesn't match any known pattern.
    """

    # If it has a "chNNN" then this is a single coarse channel output
    chans = extract_channels_from_filename(filename)
    if chans is not None:
        chan_no = chans["start"]

        if "end" not in chans:
            channel_suffix = f" for receiver channel {chan_no} ({chan_no * 1.28:.3f} MHz)"
        else:
            chan_no_end = chans["end"]
            channel_suffix = (
                f" for receiver channels {chan_no}-{chan_no_end} ({chan_no * 1.28:.3f} - {chan_no_end * 1.28:.3f} MHz)"
            )
    else:
        channel_suffix = " for all coarse channels"

    desc = ""
    if filename.endswith(WSCLEAN_README_SUFFIX):
        desc = "Full log output of the WSClean run"
    elif filename.endswith(SOLUTIONS_APPLY_README_SUFFIX):
        desc = "Full log output of the Hyperdrive solutions-apply run"
    elif fnmatch.fnmatch(os.path.basename(filename), WSCLEAN_PNG_GLOB):
        desc = "PNG preview of the main WSClean image"
    elif fnmatch.fnmatch(os.path.basename(filename), WSCLEAN_FITS_GLOB):
        suffix = next((end for end in WSCLEAN_OUTPUT_DESCRIPTIONS if filename.endswith(end)), None)
        desc = WSCLEAN_OUTPUT_DESCRIPTIONS[suffix] if suffix else WSCLEAN_OTHER_OUTPUT_DESCRIPTION
    elif "birli_readme.txt" in filename:
        desc = "Full log output of the Birli run"
    elif "hyperdrive_readme.txt" in filename:
        desc = "Full log output of the Hyperdrive run"
    elif "intercepts.png" in filename:
        desc = (
            "Plots showing, for each receiver type and polarisation, a plot of the"
            " phase intercepts in polar coordinates vs cable length"
        )
    elif "phase_fits_xx.png" in filename:
        desc = "Plot of the phase fit for each tile (phase vs frequency) for XX"
    elif "phase_fits_yy.png" in filename:
        desc = "Plot of the phase fit for each tile (phase vs frequency) for YY"
    elif "phase_fits.tsv" in filename:
        desc = "Tab separated values (TSV) file containing all of the phase fit statistics per tile"
    elif "rx_lengths.png" in filename:
        desc = "Cable length offsets in metres per receiver"
    elif "solutions_amps.png" in filename:
        desc = "Calibration solution amplitudes vs fine channel per tile"
    elif "solutions_phases.png" in filename:
        desc = "Calibration solution phase vs fine channel per tile"
    elif "solutions_amps_original.png" in filename:
        desc = "Original unmodified Hyperdrive calibration solution amplitudes vs fine channel per tile"
    elif "solutions_phases_original.png" in filename:
        desc = "Original unmodified Hyperdrive calibration solution phase vs fine channel per tile"
    elif "stats.txt" in filename:
        desc = "Before/after per-tile flagging stats, followed by Hyperdrive fine channel convergence statistics"
    elif "residual.tsv" in filename:
        desc = "Tab separated value (TSV) file of phase residuals vs frequency by receiver type and polarisation"
    elif "residual.png" in filename:
        desc = "Plot of phase residuals vs frequency by receiver type and polarisation"
    elif "gain_outliers_tiles" in filename:
        desc = "Plot of outlier gains that were removed from the calibration solutions"
    elif "_solutions.fits" in filename:
        desc = "Hyperdrive calibration solutions in FITS format."
    elif "_solutions.original.fits" in filename:
        desc = "Original unmodified Hyperdrive calibration solutions out of Hyperdrive in FITS format"

    if desc == "":
        return "Miscellaneous file"
    else:
        return f"{desc}{channel_suffix}"


def generate_plot_index_file(
    fit_id: int, plot_front_end_url: str, fit_dir: str, output_filename: str
) -> tuple[bool, dict]:
    """Scans the specified directory (non-recursively) and produces a JSON manifest
    describing each file, suitable for upload to S3 alongside the files themselves.
    The manifest includes a CloudFront URL and MIME type for each file.

    Args:
        fit_id: the fit_id of this calibration. We use this to create a dir in the plot_upload_path
        plot_front_end_url: URL base to retrieve the file. E.g. https://s3blah
        fit_dir: Directory containing the fit to index
        output_filename: full path and name of the JSON file to write

    Returns:
        bool: Success / failure
        dict: The JSON generated (if successful)
    """
    try:
        if not os.path.isdir(fit_dir):
            raise NotADirectoryError(f"Not a valid directory: {fit_dir}")

        files = []
        for filename in sorted(os.scandir(fit_dir), key=lambda e: e.name):
            if not filename.is_file():
                continue
            if filename.name == INDEX_JSON_FILENAME:
                continue

            new_entry = populate_index_json_entry(Path(filename), fit_id, plot_front_end_url)

            # None means it found a file we don't want to upload so skip it
            if new_entry is not None:
                files.append(new_entry)

        index = {
            "version": 2,
            "generated_at": datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "base_url": plot_front_end_url,
            "path": str(fit_id),
            "files": files,
        }

        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2)

        return True, index
    except Exception:
        # log it and return
        logger.exception(f"Problem generating the {output_filename} file for fit {fit_id}")
        return False, {}


def populate_index_json_entry(filename: str | Path, fit_id: int, plot_front_end_url: str) -> dict | None:
    """Builds an index.json file entry dict for a given directory entry.

    Inspects the file at ``filename``, extracts metadata (size, modification
    time, MIME type, and PNG dimensions where applicable), and returns a dict
    suitable for inclusion in the ``files`` list of an index.json file.

    Only ``.png``, ``.tsv``, ``.txt`` and ``.fits`` files are supported; all
    other extensions return ``None``. Of the ``.fits`` files, only those ending
    ``solutions.fits`` or ``solutions.original.fits``, and the WSClean outputs
    (matching WSCLEAN_FITS_GLOB), are accepted -- this deliberately excludes
    the visibility and metafits FITS files.

    Args:
        filename: A str or Path representing the file to describe.
            Must refer to an existing, stat-able file.
        fit_id: The integer fit ID, used to construct the S3 path component
            of the entry's ``url``.
        plot_front_end_url: Base URL of the calibration plot front end
            (e.g. ``"https://cal.mwatelescope.org"``). Combined with
            ``fit_id`` and the filename to form the full entry URL.

    Returns:
        A dict containing the index.json entry fields (``filename``, ``url``,
        ``size_bytes``, ``last_modified``, ``content_type``, ``description``,
        and, for PNG files, ``image_width`` and ``image_height``), or ``None``
        if the file extension is not one of ``.png``, ``.tsv``, or ``.txt``.

    Raises:
        OSError: If the file cannot be stat'd.
        Exception: Any exception raised by :func:`mwax_mover.filesystem.files.get_png_dimensions`
            for PNG files is propagated to the caller.
    """
    path = Path(filename)
    _, ext = os.path.splitext(path.name)

    if ext not in (".png", ".tsv", ".txt", EXT_FITS):
        return None

    # Now check for other files which slip through
    if ext == EXT_FITS and not (
        str(path).endswith(SOLUTIONS_FITS_SUFFIX)
        or str(path).endswith(SOLUTIONS_ORIGINAL_FITS_SUFFIX)
        or fnmatch.fnmatch(path.name, WSCLEAN_FITS_GLOB)
    ):
        # Ignore the visibility FITS files and metafits files
        return None

    stat = path.stat()
    last_modified = datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.UTC)
    mime_type, _ = mimetypes.guess_type(path.name)

    is_png = ext == ".png"
    width, height = None, None

    if is_png:
        width, height = get_png_dimensions(str(path))

    return {
        "filename": path.name,
        "url": f"{plot_front_end_url}/{fit_id}/{path.name}",
        "size_bytes": stat.st_size,
        "last_modified": last_modified.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "content_type": mime_type or "application/octet-stream",
        "description": get_file_description(str(path)),
        **({"image_width": width, "image_height": height} if is_png else {}),
    }


def extend_plot_index(index: dict, files: list[str], fit_id: int, plot_front_end_url: str) -> dict:
    """Add more files to a fit's index, e.g. the WSClean outputs published after the plots.

    An entry for a file that is already in the index is replaced by the new one.
    Files that populate_index_json_entry() does not accept are skipped.

    Args:
        index: The index dict returned by generate_plot_index_file(). It is not changed.
        files: Full paths of the files to add. They must exist.
        fit_id: The fit ID, used to build each entry's URL.
        plot_front_end_url: Base URL of the calibration plot front end.

    Returns:
        A new index dict with the entries added (sorted by filename after the existing ones)
        and a new ``generated_at``.
    """
    new_index = copy.deepcopy(index)

    new_entries = []
    for file in sorted(files, key=os.path.basename):
        entry = populate_index_json_entry(file, fit_id, plot_front_end_url)
        if entry is None:
            logger.warning(f"extend_plot_index: {file} is not an indexable file. Skipping.")
            continue
        new_entries.append(entry)

    replaced_names = {entry["filename"] for entry in new_entries}
    new_index["files"] = [f for f in new_index["files"] if f["filename"] not in replaced_names] + new_entries
    new_index["generated_at"] = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    return new_index
