"""Utilities for reading, modifying, and stitching Sigproc filterbank (.fil) files.

Provides functions to parse the variable-length binary header, read and write
integer key-value pairs within it, and concatenate multiple per-subobservation
filterbank files produced by the MWAX beamformer into a single complete
observation output file.
"""

from typing import List
import logging
import os
import re
import shutil

logger = logging.getLogger(__name__)

HEADER_END = "HEADER_END"
HEADER_END_BYTES = b"HEADER_END"
KEY_DATALEN = "datalen"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB


def get_filterbank_components(filename: str) -> tuple[bytearray, int]:
    """Parse a Sigproc filterbank file and return its header and data start position.

    Args:
        filename: Path to the filterbank (.fil) file.

    Returns:
        A tuple of (header: bytearray, data_start_index: int), where header contains
        the complete header including HEADER_END marker, and data_start_index is the
        byte offset where the raw binary data begins.

    Raises:
        RuntimeError: If file ends unexpectedly before HEADER_END marker is found.
    """
    with open(filename, "rb") as f:
        header_bytes = bytearray()
        while True:
            b = f.read(1)
            if not b:
                raise RuntimeError(f"Unexpected EOF before {HEADER_END}")
            header_bytes += b
            if HEADER_END_BYTES in header_bytes:
                break

        # Store the exact header length
        header_end_index = header_bytes.index(HEADER_END_BYTES) + len(HEADER_END)

        # each key value is stored like this:
        # 4 bytes=length of next keyword
        # N bytes=keyword
        # X bytes=value (int or string or etc)

        # The rest of the file is raw binary data
        return (header_bytes, header_end_index)


def get_filterbank_key_value_int(header: bytearray, key: str) -> int:
    """Retrieve an integer key-value pair from a filterbank file header.

    Args:
        header: The filterbank header as a bytearray.
        key: The key name to look up.

    Returns:
        The integer value associated with the key.

    Raises:
        ValueError: If the key is not found in the header.
    """
    key_bytes = key.encode("utf-8")
    cuml_bytes = bytearray()
    for b in header:
        cuml_bytes.append(b)

        # The value we want will be the next 4 bytes
        if key_bytes in cuml_bytes:
            start_idx = len(cuml_bytes)
            value_bytes = header[start_idx : start_idx + 4]
            return int.from_bytes(value_bytes, "little", signed=False)
    raise ValueError(f"Key {key} not found in filterbank file")


def set_filterbank_key_value_int(header: bytearray, key: str, value: int) -> bytearray:
    """Modify an integer key-value pair in a filterbank file header.

    Args:
        header: The filterbank header as a bytearray.
        key: The key name to modify.
        value: The new integer value to set.

    Returns:
        The modified header bytearray.

    Raises:
        ValueError: If the key is not found in the header.
    """
    key_bytes = key.encode("utf-8")
    cuml_bytes = bytearray()
    for b in header:
        cuml_bytes.append(b)

        # The value we want to replace will be the next 4 bytes
        if key_bytes in cuml_bytes:
            start_idx = len(cuml_bytes)
            header[start_idx : start_idx + 4] = value.to_bytes(4, "little", signed=False)
            return header

    raise ValueError(f"Key {key} not found in filterbank file")


def stitch_filterbank_files(files: List[str], output_dir: str) -> str:
    """Concatenate multiple filterbank files into a single observation output file.

    Combines per-subobservation filterbank files produced by the MWAX beamformer
    into a single complete observation file. Uses the header from the first file
    and concatenates all data sections.

    Args:
        files: List of filterbank file paths to stitch together.
        output_dir: Directory where the output stitched file will be written.

    Returns:
        Path to the output stitched filterbank file.

    Raises:
        Exception: If the files list is empty.
    """
    if len(files) == 0:
        raise Exception("No filterbank files to stitch")

    output_filename: str = get_stitched_filename(files[0])
    output_filename = os.path.join(output_dir, os.path.basename(output_filename))

    if len(files) == 1:
        # Nothing to stitch but we still need the output_filename to be created, so copy the file
        logger.debug(f"Only one filterbank file, no stiching needed: copying {files[0]} to {output_filename}")
        shutil.copyfile(files[0], output_filename)
        return output_filename

    sorted_files = sorted(files)

    logger.info(f"Stitching {len(files)} filterbank files: {files[0]}...{files[-1]}")

    # The first file header will be the one we will use
    first_header = bytearray()
    all_data_start_indices: List[int] = []

    for filename in sorted_files:
        # Read each header and get the start index of data
        this_header, this_data_start_indicies = get_filterbank_components(filename)

        if len(first_header) == 0:
            first_header = this_header

        all_data_start_indices.append(this_data_start_indicies)

    # Create new filterbank file
    # --- Write new file ---
    with open(output_filename, "wb") as out_file:
        # write the header
        out_file.write(first_header)

        # Read the data so we can write it to the new file
        # Now loop through all files and write data
        for file_index, filename in enumerate(sorted_files):
            with open(filename, "rb") as data_file:
                data_file.seek(all_data_start_indices[file_index])

                while True:
                    chunk = data_file.read(CHUNK_SIZE)

                    if not chunk:
                        break
                    out_file.write(chunk)

    logger.info(f"Successfully stitched filterbank files into {output_filename}")
    return output_filename


def get_filterbank_filename_components(filename: str) -> tuple[str, int, int, int, int]:
    """Parse filename components from a per-subobservation filterbank file path.

    Extracts path, observation ID, sub-observation ID, channel, and beam from
    a filename matching the pattern: /path/obsid_subobs_chXXX_beamNN.fil

    Args:
        filename: The filterbank filename to parse.

    Returns:
        A tuple of (path: str, obsid: int, subobs: int, channel: int, beam: int).

    Raises:
        ValueError: If filename does not match the expected format.
    """
    pattern = r"^(?P<path>.*)/(?P<obsid>\d{10})_(?P<subobs>\d{10})_ch(?P<chan>\d{3})_beam(?P<beam>\d{2})\.fil$"
    m = re.match(pattern, filename)

    if not m:
        raise ValueError(f"Filename does not match expected format: {filename}")

    file_path = str(m.group("path"))
    obsid = int(m.group("obsid"))
    subobsid = int(m.group("subobs"))
    chan = int(m.group("chan"))
    beam = int(m.group("beam"))

    return file_path, obsid, subobsid, chan, beam


def get_stitched_filename(filename: str) -> str:
    """Generate output filename by removing sub-observation ID from input filename.

    Converts a per-subobservation filename to a complete observation filename:
    /path/obsid_subobs_chXXX_beamNN.fil -> /path/obsid_chXXX_beamNN.fil

    Args:
        filename: The per-subobservation filterbank filename.

    Returns:
        The output filename path with sub-observation ID removed.

    Raises:
        ValueError: If filename does not match the expected format.
    """
    file_path, obsid, _, chan, beam = get_filterbank_filename_components(filename)

    return os.path.join(file_path, f"{obsid}_ch{chan:03d}_beam{beam:02d}.fil")
