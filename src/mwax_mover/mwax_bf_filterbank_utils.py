from typing import List
import logging
import os
import re
import shutil

HEADER_END = "HEADER_END"
HEADER_END_BYTES = b"HEADER_END"
KEY_DATALEN = "datalen"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB


def get_filterbank_components(filename: str) -> tuple[bytearray, int]:
    # Returns the (header, data_start) as bytearray and the start byte location respectively
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


# Returns a new header after modifying a key's value
def set_filterbank_key_value_int(header: bytearray, key: str, value: int) -> bytearray:
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


# Stitches filterbank files together and writes an output file- output filename is returned
def stitch_filterbank_files(logger: logging.Logger, files: List[str]) -> str:
    if len(files) == 0:
        raise Exception("No filterbank files to stitch")

    output_filename: str = get_stitched_filename(files[0])

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
    """
    Convert '[path]/obsid_subobs_chXXX_beamNN.vdif'
    into    '[path]/obsid_chXXX_beamNN.vdif'.

    obsid  = 10 digits
    subobs = 10 digits
    XXX    = 3 digits (zero padded)
    NN     = 2 digits (zero padded)
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
    """
    Convert '[path]/obsid_subobs_chXXX_beamNN.vdif'
    into    '[path]/obsid_chXXX_beamNN.vdif'.

    obsid  = 10 digits
    subobs = 10 digits
    XXX    = 3 digits (zero padded)
    NN     = 2 digits (zero padded)
    """
    file_path, obsid, _, chan, beam = get_filterbank_filename_components(filename)

    return os.path.join(file_path, f"{obsid}_ch{chan:03d}_beam{beam:02d}.fil")
