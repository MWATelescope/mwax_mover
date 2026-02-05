from typing import List
import logging
import re
import shutil

HEADER_END = "HEADER_END"
HEADER_END_BYTES = b"HEADER_END"
KEY_NSAMPLES = "nsamples"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB


def get_filterbank_components(filename: str) -> tuple[str, int]:
    # Returns the (header, data_start) as string and the start byte location respectively
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
        header = header_bytes[:header_end_index].decode("ascii", errors="ignore")

        # The rest of the file is raw binary data
        return (header, header_end_index)


def get_filterbank_key_value(header: str, key: str) -> str:
    lines = header.splitlines()
    for line in lines:
        if line.startswith(key):
            # Line will be:
            # key value
            # value could also have spaces, so we only want to split the first space
            values = line.split(" ", 1)
            if len(values) == 2:
                return values[1]
            else:
                # We have the key but no value- return empty string
                return ""
    raise ValueError(f"Key {key} not found in filterbank file")


# Returns a new header after modifying a key's value
def set_filterbank_key_value(header: str, key: str, value) -> str:
    replaced: bool = False
    new_header: str = ""
    lines = header.splitlines()
    for line in lines:
        if line.startswith(key):
            # Line will be:
            # key value
            line = f"{key} {value}\n"
            replaced = True

        new_header += line

    if not replaced:
        raise ValueError(f"Key {key} not found in filterbank file")

    return new_header


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
    all_headers: List[str] = []
    all_n_samples: List[int] = []
    all_data_start_indices: List[int] = []

    for filename in sorted_files:
        # Read each header and get the start index of data
        this_header, this_data_start_indicies = get_filterbank_components(filename)

        # Get the nsamples value for this file from the header
        this_nsamples = get_filterbank_key_value(this_header, KEY_NSAMPLES)

        try:
            all_n_samples.append(int(this_nsamples))
        except ValueError:
            logger.warning(f"{KEY_NSAMPLES} in {filename} is not a number! {this_nsamples}")
            pass

        all_headers.append(this_header)
        all_data_start_indices.append(this_data_start_indicies)

    # Go through all files and get the total number of samples
    total_nsamples = sum(all_n_samples)

    # update the first header nsamples (this will be our header for the new file)
    new_header: str = set_filterbank_key_value(all_headers[0], KEY_NSAMPLES, total_nsamples)

    # Ensure new header and old header have same length!
    assert len(new_header) == len(all_headers[0])

    # Encode it into bytes
    new_header_bytes = new_header.encode("ascii")

    # Create new filterbank file
    # --- Write new file ---
    with open(output_filename, "wb") as out_file:
        # write the header
        out_file.write(new_header_bytes)

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


def get_stitched_filename(filename: str) -> str:
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

    file_path = m.group("path")
    obsid = m.group("obsid")
    chan = m.group("chan")
    beam = m.group("beam")

    return f"{file_path}/{obsid}_ch{chan}_beam{beam}.fil"
