"""PSRDADA subfile header reading/writing, and the external stats/ringbuffer
tools that act on subfiles.

Key constants: PSRDADA_HEADER_BYTES and the PSRDADA_* header keyword names.
Key functions: read_subfile_value(s)/read_subfile_trigger_value for reading
the ASCII header, inject_subfile_header()/inject_beamformer_headers() for
overwriting it in place, write_mock_subfile(_from_header) for building test
fixtures, and process_mwax_stats()/load_psrdada_ringbuffer()/
run_mwax_packet_stats()/copy_subfile_to_disk_dd() for running the external
mwax_stats, dada_diskdb, mwax_packet_stats and dd binaries against a subfile.
"""

import logging
import os
import time
from enum import Enum

from mwax_mover.core.command import run_command
from mwax_mover.core.env import running_under_pytest
from mwax_mover.core.units import bytes_to_gigabytes

logger = logging.getLogger(__name__)


# number of lines of the PSRDADA header to read looking for keywords
PSRDADA_HEADER_BYTES = 4096


# PSRDADA keywords
PSRDADA_MODE = "MODE"


PSRDADA_TRANSFER_SIZE = "TRANSFER_SIZE"


PSRDADA_OBS_ID = "OBS_ID"


PSRDADA_SUBOBS_ID = "SUBOBS_ID"


PSRDADA_TRIGGER_ID = "TRIGGER_ID"


PSRDADA_NINPUTS = "NINPUTS"


PSRDADA_COARSE_CHANNEL = "COARSE_CHANNEL"


class CorrelatorMode(Enum):
    """Class representing correlator mode"""

    NO_CAPTURE = "NO_CAPTURE"
    CORR_MODE_CHANGE = "CORR_MODE_CHANGE"
    MWAX_CORRELATOR = "MWAX_CORRELATOR"
    MWAX_VCS = "MWAX_VCS"
    MWAX_BUFFER = "MWAX_BUFFER"
    MWAX_BEAMFORMER = "MWAX_BEAMFORMER"
    MWAX_CORR_BF = "MWAX_CORR_BF"

    @staticmethod
    def is_no_capture(mode_string: str) -> bool:
        """Check if the mode indicates no capture or mode change.

        Args:
            mode_string: The correlator mode string to check.

        Returns:
            True if the mode is NO_CAPTURE or CORR_MODE_CHANGE, False otherwise.
        """
        return mode_string in [
            CorrelatorMode.NO_CAPTURE.value,
            CorrelatorMode.CORR_MODE_CHANGE.value,
        ]

    @staticmethod
    def is_correlator(mode_string: str) -> bool:
        """Check if the mode indicates a correlator observation.

        Args:
            mode_string: The correlator mode string to check.

        Returns:
            True if the mode is MWAX_CORRELATOR or MWAX_CORR_BF, False otherwise.
        """
        return mode_string in [
            CorrelatorMode.MWAX_CORRELATOR.value,
            CorrelatorMode.MWAX_CORR_BF.value,
        ]

    @staticmethod
    def is_vcs(mode_string: str) -> bool:
        """Check if the mode indicates a VCS observation.

        Args:
            mode_string: The correlator mode string to check.

        Returns:
            True if the mode is MWAX_VCS, False otherwise.
        """
        return mode_string in [
            CorrelatorMode.MWAX_VCS.value,
        ]

    @staticmethod
    def is_voltage_buffer(mode_string: str) -> bool:
        """Check if the mode indicates a voltage buffer observation.

        Args:
            mode_string: The correlator mode string to check.

        Returns:
            True if the mode is MWAX_BUFFER, False otherwise.
        """
        return mode_string == CorrelatorMode.MWAX_BUFFER.value

    @staticmethod
    def is_beamformer(mode_string: str) -> bool:
        """Check if the mode indicates a beamformer observation.

        Args:
            mode_string: The correlator mode string to check.

        Returns:
            True if the mode is MWAX_BEAMFORMER or MWAX_CORR_BF, False otherwise.
        """
        return mode_string in [
            CorrelatorMode.MWAX_BEAMFORMER.value,
            CorrelatorMode.MWAX_CORR_BF.value,
        ]


def process_mwax_stats(
    mwax_stats_dir: str,
    full_filename: str,
    numa_node: int | None,
    timeout: int,
    stats_dump_dir: str,
    metafits_path: str,
) -> bool:
    """
    Run the ``mwax_stats`` binary against an MWA visibility or voltage file.

    Constructs and executes a shell command of the form::

        <mwax_stats_dir>/mwax_stats -t <full_filename> -m <metafits_filename> -o <stats_dump_dir>

    Args:
        mwax_stats_dir: Directory containing the ``mwax_stats`` binary.
        full_filename: Full path to the data file to analyse. The observation ID
            is derived from the first 10 characters of the basename.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.
        stats_dump_dir: Directory where ``mwax_stats`` will write its output.
        metafits_path: Directory containing the metafits file for the observation.

    Returns:
        True if ``mwax_stats`` exited successfully, False otherwise.
    """
    # This code will execute the mwax stats command
    obs_id = str(os.path.basename(full_filename)[0:10])

    metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    cmd = f"{mwax_stats_dir}/mwax_stats -t {full_filename} -m {metafits_filename} -o {stats_dump_dir}"

    logger.debug(f"{full_filename}- attempting to run stats: {cmd}")

    start_time = time.time()
    return_value, stdout = run_command(cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} mwax_stats success in {elapsed:.3f} seconds")
    else:
        logger.error(f"{full_filename} mwax_stats failed with error {stdout}")

    return return_value


def load_psrdada_ringbuffer(full_filename: str, ringbuffer_key: str, numa_node, timeout: int) -> bool:
    """
    Load a subfile into a PSRDADA ring buffer using ``dada_diskdb``.

    Constructs and executes a shell command of the form::

        dada_diskdb -k <ringbuffer_key> -f <full_filename>

    Logs the transfer rate in Gbps on success.

    Args:
        full_filename: Full path to the ``.sub`` subfile to load.
        ringbuffer_key: Hexadecimal PSRDADA ring buffer key (e.g. ``'dada'``).
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.

    Returns:
        True if ``dada_diskdb`` exited successfully, False otherwise.
    """

    cmd = f"dada_diskdb -k {ringbuffer_key} -f {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()

    if running_under_pytest():
        logger.debug(
            f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key} (mocked as running in pytest)"
        )
        time.sleep(2)
        return_value = True
        stdout = ""
    else:
        logger.debug(f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key}")
        return_value, stdout = run_command(cmd, numa_node, timeout)

    elapsed = time.time() - start_time

    size_gigabytes = bytes_to_gigabytes(size)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(
            f"{full_filename} load_psrdada_ringbuffer success"
            f" ({size_gigabytes:.3f}GB in {elapsed:.3f} sec at"
            f" {gbps_per_sec:.3f} Gbps)"
        )
    else:
        logger.error(f"{full_filename} load_psrdada_ringbuffer failed with error {stdout}")

    return return_value


def run_mwax_packet_stats(mwax_stats_dir: str, full_filename: str, output_dir: str, numa_node, timeout: int) -> bool:
    """
    Run the ``mwax_packet_stats`` Rust binary against a subfile to generate packet statistics.

    Constructs and executes a shell command of the form::

        <mwax_stats_dir>/mwax_packet_stats -o <output_dir> -s <full_filename>

    Args:
        mwax_stats_dir: Directory containing the ``mwax_packet_stats`` binary.
        full_filename: Full path to the ``.sub`` subfile to analyse.
        output_dir: Directory where ``mwax_packet_stats`` will write its output.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.

    Returns:
        True if ``mwax_packet_stats`` exited successfully, False otherwise.
    """
    logger.debug(f"{full_filename}- attempting to execute mwax_packet_stats")

    cmd = f"{mwax_stats_dir}/mwax_packet_stats -o {output_dir} -s {full_filename}"

    start_time = time.time()
    return_value, stdout = run_command(cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} mwax_packet_stats success in {elapsed:.3f} sec")
    else:
        logger.error(f"{full_filename} mwax_packet_stats failed with error {stdout}")

    return return_value


def copy_subfile_to_disk_dd(
    filename: str,
    numa_node: int,
    destination_path: str,
    timeout: int,
    destination_filename: str,
    bytes_to_write: int,
) -> bool:
    """
    Copy the first N bytes of a subfile to disk using the system ``dd`` command.

    Used when subfiles are pre-allocated for a larger tile count than the
    observation actually uses (e.g. allocated for 144T but the observation is
    128T). The ``u2s`` process writes only the relevant data, so only the first
    ``bytes_to_write`` bytes need to be copied; the rest of the file is empty.

    Uses ``oflag=direct`` and ``iflag=count_bytes`` for efficient I/O, and logs
    the transfer rate in GB/sec on success.

    Note: ``destination_filename`` must be an actual filename — ``dd`` does not
    accept ``'.'`` as a destination.

    Args:
        filename: Full (or relative) path to the source subfile.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        destination_path: Destination directory path (no trailing slash needed).
        timeout: Maximum number of seconds to wait for the command to complete.
        destination_filename: Destination filename only (no path component).
            Must be a real filename, not ``'.'``.
        bytes_to_write: Number of bytes to copy from the start of the source file.

    Returns:
        True if ``dd`` exited successfully, False otherwise.
    """
    logger.debug(f"{filename}- Copying first {bytes_to_write} bytes of file into {destination_path}")

    command = (
        f"dd if={filename} of={destination_path}/{destination_filename}"
        f" bs=4M oflag=direct iflag=count_bytes count={bytes_to_write}"
    )

    start_time = time.time()
    retval, stdout = run_command(command, numa_node, timeout, False)

    if retval:
        elapsed = time.time() - start_time
        speed = bytes_to_gigabytes(bytes_to_write) / elapsed

        logger.info(
            f"{filename}- Copying first {bytes_to_write} bytes of file into"
            f" {destination_path}/{destination_filename} was successful"
            f" (took {elapsed:.3f} secs at {speed:.3f} GB/sec)."
        )
    else:
        logger.error(
            f"{filename}- Copying first {bytes_to_write} bytes of file into"
            f" {destination_path}/{destination_filename} failed with error"
            f" {stdout}"
        )

    return retval


def inject_subfile_header(subfile_filename: str, key_value_pairs: str):
    """
    Overwrite the last line of a PSRDADA subfile header with new key-value pairs.

    Reads the first ``PSRDADA_HEADER_BYTES`` (4096) bytes of the subfile,
    replaces the final line with ``key_value_pairs`` (padded with null bytes
    to preserve the exact header size), and writes the modified header back
    in place.

    Multiple key-value pairs should be separated by ``'\\n'``. Each pair
    must be space-separated (``'KEY VALUE'``) and end with a newline.

    Args:
        subfile_filename: Path to the ``.sub`` subfile to modify in place.
        key_value_pairs: String of one or more ``'KEY VALUE\\n'`` pairs to
            write into the last line of the header. Must not exceed the
            length of the existing last line.

    Raises:
        ValueError: If ``key_value_pairs`` is longer than the available space
            in the last line of the header.
        Exception: If the resulting header byte array is not exactly
            ``PSRDADA_HEADER_BYTES`` bytes long.
    """
    data = []

    # Read the psrdada header data in to a list (one line per item)
    with open(subfile_filename, "rb") as subfile:
        data = subfile.read(PSRDADA_HEADER_BYTES).decode("UTF-8").split("\n")

    last_line_index = len(data) - 1
    last_row_len = len(data[last_line_index])

    new_settings_len = len(key_value_pairs)

    if new_settings_len > last_row_len:
        raise ValueError(
            f"inject_subfile_header(): key_value_pairs length ({new_settings_len})"
            f" exceeds the available space in the last header line ({last_row_len})."
            " Cannot inject without corrupting the header."
        )

    null_trail = "\0" * (last_row_len - new_settings_len)
    data[last_line_index] = key_value_pairs + null_trail

    # convert our list of lines back to a byte array
    new_string = "\n".join(data)

    new_bytes = bytes(new_string, "UTF-8")
    if len(new_bytes) != PSRDADA_HEADER_BYTES:
        raise Exception(
            "inject_subfile_header(): new_bytes length is not"
            f" {PSRDADA_HEADER_BYTES} as expected it is {len(new_bytes)}."
            f" Newbytes = [{new_string}]"
        )

    # Overwrite the first 4096 bytes with our updated header
    with open(subfile_filename, "r+b") as subfile:
        subfile.seek(0)
        subfile.write(new_bytes)


def inject_beamformer_headers(subfile_filename: str, beamformer_settings: str):
    """
    Write beamformer settings into a PSRDADA subfile header.

    A thin wrapper around ``inject_subfile_header`` for the beamformer use case.
    NOTE: despite the name, this OVERWRITES the last line of the existing header
    rather than appending to it -- see inject_subfile_header.

    Args:
        subfile_filename: Path to the ``.sub`` subfile to modify in place.
        beamformer_settings: String of one or more ``'KEY VALUE\\n'`` pairs
            representing the beamformer configuration to inject.
    """
    inject_subfile_header(subfile_filename, beamformer_settings)


def read_subfile_value(filename: str, key: str) -> str | None:
    """
    Read a single keyword value from a PSRDADA subfile header.

    Reads the first ``PSRDADA_HEADER_BYTES`` (4096) bytes of the file and
    searches line-by-line for a line with exactly two whitespace-separated
    tokens whose first token matches ``key``.

    Args:
        filename: Path to the ``.sub`` subfile to read.
        key: The PSRDADA header keyword to look up (case-sensitive).

    Returns:
        The value string associated with ``key``, or None if the keyword is
        not found in the header.
    """
    subfile_value = None

    with open(filename, "rb") as subfile:
        subfile_text = subfile.read(PSRDADA_HEADER_BYTES).decode()
        subfile_text_lines = subfile_text.splitlines()

        for line in subfile_text_lines:
            split_line = line.split()

            # We should have 2 items, keyword and value
            if len(split_line) == 2:
                keyword = split_line[0].strip()
                value = split_line[1].strip()

                if keyword == key:
                    subfile_value = value
                    break

    return subfile_value


def read_subfile_values(filename: str, keys: list[str]) -> dict:
    """
    Read multiple keyword values from a PSRDADA subfile header in a single pass.

    Reads the first ``PSRDADA_HEADER_BYTES`` (4096) bytes of the file and
    searches line-by-line for lines with exactly two whitespace-separated
    tokens. Stops early once all requested keys have been found.

    Args:
        filename: Path to the ``.sub`` subfile to read.
        keys: A list of PSRDADA header keywords to look up (case-sensitive).

    Returns:
        A dict mapping each key in ``keys`` to its value string, or None for
        any keyword not found in the header.
    """
    subfile_values = {}

    # Create the dict with None values for all keys
    for key in keys:
        subfile_values[key] = None

    # Track which keys we have actually resolved. NOTE: this used to be a plain
    # counter incremented on every matching line, so a keyword appearing twice
    # in the header counted twice and could satisfy the early exit below before
    # every requested key had been seen.
    remaining = set(keys)

    with open(filename, "rb") as subfile:
        subfile_text = subfile.read(PSRDADA_HEADER_BYTES).decode()
        subfile_text_lines = subfile_text.splitlines()

        for line in subfile_text_lines:
            split_line = line.split()

            # We should have 2 items, keyword and value
            if len(split_line) == 2:
                keyword = split_line[0].strip()
                value = split_line[1].strip()

                if keyword in remaining:
                    subfile_values[keyword] = value
                    remaining.discard(keyword)

                    if not remaining:
                        # Exit loop early if we have all the values
                        break

    return subfile_values


def read_subfile_trigger_value(subfile_filename: str):
    """
    Read the ``TRIGGER_ID`` value from a PSRDADA subfile header.

    A convenience wrapper around ``read_subfile_value`` that casts the
    result to an integer.

    Args:
        subfile_filename: Path to the ``.sub`` subfile to read.

    Returns:
        The trigger ID as an int, or None if the ``TRIGGER_ID`` keyword is
        not present in the header.
    """
    value = read_subfile_value(subfile_filename, PSRDADA_TRIGGER_ID)

    if value:
        return int(value)
    else:
        return None


def write_mock_subfile_from_header(output_filename, header):
    """
    Write a mock PSRDADA subfile from a pre-built header string (for use in tests).

    Pads the header to exactly 4096 bytes with null bytes, then appends 256
    bytes of incrementing data (0x00–0xFF) to simulate a minimal subfile payload.

    Args:
        output_filename: Path to write the mock subfile to.
        header: ASCII string containing the PSRDADA header content. Must be
            shorter than 4096 bytes to leave room for null padding.
    """

    # Append the remainder of the 4096 bytes
    remainder_len = 4096 - len(header)
    padding = [0x0 for _ in range(remainder_len)]
    assert len(padding) == remainder_len
    # add 256 bytes of data to this subfile
    data_padding = [x for x in range(256)]
    assert len(data_padding) == 256

    # Convert to bytes
    test_header_bytes = bytes(header, "UTF-8")

    # Generate a test sub file
    with open(output_filename, "wb") as write_file:
        write_file.write(test_header_bytes)
        write_file.write(bytearray(padding))
        write_file.write(bytearray(data_padding))


def write_mock_subfile(
    output_filename,
    obs_id,
    subobs_id,
    mode,
    obs_offset,
    rec_channel,
    corr_channel,
):
    """
    Write a complete mock PSRDADA subfile for use in tests.

    Constructs a realistic PSRDADA ASCII header using the supplied parameters,
    pads it to 4096 bytes, and appends a 256-byte dummy data payload.

    Args:
        output_filename: Path to write the mock subfile to.
        obs_id: MWA observation ID to embed in the header (``OBS_ID``).
        subobs_id: Sub-observation ID to embed in the header (``SUBOBS_ID``).
        mode: Correlator mode string to embed in the header (``MODE``).
        obs_offset: Offset in seconds from the start of the observation
            (``OBS_OFFSET``).
        rec_channel: Receiver coarse channel number (``COARSE_CHANNEL``).
        corr_channel: Correlator coarse channel number
            (``CORR_COARSE_CHANNEL``).
    """
    # Create ascii header
    header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        f"OBS_ID {obs_id}\n"
        f"SUBOBS_ID {subobs_id}\n"
        f"MODE {mode}\n"
        "UTC_START 2023-01-13-03:33:10\n"
        f"OBS_OFFSET {obs_offset}\n"
        "NBIT 8\n"
        "NPOL 2\n"
        "NTIMESAMPLES 64000\n"
        "NINPUTS 256\n"
        "NINPUTS_XGPU 256\n"
        "APPLY_PATH_WEIGHTS 0\n"
        "APPLY_PATH_DELAYS 1\n"
        "APPLY_PATH_PHASE_OFFSETS 1\n"
        "INT_TIME_MSEC 500\n"
        "FSCRUNCH_FACTOR 200\n"
        "APPLY_VIS_WEIGHTS 0\n"
        "TRANSFER_SIZE 5275648000\n"
        "PROJ_ID G0060\n"
        "EXPOSURE_SECS 200\n"
        f"COARSE_CHANNEL {rec_channel}\n"
        f"CORR_COARSE_CHANNEL {corr_channel}\n"
        "SECS_PER_SUBOBS 8\n"
        "UNIXTIME 1673580790\n"
        "UNIXTIME_MSEC 0\n"
        "FINE_CHAN_WIDTH_HZ 40000\n"
        "NFINE_CHAN 32\n"
        "BANDWIDTH_HZ 1280000\n"
        "SAMPLE_RATE 1280000\n"
        "MC_IP 0.0.0.0\n"
        "MC_PORT 0\n"
        "MC_SRC_IP 0.0.0.0\n"
        "MWAX_U2S_VER 2.09-87\n"
        "IDX_PACKET_MAP 0+200860892\n"
        "IDX_METAFITS 32+1\n"
        "IDX_DELAY_TABLE 16383744+0\n"
        "IDX_MARGIN_DATA 256+0\n"
        "MWAX_SUB_VER 2\n"
    )

    write_mock_subfile_from_header(output_filename, header)
