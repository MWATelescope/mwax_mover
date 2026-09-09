"""Tests for fits.subfile: PSRDADA subfile header reading/writing.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).
"""

import os

from mwax_mover.fits.subfile import (
    CorrelatorMode,
    PSRDADA_HEADER_BYTES,
    PSRDADA_MODE,
    inject_beamformer_headers,
    read_subfile_value,
    read_subfile_values,
    write_mock_subfile,
    write_mock_subfile_from_header,
)


def test_correlator_mode_class():
    assert CorrelatorMode.is_no_capture("NO_CAPTURE")
    assert not CorrelatorMode.is_correlator("NO_CAPTURE")
    assert not CorrelatorMode.is_vcs("NO_CAPTURE")
    assert not CorrelatorMode.is_voltage_buffer("NO_CAPTURE")
    assert not CorrelatorMode.is_beamformer("NO_CAPTURE")

    assert not CorrelatorMode.is_no_capture("MWAX_CORRELATOR")
    assert CorrelatorMode.is_correlator("MWAX_CORRELATOR")
    assert not CorrelatorMode.is_vcs("MWAX_CORRELATOR")
    assert not CorrelatorMode.is_voltage_buffer("MWAX_CORRELATOR")
    assert not CorrelatorMode.is_beamformer("MWAX_CORRELATOR")

    assert not CorrelatorMode.is_no_capture("MWAX_VCS")
    assert not CorrelatorMode.is_correlator("MWAX_VCS")
    assert CorrelatorMode.is_vcs("MWAX_VCS")
    assert not CorrelatorMode.is_voltage_buffer("MWAX_VCS")
    assert not CorrelatorMode.is_beamformer("MWAX_VCS")

    assert not CorrelatorMode.is_no_capture("MWAX_BUFFER")
    assert not CorrelatorMode.is_correlator("MWAX_BUFFER")
    assert not CorrelatorMode.is_vcs("MWAX_BUFFER")
    assert CorrelatorMode.is_voltage_buffer("MWAX_BUFFER")
    assert not CorrelatorMode.is_beamformer("MWAX_BUFFER")

    assert not CorrelatorMode.is_no_capture("MWAX_BEAMFORMER")
    assert not CorrelatorMode.is_correlator("MWAX_BEAMFORMER")
    assert not CorrelatorMode.is_vcs("MWAX_BEAMFORMER")
    assert not CorrelatorMode.is_voltage_buffer("MWAX_BEAMFORMER")
    assert CorrelatorMode.is_beamformer("MWAX_BEAMFORMER")


def test_write_mock_subfile():
    """Test that our mock subfile is correct"""
    output_filename = "/tmp/test005_test_subfile1.sub"

    # Write out the mock subfile
    write_mock_subfile(
        output_filename,
        obs_id=1234567890,
        subobs_id=1234567898,
        mode="MWAX_VCS",
        obs_offset=8,
        rec_channel=123,
        corr_channel=5,
    )

    # This is what it should look like
    expected_header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        "OBS_ID 1234567890\n"
        "SUBOBS_ID 1234567898\n"
        "MODE MWAX_VCS\n"
        "UTC_START 2023-01-13-03:33:10\n"
        "OBS_OFFSET 8\n"
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
        "COARSE_CHANNEL 123\n"
        "CORR_COARSE_CHANNEL 5\n"
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

    # Read in the first 4096 bytes as text/ascii
    with open(output_filename, "rb") as subfile:
        # Read header
        header_bytes = subfile.read(len(expected_header))

    # convert bytes to ascii
    header_text = header_bytes.decode()

    assert header_text == expected_header


def test_inject_beamformer_headers():
    """Test that, given a sub file which has a 4096 byte header
    that we can find the end of header and 'paste' in the beamformer
    header to the end (and still maintain a 4096 byte header!)"""

    # Generate test beamformer settings
    beamformer_settings_string = (
        "NUM_INCOHERENT_BEAMS 2\n"
        "INCOHERENT_BEAM_01_CHANNELS 1280000\n"
        "INCOHERENT_BEAM_01_TIME_INTEG 1\n"
        "INCOHERENT_BEAM_02_CHANNELS 128\n"
        "INCOHERENT_BEAM_02_TIME_INTEG 100\n"
        "NUM_COHERENT_BEAMS 0\n"
    )

    # Generate a test header
    test_header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        "OBS_ID 1357616008\n"
        "SUBOBS_ID 1357623888\n"
        "MODE NO_CAPTURE\n"
        "UTC_START 2023-01-13-03:33:10\n"
        "OBS_OFFSET 7880\n"
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
        "COARSE_CHANNEL 169\n"
        "CORR_COARSE_CHANNEL 12\n"
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

    assert len(test_header) == 720

    # Append the remainder of the 4096 bytes
    remainder_len = PSRDADA_HEADER_BYTES - len(test_header)
    padding = [0x0 for _ in range(remainder_len)]
    assert len(padding) == remainder_len
    # add 255 bytes of data to this subfile
    data_padding = [x for x in range(256)]
    assert len(data_padding) == 256

    # Write the subfile
    subfile_name = "/tmp/test005_test_subfile2.sub"
    write_mock_subfile_from_header(subfile_name, test_header)

    # inject the beamformer settings
    inject_beamformer_headers(subfile_name, beamformer_settings_string)

    # Check file size
    assert os.path.getsize(subfile_name) == PSRDADA_HEADER_BYTES + len(bytearray(data_padding))

    # we can also test read_subfile_value(item, key)
    assert read_subfile_value(subfile_name, PSRDADA_MODE) == "NO_CAPTURE"
    assert read_subfile_value(subfile_name, "NUM_INCOHERENT_BEAMS") == "2"

    # check for None on a non-existent key
    assert read_subfile_value(subfile_name, "MISSING_KEY123") is None


def test_read_subfile_values():
    # Generate a test header
    test_header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        "OBS_ID 1357616008\n"
        "SUBOBS_ID 1357623888\n"
        "MODE NO_CAPTURE\n"
        "UTC_START 2023-01-13-03:33:10\n"
        "OBS_OFFSET 7880\n"
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
        "COARSE_CHANNEL 169\n"
        "CORR_COARSE_CHANNEL 12\n"
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
    # Write the subfile
    subfile_name = "/tmp/test005_test_subfile_3.sub"
    write_mock_subfile_from_header(subfile_name, test_header)

    keys = ["OBS_ID", "MODE", "EXPOSURE_SECS"]

    results = read_subfile_values(subfile_name, keys)

    assert results["OBS_ID"] == "1357616008"
    assert results["MODE"] == "NO_CAPTURE"
    assert results["EXPOSURE_SECS"] == "200"
