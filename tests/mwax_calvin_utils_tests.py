"""
Tests for the mwax_calvin_utils.py module
"""

import mwalib
import mwax_mover
import mwax_mover.mwax_calvin_utils


def test_estimate_birli_output_GB():
    test_metafits = "tests/data/correlator_C001/1244973688_metafits.fits"

    metafits_context = mwalib.MetafitsContext(test_metafits, None)

    calc_bytes: float = mwax_mover.mwax_calvin_utils.estimate_birli_output_bytes(metafits_context, 40, 2.0)

    # Manually calculate the gigabytes
    # manual = baselines * coarse_channels * fine_channels * pols * values * bytes_per_value * timesteps
    manual_bytes: float = 8256 * 24 * 32 * 4 * 2 * 4 * 60

    assert calc_bytes == manual_bytes


def test_split_aocal_file_into_coarse_channels_24_per_file():
    in_filename = "tests/data/aocal_split/1451758560_solutions.bin"

    chans = [
        109,
        110,
        111,
        112,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
        121,
        122,
        123,
        124,
        125,
        126,
        127,
        128,
        129,
        130,
        131,
        132,
    ]

    out_files = mwax_mover.mwax_calvin_utils.split_aocal_file_into_coarse_channels(
        1451758560,
        in_filename,
        chans,
    )

    for c_idx, c in enumerate(chans):
        assert out_files[c_idx] == f"tests/data/aocal_split/1451758560_ch{chans[c_idx]}_aocal.bin"


def test_split_aocal_file_into_coarse_channels_1_per_file():
    in_filenames = [
        "tests/data/aocal_split/1450212840_ch101_solutions.bin",
        "tests/data/aocal_split/1450212840_ch107_solutions.bin",
        "tests/data/aocal_split/1450212840_ch113_solutions.bin",
        "tests/data/aocal_split/1450212840_ch120_solutions.bin",
        "tests/data/aocal_split/1450212840_ch127_solutions.bin",
        "tests/data/aocal_split/1450212840_ch134_solutions.bin",
        "tests/data/aocal_split/1450212840_ch142_solutions.bin",
        "tests/data/aocal_split/1450212840_ch150_solutions.bin",
        "tests/data/aocal_split/1450212840_ch158_solutions.bin",
        "tests/data/aocal_split/1450212840_ch167_solutions.bin",
        "tests/data/aocal_split/1450212840_ch177_solutions.bin",
        "tests/data/aocal_split/1450212840_ch187_solutions.bin",
        "tests/data/aocal_split/1450212840_ch210_solutions.bin",
        "tests/data/aocal_split/1450212840_ch226_solutions.bin",
        "tests/data/aocal_split/1450212840_ch58_solutions.bin",
        "tests/data/aocal_split/1450212840_ch61_solutions.bin",
        "tests/data/aocal_split/1450212840_ch65_solutions.bin",
        "tests/data/aocal_split/1450212840_ch69_solutions.bin",
        "tests/data/aocal_split/1450212840_ch73_solutions.bin",
        "tests/data/aocal_split/1450212840_ch77_solutions.bin",
        "tests/data/aocal_split/1450212840_ch81_solutions.bin",
        "tests/data/aocal_split/1450212840_ch86_solutions.bin",
        "tests/data/aocal_split/1450212840_ch91_solutions.bin",
        "tests/data/aocal_split/1450212840_ch96_solutions.bin",
    ]

    chans = [
        58,
        61,
        65,
        69,
        73,
        77,
        81,
        86,
        91,
        96,
        101,
        107,
        113,
        120,
        127,
        134,
        142,
        150,
        158,
        167,
        177,
        187,
        210,
        226,
    ]

    all_files = []

    for f_idx, f in enumerate(in_filenames):
        out_files = mwax_mover.mwax_calvin_utils.split_aocal_file_into_coarse_channels(
            1450212840,
            f,
            [
                chans[f_idx],
            ],
        )

        for of in out_files:
            all_files.append(of)

    for c_idx, c in enumerate(chans):
        assert f"tests/data/aocal_split/1450212840_ch{chans[c_idx]}_aocal.bin" in all_files[c_idx]
