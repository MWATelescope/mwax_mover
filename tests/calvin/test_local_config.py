"""Tests for calvin.local_config: the settings mwax_dl_conv_cal_image reads from a calvin_processor.cfg."""

import os
import re

import pytest
import tests_common

from mwax_mover.calvin.local_config import LocalConfigError, LocalSettings, check_files_exist, read_local_config

SECTIONS_THE_TOOL_NEEDS = ["birli", "hyperdrive", "wsclean", "processing", "downloading"]


def _cfg_text() -> str:
    """The text of the rendered processor test config (all sections, with paths filled in)."""
    tests_common.setup_test_directories("mwax_calvin_processor")
    with open(tests_common.render_test_config("mwax_calvin_processor"), encoding="UTF-8") as f:
        return f.read()


def _write(tmp_path, text: str) -> str:
    path = tmp_path / "calvin_processor.cfg"
    path.write_text(text, encoding="UTF-8")
    return str(path)


def _only_sections(text: str, keep: list[str]) -> str:
    """Keep only the named sections of a config text."""
    parts = re.split(r"(?m)^(?=\[)", text)
    return "".join(p for p in parts if p.startswith("[") and p[1 : p.index("]")] in keep)


def _without_key(text: str, section: str, key: str) -> str:
    """Remove one key line from one section."""
    out, in_section, removed = [], False, False
    for line in text.splitlines(keepends=True):
        if line.startswith("["):
            in_section = line.strip() == f"[{section}]"
        if in_section and re.match(rf"{key}\s*=", line):
            removed = True
            continue
        out.append(line)
    assert removed, f"[{section}] {key} is not in the test config"
    return "".join(out)


def test_reads_a_real_processor_config(tmp_path):
    settings = read_local_config(_write(tmp_path, _cfg_text()))

    assert isinstance(settings, LocalSettings)
    assert settings.birli_timeout == 3600
    assert settings.birli_max_mem_gib == 830
    assert settings.birli_freq_res_khz == 40
    assert settings.birli_int_time_res_sec == 2.0
    assert settings.hyperdrive_timeout == 7200
    assert settings.hyperdrive_apply_timeout == 7200
    assert settings.hyperdrive_num_sources == 99
    assert settings.hyperdrive_source_list_type == "rts"
    assert "--uvw-min 50m" in settings.hyperdrive_extra_args
    assert settings.phase_fit_niter == 3
    assert settings.gains_cut_off_max == 100.0
    assert settings.gain_outlier_modify_gains is False
    assert settings.keep_completed_visibility_files is False
    assert settings.wsclean_timeout == 14400
    assert settings.wsclean_max_mem_gib == 830
    assert settings.wsclean_args.size == (3072, 3072)
    assert settings.wsclean_args.channels_out == 4
    assert settings.download_retries == 3
    assert settings.download_retry_wait == 240
    assert settings.download_timeout == 3600
    assert settings.https_proxy == "http://localhost:3128"
    assert settings.no_proxy == "asvo.mwatelescope.org"


def test_does_not_need_the_database_upload_giant_squid_or_job_path_settings(tmp_path):
    text = _only_sections(_cfg_text(), SECTIONS_THE_TOOL_NEEDS)
    text = re.sub(
        r"(?m)^(job_input_path|job_output_path|temp_working_path|cal_export_path|plot_upload_path|"
        r"plot_front_end_url|acacia_projects_profile|acacia_projects_bucket)\s*=.*\n",
        "",
        text,
    )

    settings = read_local_config(_write(tmp_path, text))

    assert settings.birli_timeout == 3600
    assert settings.phase_fit_niter == 3


def test_reads_the_wsclean_options_even_if_imaging_is_not_enabled_in_the_config(tmp_path):
    text = _cfg_text()
    assert "[wsclean]\nenabled=0" in text

    assert read_local_config(_write(tmp_path, text)).wsclean_args.scale == "0.75amin"


def test_proxy_settings_are_optional(tmp_path):
    text = _without_key(_without_key(_cfg_text(), "downloading", "https_proxy"), "downloading", "no_proxy")

    settings = read_local_config(_write(tmp_path, text))

    assert settings.https_proxy is None
    assert settings.no_proxy is None


@pytest.mark.parametrize("value", ["none", "off", ""])
def test_gains_cut_off_max_that_is_not_a_number_means_off(tmp_path, value):
    text = _cfg_text().replace("gains_cut_off_max=100", f"gains_cut_off_max={value}")

    assert read_local_config(_write(tmp_path, text)).gains_cut_off_max is None


def test_gains_cut_off_max_is_optional(tmp_path):
    text = _without_key(_cfg_text(), "processing", "gains_cut_off_max")

    assert read_local_config(_write(tmp_path, text)).gains_cut_off_max is None


@pytest.mark.parametrize(
    ("section", "key"),
    [
        ("birli", "binary_path"),
        ("birli", "freq_res_khz"),
        ("hyperdrive", "apply_timeout"),
        ("hyperdrive", "source_list_filename"),
        ("processing", "phase_fit_niter"),
        ("processing", "tile_bad_channel_fraction"),
        ("wsclean", "timeout"),
        ("wsclean", "max_mem_gib"),
        ("wsclean", "scale"),
        ("downloading", "download_retries"),
    ],
)
def test_a_missing_key_is_named_in_the_error(tmp_path, section, key):
    text = _without_key(_cfg_text(), section, key)

    with pytest.raises(LocalConfigError, match=re.escape(f"[{section}]") + ".*" + key):
        read_local_config(_write(tmp_path, text))


def test_a_missing_section_is_named_in_the_error(tmp_path):
    text = _only_sections(_cfg_text(), ["birli", "wsclean", "processing", "downloading"])

    with pytest.raises(LocalConfigError, match=r"\[hyperdrive\]"):
        read_local_config(_write(tmp_path, text))


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "timeout=3600\nmax_mem_gib=830\nfreq_res_khz",
            "timeout=soon\nmax_mem_gib=830\nfreq_res_khz",
            r"\[birli\] timeout must be a int",
        ),
        (
            "tile_bad_channel_fraction=0.5",
            "tile_bad_channel_fraction=half",
            r"\[processing\] tile_bad_channel_fraction must be a float",
        ),
        ("gain_outlier_modify_gains=0", "gain_outlier_modify_gains=maybe", r"\[processing\] gain_outlier_modify_gains"),
        ("size=3072 3072", "size=big", r"\[wsclean\].*size"),
        ("channels_out=4", "channels_out=1", r"\[wsclean\].*join_channels"),
    ],
)
def test_a_bad_value_is_named_in_the_error(tmp_path, old, new, message):
    text = _cfg_text()
    assert old in text
    text = text.replace(old, new, 1)

    with pytest.raises(LocalConfigError, match=message):
        read_local_config(_write(tmp_path, text))


def test_a_missing_file_is_an_error(tmp_path):
    with pytest.raises(LocalConfigError, match="does not exist"):
        read_local_config(str(tmp_path / "nope.cfg"))


def test_check_files_exist(tmp_path):
    settings = read_local_config(_write(tmp_path, _cfg_text()))
    bin_dir = tests_common.get_test_bin_dir("mwax_calvin_processor")
    for name in ("birli", "hyperdrive", "wsclean", "srclist.txt"):
        assert os.path.exists(os.path.join(bin_dir, name))

    assert check_files_exist(settings, need_birli=True) == []

    os.remove(os.path.join(bin_dir, "birli"))
    os.remove(os.path.join(bin_dir, "wsclean"))
    problems = check_files_exist(settings, need_birli=True)
    assert len(problems) == 2
    assert any("[birli] binary_path" in p for p in problems)
    assert any("[wsclean] binary_path" in p for p in problems)

    # Birli is not needed if the download already has measurement sets
    assert [p for p in check_files_exist(settings, need_birli=False) if "birli" in p] == []
