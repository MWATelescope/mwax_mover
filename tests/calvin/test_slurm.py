"""Tests for calvin.slurm: the sbatch script's walltime."""

import re

import pytest

from mwax_mover.calvin.pipeline import CalvinJobType
from mwax_mover.calvin.slurm import create_sbatch_script, format_wall_time
from mwax_mover.constants import (
    DEFAULT_MWA_ASVO_WALL_TIME_SECS,
    DEFAULT_REALTIME_WALL_TIME_SECS,
    SLURM_USR1_WARNING_SECS,
)


def _script(jobtype: CalvinJobType, wall_time_secs: int | None = None, bulk_request: bool = False) -> str:
    kwargs = {} if wall_time_secs is None else {"wall_time_secs": wall_time_secs}
    return create_sbatch_script("/cfg.cfg", 1234567890, jobtype, "/logs", [1, 2], bulk_request, "", **kwargs)


def _time_line(script: str) -> str:
    match = re.search(r"^#SBATCH --time=(\S+)$", script, re.MULTILINE)
    assert match is not None
    return match.group(1)


@pytest.mark.parametrize(
    ("secs", "expected"),
    [
        (0, "00:00:00"),
        (59, "00:00:59"),
        (3600, "01:00:00"),
        (6 * 3600, "06:00:00"),
        (12 * 3600 + 30 * 60 + 5, "12:30:05"),
        (36 * 3600, "36:00:00"),
    ],
)
def test_format_wall_time(secs, expected):
    assert format_wall_time(secs) == expected


def test_default_wall_times_are_6_hours_for_realtime_and_12_for_mwa_asvo():
    assert DEFAULT_REALTIME_WALL_TIME_SECS == 6 * 3600
    assert DEFAULT_MWA_ASVO_WALL_TIME_SECS == 12 * 3600


def test_create_sbatch_script_uses_the_default_wall_time_per_job_type():
    assert _time_line(_script(CalvinJobType.realtime)) == "06:00:00"
    assert _time_line(_script(CalvinJobType.mwa_asvo)) == "12:00:00"


def test_create_sbatch_script_uses_the_given_wall_time():
    assert _time_line(_script(CalvinJobType.realtime, 5400)) == "01:30:00"
    assert _time_line(_script(CalvinJobType.mwa_asvo, 20 * 3600)) == "20:00:00"
    assert _time_line(_script(CalvinJobType.mwa_asvo, 20 * 3600, bulk_request=True)) == "20:00:00"


def test_create_sbatch_script_signals_the_job_before_the_wall_time():
    assert f"#SBATCH --signal=USR1@{SLURM_USR1_WARNING_SECS}" in _script(CalvinJobType.realtime)
