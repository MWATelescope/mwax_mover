"""Tests for core.command.write_readme_file().

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg). write_readme_file() itself moved from calvin/pipeline.py
to core/command.py in a post-restructure tweak -- these tests moved with
it. run_command()/start_command()/check_popen_finished() have no
dedicated tests yet.
"""

import logging
import os
from pathlib import Path

from mwax_mover.core.command import write_readme_file


def test_write_readme_success_exit_code_zero(tmp_path):
    fname = str(tmp_path / "readme_ok.txt")
    write_readme_file(fname, cmd="my_command arg1", exit_code=0, output="some output", error="")
    assert os.path.exists(fname)
    content = Path(fname).read_text()
    assert "succeeded" in content  # typo fixed in source: was "succeded"
    assert "my_command arg1" in content
    assert "some output" in content


def test_write_readme_failure_exit_code_nonzero(tmp_path):
    fname = str(tmp_path / "readme_fail.txt")
    write_readme_file(fname, cmd="bad_command", exit_code=1, output="", error="something went wrong")
    content = Path(fname).read_text()
    assert "failed" in content
    assert "something went wrong" in content


def test_write_readme_includes_exit_code(tmp_path):
    fname = str(tmp_path / "readme_code.txt")
    write_readme_file(fname, cmd="cmd", exit_code=42, output="out", error="err")
    content = Path(fname).read_text()
    assert "42" in content


def test_write_readme_no_exception_on_bad_path(caplog):
    """An unwritable path should not raise — it should log a warning."""
    bad_path = "/nonexistent/deeply/nested/path/readme.txt"
    with caplog.at_level(logging.WARNING):
        write_readme_file(bad_path, cmd="cmd", exit_code=0, output="", error="")
    assert any("Could not write" in r.message or bad_path in r.message for r in caplog.records)
