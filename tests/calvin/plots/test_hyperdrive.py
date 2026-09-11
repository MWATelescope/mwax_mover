"""Tests for calvin.plots.hyperdrive: generating solution plots via
the hyperdrive binary.

Split out of the former test023_calvin_plots.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

from pathlib import Path
from unittest.mock import patch

from mwax_mover.calvin.plots.hyperdrive import generate_plots, generate_plots_for_files


class TestGeneratePlotsRename:
    """Tests that the "before" rename only touches its own input's plots.

    The rename used to glob the whole output directory, which was only safe
    because it ran serially (an already-renamed file stops matching). That made
    the function impossible to parallelise and cost a full directory scan per
    file. These tests pin the scoped behaviour, since the real hyperdrive binary
    isn't available in the test environment.
    """

    @staticmethod
    def _fake_hyperdrive(output_dir, stem, suffixes=("amps", "phases")):
        """Return a run_command stand-in that creates hyperdrive's plots."""

        def _run(cmd, *args, **kwargs):
            for suffix in suffixes:
                Path(output_dir, f"{stem}_{suffix}.png").write_text("fake plot")
            return True, ""

        return _run

    def test_renames_only_its_own_files(self, tmp_path):
        """Another picket's plots in the same directory are left alone."""
        stem = "1391522232_ch62_solutions"
        # A sibling picket's plots, already sitting in the shared output dir
        other_amps = tmp_path / "1391522232_ch67_solutions_amps.png"
        other_amps.write_text("other picket")

        with patch(
            "mwax_mover.calvin.plots.hyperdrive.run_command",
            side_effect=self._fake_hyperdrive(tmp_path, stem),
        ):
            success, error = generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert success, error
        # Its own plots were renamed
        assert (tmp_path / f"{stem}_amps_original.png").exists()
        assert (tmp_path / f"{stem}_phases_original.png").exists()
        assert not (tmp_path / f"{stem}_amps.png").exists()
        # The other picket's plot was NOT touched
        assert other_amps.exists()
        assert not (tmp_path / "1391522232_ch67_solutions_amps_original.png").exists()

    def test_after_run_does_not_rename(self, tmp_path):
        """before=False leaves hyperdrive's filenames as produced."""
        stem = "1391522232_ch62_solutions"

        with patch(
            "mwax_mover.calvin.plots.hyperdrive.run_command",
            side_effect=self._fake_hyperdrive(tmp_path, stem),
        ):
            success, _ = generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=False,
            )

        assert success
        assert (tmp_path / f"{stem}_amps.png").exists()
        assert not (tmp_path / f"{stem}_amps_original.png").exists()

    def test_renames_unexpected_suffixes_too(self, tmp_path):
        """A suffix we didn't anticipate is still protected from being overwritten.

        The rename globs on the solution stem rather than hardcoding
        "_amps"/"_phases", so a hyperdrive version emitting a third plot type
        doesn't silently lose its "before" copy.
        """
        stem = "1391522232_ch62_solutions"

        with patch(
            "mwax_mover.calvin.plots.hyperdrive.run_command",
            side_effect=self._fake_hyperdrive(tmp_path, stem, suffixes=("amps", "phases", "delays")),
        ):
            generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert (tmp_path / f"{stem}_delays_original.png").exists()

    def test_already_renamed_files_are_not_double_renamed(self, tmp_path):
        """A second pass must not produce *_original_original.png."""
        stem = "1391522232_ch62_solutions"
        (tmp_path / f"{stem}_amps_original.png").write_text("from an earlier run")

        with patch(
            "mwax_mover.calvin.plots.hyperdrive.run_command",
            side_effect=self._fake_hyperdrive(tmp_path, stem, suffixes=("phases",)),
        ):
            generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert not (tmp_path / f"{stem}_amps_original_original.png").exists()
        assert (tmp_path / f"{stem}_amps_original.png").exists()

    def test_warns_when_hyperdrive_produced_nothing(self, tmp_path, caplog):
        """A success with no matching plots is surfaced, not silently ignored."""
        stem = "1391522232_ch62_solutions"

        with patch("mwax_mover.calvin.plots.hyperdrive.run_command", return_value=(True, "")):
            success, _ = generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert success
        assert "produced no plots matching" in caplog.text


class TestGeneratePlotsReftile:
    """Tests for --reftile, threaded through so hyperdrive's own plots use
    the same reference tile as calvin's internal calculations (see
    HyperfitsSolutionGroup.select_refant and docs/REF_TILE_SELECTION.md).
    """

    def test_reftile_omitted_by_default(self, tmp_path):
        """reftile=None (the default) produces no --reftile on the command line -- existing callers unaffected."""
        stem = "1391522232_ch62_solutions"
        captured_cmd = {}

        def _run(cmd, *args, **kwargs):
            captured_cmd["cmd"] = cmd
            return True, ""

        with patch("mwax_mover.calvin.plots.hyperdrive.run_command", side_effect=_run):
            generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert "--reftile" not in captured_cmd["cmd"]

    def test_reftile_appended_when_given(self, tmp_path):
        """reftile='Tile104' appends --reftile Tile104 to the command line."""
        stem = "1391522232_ch62_solutions"
        captured_cmd = {}

        def _run(cmd, *args, **kwargs):
            captured_cmd["cmd"] = cmd
            return True, ""

        with patch("mwax_mover.calvin.plots.hyperdrive.run_command", side_effect=_run):
            generate_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
                reftile="Tile104",
            )

        assert "--reftile Tile104" in captured_cmd["cmd"]


class TestGeneratePlotsForFiles:
    """Tests for the concurrent per-file hyperdrive plot wrapper."""

    def test_every_file_is_attempted(self):
        """All solution files get a hyperdrive invocation."""
        files = [f"/data/obs_ch{c}_solutions.fits" for c in (62, 67, 73)]

        patch_target = "mwax_mover.calvin.plots.hyperdrive.generate_plots"
        with patch(patch_target, return_value=(True, "")) as mock_gen:
            failures = generate_plots_for_files(
                123, files, "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
            )

        assert failures == []
        assert mock_gen.call_count == 3
        assert {call.args[1] for call in mock_gen.call_args_list} == set(files)

    def test_one_failure_does_not_stop_the_others(self):
        """A failing file is reported but the rest still run."""
        files = [f"/data/obs_ch{c}_solutions.fits" for c in (62, 67, 73)]

        def _gen(obs_id, filename, *args, **kwargs):
            if "ch67" in filename:
                return False, "hyperdrive exploded"
            return True, ""

        with patch("mwax_mover.calvin.plots.hyperdrive.generate_plots", side_effect=_gen) as mock_gen:
            failures = generate_plots_for_files(
                123, files, "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
            )

        assert mock_gen.call_count == 3
        assert len(failures) == 1
        assert "ch67" in failures[0][0]
        assert failures[0][1] == "hyperdrive exploded"

    def test_raised_exception_is_captured_not_propagated(self):
        """Plots are diagnostic: a crash must not fail the calibration."""
        files = ["/data/obs_ch62_solutions.fits", "/data/obs_ch67_solutions.fits"]

        def _gen(obs_id, filename, *args, **kwargs):
            if "ch62" in filename:
                raise RuntimeError("boom")
            return True, ""

        with patch("mwax_mover.calvin.plots.hyperdrive.generate_plots", side_effect=_gen):
            failures = generate_plots_for_files(
                123, files, "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
            )

        assert len(failures) == 1
        assert failures[0][1] == "boom"

    def test_empty_file_list_is_a_no_op(self):
        """No files means no pool and no work."""
        with patch("mwax_mover.calvin.plots.hyperdrive.generate_plots") as mock_gen:
            assert (
                generate_plots_for_files(123, [], "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True) == []
            )
        mock_gen.assert_not_called()

    def test_before_flag_is_passed_through(self):
        """The before/after distinction must survive the pool dispatch."""
        patch_target = "mwax_mover.calvin.plots.hyperdrive.generate_plots"
        with patch(patch_target, return_value=(True, "")) as mock_gen:
            generate_plots_for_files(
                123, ["/data/a_solutions.fits"], "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=False
            )

        # (obs_id, filename, binary, metafits, output_dir, before, max_amp)
        assert mock_gen.call_args_list[0].args[5] is False

    def test_reftile_is_passed_through(self):
        """reftile is forwarded to every file's generate_plots call, unchanged."""
        files = [f"/data/obs_ch{c}_solutions.fits" for c in (62, 67)]
        patch_target = "mwax_mover.calvin.plots.hyperdrive.generate_plots"
        with patch(patch_target, return_value=(True, "")) as mock_gen:
            generate_plots_for_files(
                123,
                files,
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                "/out",
                before=True,
                reftile="Tile104",
            )

        assert mock_gen.call_count == 2
        # (obs_id, filename, binary, metafits, output_dir, before, max_amp, reftile)
        assert all(call.args[7] == "Tile104" for call in mock_gen.call_args_list)
