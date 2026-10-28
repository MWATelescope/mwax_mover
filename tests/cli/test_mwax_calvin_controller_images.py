"""Tests for how the controller handles a fit's imaging directory (<fit_id>_images)."""

from pathlib import Path
from unittest.mock import patch

import pytest

from mwax_mover.cli.mwax_calvin_controller import (
    MWAXCalvinController,
    fit_dir_sort_key,
    parse_fit_dir_name,
)

FIT = 1768401673707300


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (str(FIT), (FIT, False)),
        (f"{FIT}_images", (FIT, True)),
        ("_images", None),
        ("notafit", None),
        (f"{FIT}_other", None),
        (f".staging-{FIT}_images", None),
    ],
)
def test_parse_fit_dir_name(name, expected):
    assert parse_fit_dir_name(name) == expected


def test_images_dir_sorts_directly_after_its_fit_dir_and_newest_first():
    names = [f"{FIT}_images", "zzz", str(FIT - 1), f"{FIT + 1}_images", str(FIT), str(FIT + 1), f"{FIT - 1}_images"]

    names.sort(key=lambda n: fit_dir_sort_key(Path(n)))

    assert names == [
        str(FIT + 1),
        f"{FIT + 1}_images",
        str(FIT),
        f"{FIT}_images",
        str(FIT - 1),
        f"{FIT - 1}_images",
        "zzz",
    ]


def test_get_s3_subpath_maps_both_directories_to_the_fit_id():
    assert MWAXCalvinController.get_s3_subpath(str(FIT)) == str(FIT)
    assert MWAXCalvinController.get_s3_subpath(f"{FIT}_images") == str(FIT)
    assert MWAXCalvinController.get_s3_subpath("notafit") == "notafit"


class TestUploadImagesDirs:
    @staticmethod
    def make_controller() -> MWAXCalvinController:
        mcal = MWAXCalvinController()
        mcal.cfg_plots_s3_profile = "test-profile"
        mcal.cfg_plots_s3_bucket = "test-bucket"
        mcal.cfg_plots_upload_max_fits_per_pass = 100
        return mcal

    @staticmethod
    def make_dirs(base: Path, names: list[str]) -> None:
        for name in names:
            (base / name).mkdir(parents=True)
            (base / name / "file.png").write_text("x")

    @staticmethod
    def fake_rclone_move(path, profile, bucket, dest_subpath=None, min_file_age_secs=0):
        for src_file in Path(path).iterdir():
            src_file.unlink()
        return (1, 1024)

    def test_images_dir_is_skipped_while_its_fit_dir_exists(self, tmp_path):
        base = tmp_path / "plots"
        self.make_dirs(base, [str(FIT), f"{FIT}_images"])

        with patch("mwax_mover.cli.mwax_calvin_controller.rclone_move", side_effect=self.fake_rclone_move) as mock_move:
            self.make_controller().upload_published_fit_dirs(str(base))

        # Only the fit dir this pass, going to its own path
        assert [c.kwargs["dest_subpath"] for c in mock_move.call_args_list] == [str(FIT)]
        assert (base / f"{FIT}_images").exists()
        assert not (base / str(FIT)).exists()

    def test_images_dir_is_uploaded_to_the_fit_path_on_the_next_pass(self, tmp_path):
        base = tmp_path / "plots"
        self.make_dirs(base, [str(FIT), f"{FIT}_images"])
        mcal = self.make_controller()

        with patch("mwax_mover.cli.mwax_calvin_controller.rclone_move", side_effect=self.fake_rclone_move) as mock_move:
            mcal.upload_published_fit_dirs(str(base))
            mcal.upload_published_fit_dirs(str(base))

        assert [(Path(c.args[0]).name, c.kwargs["dest_subpath"]) for c in mock_move.call_args_list] == [
            (str(FIT), str(FIT)),
            (f"{FIT}_images", str(FIT)),
        ]
        assert list(base.iterdir()) == []

    def test_images_dir_alone_is_uploaded_immediately(self, tmp_path):
        base = tmp_path / "plots"
        self.make_dirs(base, [f"{FIT}_images"])

        with patch("mwax_mover.cli.mwax_calvin_controller.rclone_move", side_effect=self.fake_rclone_move) as mock_move:
            self.make_controller().upload_published_fit_dirs(str(base))

        assert [c.kwargs["dest_subpath"] for c in mock_move.call_args_list] == [str(FIT)]

    def test_a_skipped_images_dir_does_not_count_as_a_backlog(self, tmp_path):
        base = tmp_path / "plots"
        self.make_dirs(base, [str(FIT), f"{FIT}_images"])
        mcal = self.make_controller()
        mcal.cfg_plots_upload_max_fits_per_pass = 1

        with patch("mwax_mover.cli.mwax_calvin_controller.rclone_move", side_effect=self.fake_rclone_move):
            more_pending = mcal.upload_published_fit_dirs(str(base))

        assert more_pending is False

    def test_staging_dir_of_an_images_dir_is_never_uploaded(self, tmp_path):
        base = tmp_path / "plots"
        self.make_dirs(base, [f".staging-{FIT}_images"])

        with patch("mwax_mover.cli.mwax_calvin_controller.rclone_move") as mock_move:
            self.make_controller().upload_published_fit_dirs(str(base))

        mock_move.assert_not_called()
