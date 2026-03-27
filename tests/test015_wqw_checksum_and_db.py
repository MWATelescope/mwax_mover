"""Unit tests for ChecksumAndDBProcessor.handler() in mwax_wqw_checksum_and_db.py.

Tests cover all routing branches:
  - Invalid filename → False
  - Archiving disabled → dont_archive / processing_stats paths (all 5 file types)
  - Archiving enabled, archive project → outgoing / processing_stats paths (all 5 file types)
  - Archiving enabled, no-archive project (C123) → dont_archive / processing_stats paths (all 5 file types)
  - Error conditions: FileNotFoundError, DB insert failure, file removed after DB insert, unknown filetype
"""

import logging
import os
from unittest.mock import MagicMock, patch

import pytest

from mwax_mover.mwax_db import MWAXDBHandler
from mwax_mover.mwax_wqw_checksum_and_db import ChecksumAndDBProcessor
from mwax_mover.utils import ValidationData, MWADataFileType
from tests_common import setup_test_directories

# Setup root logger so processor log output is visible when running with -s
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DUMMY_CHECKSUM = "abc123md5def456"
DUMMY_FILE_SIZE = 123_456_789

# Filename used as the item being processed (lives in vis incoming dir)
DUMMY_FILENAME = "1244973688_20190619100110_ch114_000.fits"


# ---------------------------------------------------------------------------
# Module-scoped fixture: create test015 directories once for the whole module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dirs() -> dict:
    """Create the test015 directory tree and return a dict of relevant paths."""
    base = setup_test_directories("test015")
    return {
        "base": base,
        "metafits":          os.path.join(base, "vulcan/metafits"),
        "vis_incoming":      os.path.join(base, "visdata/incoming"),
        "vis_proc_stats":    os.path.join(base, "visdata/processing_stats"),
        "vis_outgoing":      os.path.join(base, "visdata/outgoing"),
        "vis_dont_archive":  os.path.join(base, "visdata/dont_archive"),
        "volt_incoming":     os.path.join(base, "voltdata/incoming"),
        "volt_outgoing":     os.path.join(base, "voltdata/outgoing"),
        "volt_dont_archive": os.path.join(base, "voltdata/dont_archive"),
        "bf_stitching":      os.path.join(base, "voltdata/bf/stitching"),
        "bf_outgoing":       os.path.join(base, "voltdata/bf/outgoing"),
        "bf_dont_archive":   os.path.join(base, "voltdata/bf/dont_archive"),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_processor(dirs: dict, archiving_enabled: bool) -> ChecksumAndDBProcessor:
    """Create a ChecksumAndDBProcessor wired to the test015 directory tree."""
    db = MWAXDBHandler("dummy", 5432, "dummy", "dummy", "dummy")
    return ChecksumAndDBProcessor(
        metafits_path=dirs["metafits"],
        visdata_incoming_path=dirs["vis_incoming"],
        visdata_processing_stats_path=dirs["vis_proc_stats"],
        visdata_outgoing_path=dirs["vis_outgoing"],
        visdata_dont_archive_path=dirs["vis_dont_archive"],
        voltdata_incoming_path=dirs["volt_incoming"],
        voltdata_outgoing_path=dirs["volt_outgoing"],
        voltdata_dont_archive_path=dirs["volt_dont_archive"],
        bf_stitching_path=dirs["bf_stitching"],
        bf_outgoing_path=dirs["bf_outgoing"],
        bf_dont_archive_path=dirs["bf_dont_archive"],
        list_of_corr_hi_priority_projects=[],
        list_of_vcs_hi_priority_projects=[],
        db_handler_object=db,
        archiving_enabled=archiving_enabled,
    )


def make_valid_val(filetype_id: int, project_id: str = "G0001") -> ValidationData:
    """Return a ValidationData indicating a valid file."""
    return ValidationData(
        valid=True,
        obs_id=1244973688,
        project_id=project_id,
        filetype_id=filetype_id,
        file_ext=".fits",
        calibrator=False,
        validation_message="",
    )


def make_invalid_val() -> ValidationData:
    """Return a ValidationData indicating an invalid file."""
    return ValidationData(
        valid=False,
        obs_id=0,
        project_id="",
        filetype_id=0,
        file_ext="",
        calibrator=False,
        validation_message="Invalid filename",
    )


def dummy_item(dirs: dict) -> str:
    """Return the full path of the dummy item, sitting in vis incoming."""
    return os.path.join(dirs["vis_incoming"], DUMMY_FILENAME)


def mock_stat(file_size: int = DUMMY_FILE_SIZE) -> MagicMock:
    """Return a mock os.stat result with the given st_size."""
    s = MagicMock()
    s.st_size = file_size
    return s


# ---------------------------------------------------------------------------
# Group 1: Invalid filename
# ---------------------------------------------------------------------------

class TestInvalidFilename:
    def test_invalid_filename_returns_false(self, dirs):
        """handler() returns False when filename validation fails."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_invalid_val()):
            result = processor.handler(item)

        assert result is False


# ---------------------------------------------------------------------------
# Group 2: Archiving DISABLED — routing only (no checksum or DB work)
# ---------------------------------------------------------------------------

class TestArchivingDisabled:
    """When archiving_enabled=False the handler skips checksum+DB and routes
    directly to dont_archive or processing_stats based on filetype."""

    def _run(self, dirs: dict, filetype_id: int, project_id: str = "G0001"):
        processor = make_processor(dirs, archiving_enabled=False)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(filetype_id, project_id)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename") as mock_rename:
            result = processor.handler(item)

        return result, mock_rename, item

    def test_voltages_go_to_volt_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWAX_VOLTAGES.value)
        assert result is True
        expected = os.path.join(dirs["volt_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_visibilities_go_to_processing_stats(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWAX_VISIBILITIES.value)
        assert result is True
        expected = os.path.join(dirs["vis_proc_stats"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_ppd_goes_to_vis_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWA_PPD_FILE.value)
        assert result is True
        expected = os.path.join(dirs["vis_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_vdif_goes_to_bf_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.VDIF.value)
        assert result is True
        expected = os.path.join(dirs["bf_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_filterbank_goes_to_bf_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.FILTERBANK.value)
        assert result is True
        expected = os.path.join(dirs["bf_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_unknown_filetype_returns_false(self, dirs):
        """An unrecognised filetype with archiving disabled should return False."""
        processor = make_processor(dirs, archiving_enabled=False)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(filetype_id=99)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename"):
            result = processor.handler(item)

        assert result is False


# ---------------------------------------------------------------------------
# Group 3: Archiving ENABLED, archive project (not C123)
# ---------------------------------------------------------------------------

class TestArchivingEnabledArchiveProject:
    """Archiving on, project is not C123 → files routed to outgoing or processing_stats."""

    def _run(self, dirs: dict, filetype_id: int):
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(filetype_id, project_id="G0001")), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename") as mock_rename:
            result = processor.handler(item)

        return result, mock_rename, item

    def test_voltages_go_to_volt_outgoing(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWAX_VOLTAGES.value)
        assert result is True
        expected = os.path.join(dirs["volt_outgoing"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_visibilities_go_to_processing_stats(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWAX_VISIBILITIES.value)
        assert result is True
        expected = os.path.join(dirs["vis_proc_stats"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_ppd_goes_to_vis_outgoing(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWA_PPD_FILE.value)
        assert result is True
        expected = os.path.join(dirs["vis_outgoing"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_vdif_goes_to_bf_outgoing(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.VDIF.value)
        assert result is True
        expected = os.path.join(dirs["bf_outgoing"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_filterbank_goes_to_bf_outgoing(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.FILTERBANK.value)
        assert result is True
        expected = os.path.join(dirs["bf_outgoing"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_unknown_filetype_returns_false(self, dirs):
        """An unrecognised filetype when archiving is enabled should return False."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(filetype_id=99, project_id="G0001")), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True):
            result = processor.handler(item)

        assert result is False

    def test_trigger_id_read_for_voltages(self, dirs):
        """For voltage files, read_subfile_trigger_value must be called and the
        returned trigger_id forwarded to insert_data_file_row."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VOLTAGES.value, project_id="G0001")), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=42) as mock_trigger, \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True) as mock_insert, \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename"):
            result = processor.handler(item)

        assert result is True
        mock_trigger.assert_called_once_with(item)
        # trigger_id=42 must appear in the positional args of insert_data_file_row
        assert 42 in mock_insert.call_args[0]

    def test_trigger_id_not_read_for_visibilities(self, dirs):
        """For non-voltage files, read_subfile_trigger_value must NOT be called."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VISIBILITIES.value, project_id="G0001")), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None) as mock_trigger, \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename"):
            result = processor.handler(item)

        assert result is True
        mock_trigger.assert_not_called()


# ---------------------------------------------------------------------------
# Group 4: Archiving ENABLED, no-archive project (C123)
# ---------------------------------------------------------------------------

class TestArchivingEnabledNoArchiveProject:
    """Archiving on but project_id is C123, so should_project_be_archived returns False.
    Files are routed to dont_archive paths (except VISIBILITIES → processing_stats)."""

    def _run(self, dirs: dict, filetype_id: int):
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(filetype_id, project_id="C123")), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename") as mock_rename:
            result = processor.handler(item)

        return result, mock_rename, item

    def test_voltages_go_to_volt_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWAX_VOLTAGES.value)
        assert result is True
        expected = os.path.join(dirs["volt_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_visibilities_go_to_processing_stats(self, dirs):
        """Even for C123, visibilities still go to processing_stats for stats generation."""
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWAX_VISIBILITIES.value)
        assert result is True
        expected = os.path.join(dirs["vis_proc_stats"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_ppd_goes_to_vis_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.MWA_PPD_FILE.value)
        assert result is True
        expected = os.path.join(dirs["vis_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_vdif_goes_to_bf_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.VDIF.value)
        assert result is True
        expected = os.path.join(dirs["bf_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)

    def test_filterbank_goes_to_bf_dont_archive(self, dirs):
        result, mock_rename, item = self._run(dirs, MWADataFileType.FILTERBANK.value)
        assert result is True
        expected = os.path.join(dirs["bf_dont_archive"], DUMMY_FILENAME)
        mock_rename.assert_called_once_with(item, expected)


# ---------------------------------------------------------------------------
# Group 5: Error conditions
# ---------------------------------------------------------------------------

class TestErrorConditions:
    def test_file_not_found_during_stat_returns_true(self, dirs):
        """If the file disappears before os.stat (checksum phase), return True (already gone)."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VOLTAGES.value)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat",
                   side_effect=FileNotFoundError("gone")):
            result = processor.handler(item)

        assert result is True

    def test_db_insert_failure_returns_false(self, dirs):
        """If insert_data_file_row returns False, handler returns False to trigger requeue."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VOLTAGES.value)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=False):
            result = processor.handler(item)

        assert result is False

    def test_file_removed_after_db_insert_returns_true(self, dirs):
        """If the file no longer exists after a successful DB insert (e.g. a prior FK conflict
        triggered deletion), return True — treat the item as done."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VOLTAGES.value)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=False):
            result = processor.handler(item)

        assert result is True

    def test_checksum_forwarded_to_db_insert(self, dirs):
        """The MD5 string returned by do_checksum_md5 must appear in insert_data_file_row args."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VISIBILITIES.value)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat()), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True) as mock_insert, \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename"):
            result = processor.handler(item)

        assert result is True
        assert DUMMY_CHECKSUM in mock_insert.call_args[0]

    def test_file_size_forwarded_to_db_insert(self, dirs):
        """The file size from os.stat must appear in insert_data_file_row args."""
        processor = make_processor(dirs, archiving_enabled=True)
        item = dummy_item(dirs)

        with patch("mwax_mover.mwax_wqw_checksum_and_db.utils.validate_filename",
                   return_value=make_valid_val(MWADataFileType.MWAX_VISIBILITIES.value)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.stat", return_value=mock_stat(DUMMY_FILE_SIZE)), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.do_checksum_md5",
                   return_value=DUMMY_CHECKSUM), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.utils.read_subfile_trigger_value",
                   return_value=None), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.insert_data_file_row",
                   return_value=True) as mock_insert, \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.path.exists",
                   return_value=True), \
             patch("mwax_mover.mwax_wqw_checksum_and_db.os.rename"):
            result = processor.handler(item)

        assert result is True
        assert DUMMY_FILE_SIZE in mock_insert.call_args[0]
