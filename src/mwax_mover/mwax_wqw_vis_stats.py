from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME
from mwax_mover.utils import ValidationData
from mwax_mover import utils
from logging import Logger
import os


class VisStatsProcessor(MWAXWatchQueueWorker):
    def __init__(
        self,
        logger: Logger,
        metafits_path: str,
        visdata_processing_stats_path: str,
        mwax_stats_binary_dir: str,
        mwax_stats_timeout_sec: int,
        mwax_stats_dump_dir: str,
        archiving_enabled: bool,
        visdata_outgoing_path: str,
        visdata_outgoing_cal_path: str,
        visdata_dont_archive_path: str,
    ):
        super().__init__(
            "VisStatsProcessing",
            logger,
            [(visdata_processing_stats_path, ".fits")],
            MODE_WATCH_DIR_FOR_RENAME,
        )

        self.visdata_processing_stats_path = visdata_processing_stats_path
        self.mwax_stats_binary_dir = mwax_stats_binary_dir
        self.mwax_stats_timeout_sec = mwax_stats_timeout_sec
        self.mwax_stats_dump_dir = mwax_stats_dump_dir
        self.metafits_path = metafits_path
        self.archiving_enabled = archiving_enabled
        self.visdata_outgoing_path = visdata_outgoing_path
        self.visdata_outgoing_cal_path = visdata_outgoing_cal_path
        self.visdata_dont_archive_path = visdata_dont_archive_path

    def handler(self, item: str) -> bool:
        """This runs stats against mwax FITS files"""
        self.logger.info(f"{item}- stats_handler() Started...")

        # This is a normal mwax fits file.
        # Run stats on it, but only if it is the 000 file.
        # Don't bother doing the 001, 002, etc if they exist
        if os.path.basename(item)[-9:] == "_000.fits":
            if (
                utils.process_mwax_stats(
                    self.logger,
                    self.mwax_stats_binary_dir,
                    item,
                    None,
                    self.mwax_stats_timeout_sec,
                    self.mwax_stats_dump_dir,
                    self.metafits_path,
                )
                is not True
            ):
                self.logger.warning(f"{item}- stats_handler() mwax_stats failed. Skipping.")
        else:
            self.logger.debug(f"{item}- stats_handler() skipping mwax_stats as file does not end in _000.fits")

        # If observation is a calibrator AND this host is enabled as an archiver then
        # we should put the obs into the cal_outgoing dir so that calvin can
        # pull it in and calibrate it. The calvin server which processed the file(s)
        # will then call our webservice endpoint "release_cal_obs" and then that will
        # move the file into visdata_outgoing so it can be archived.

        # Is this host doing archiving?
        if self.archiving_enabled == 1:
            # Validate and get info about the obs
            obs_info: ValidationData = utils.validate_filename(self.logger, item, self.metafits_path)

            # Should this project be archived?
            if utils.should_project_be_archived(obs_info.project_id):
                if obs_info.calibrator:
                    # Send to cal_outgoing
                    # Take the input filename - strip the path, then append the output path
                    outgoing_filename = os.path.join(self.visdata_outgoing_cal_path, os.path.basename(item))
                    self.logger.debug(f"{item}- stats_handler() moving file to outgoing cal dir")
                    os.rename(item, outgoing_filename)
                else:
                    # Not a calibrator just archive it
                    # Send to vis_outgoing
                    # Take the input filename - strip the path, then append the output path
                    outgoing_filename = os.path.join(self.visdata_outgoing_path, os.path.basename(item))
                    self.logger.debug(f"{item}- stats_handler() moving file to outgoing vis dir")
                    os.rename(item, outgoing_filename)
            else:
                # No this project doesn't get archived or calibrated, just send it to dont_archive
                outgoing_filename = os.path.join(self.visdata_dont_archive_path, os.path.basename(item))
                self.logger.debug(f"{item}- stats_handler() moving file to {self.visdata_dont_archive_path}")
                os.rename(item, outgoing_filename)
        else:
            # This host is not doing any archiving
            outgoing_filename = os.path.join(self.visdata_dont_archive_path, os.path.basename(item))
            self.logger.debug(f"{item}- stats_handler() moving file to {self.visdata_dont_archive_path}")
            os.rename(item, outgoing_filename)

        self.logger.info(f"{item}- stats_handler() Finished")
        return True
