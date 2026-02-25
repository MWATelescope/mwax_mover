from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover import utils
from mwax_mover import mwax_bf_vdif_utils
from mwax_mover import mwax_bf_filterbank_utils
from logging import Logger
import os
import glob
import shutil

METAFITS_EXPOSURE = "EXPOSURE"


class BfStitchingProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
        logger: Logger,
        metafits_path: str,
        bf_incoming_path: str,
        bf_stitching_path: str,
        bf_dont_archive_path: str,
        list_of_corr_hi_priority_projects: list[str],
        list_of_vcs_hi_priority_projects: list[str],
        archiving_enabled: bool,
        keep_original_files_after_stitching: bool,
    ):
        super().__init__(
            "BfStitchingProcessor",
            logger,
            metafits_path,
            [
                (bf_incoming_path, ".vdif"),
                (bf_incoming_path, ".fil"),
            ],
            MODE_WATCH_DIR_FOR_RENAME,
            list_of_corr_hi_priority_projects,
            list_of_vcs_hi_priority_projects,
        )
        self.bf_incoming_path = bf_incoming_path
        self.bf_stitching_path = bf_stitching_path
        self.bf_dont_archive_path = bf_dont_archive_path
        self.list_of_correlator_high_priority_projects = list_of_corr_hi_priority_projects
        self.list_of_vcs_high_priority_projects = list_of_vcs_hi_priority_projects
        self.archiving_enabled = archiving_enabled
        self.keep_original_files_after_stitching = keep_original_files_after_stitching

    def handler(self, item: str) -> bool:
        #
        # Each time we get a new filterbank or vdif file
        # We *may* need to kick off stitching things together.
        # Once stitched, we send to the checksum+db queue
        #
        filename = os.path.basename(item)
        ext = os.path.splitext(filename)[1]

        # get the obsid and subobs from the filename
        # obsid_subobsid_chXXX_beamXX.vdif / .fil
        try:
            try:
                obs_id = int(filename[0:10])
            except Exception:
                raise ValueError(f"{item}: Error getting obs_id from filename {filename}")
            try:
                subobs_id = int(filename[11:21])
            except Exception:
                raise ValueError(f"{item}: Error getting subobs_id from filename {filename}")
        except Exception:
            self.logger.warning(
                f"{item}: filename not in correct format. Should be obsid_subobsid_chXXX_beamXX.vdif or .fil. It's probably a failed file from a previous run and can probably be safely deleted (by you). Skipping file."
            )
            return True

        # Determine metafits filename
        metafits_filename = os.path.join(self.metafits_path, f"{obs_id}_metafits.fits")

        try:
            duration_sec = int(utils.get_metafits_value(metafits_filename, METAFITS_EXPOSURE))
        except Exception:
            raise ValueError(f"{item}: Error reading {METAFITS_EXPOSURE} from metafits filename {metafits_filename}")

        self.logger.debug(f"{item}: Read {METAFITS_EXPOSURE} of {duration_sec}s from {metafits_filename}")

        # Now determine the last subobs we should see
        # we subtract 8 seconds because the subobsid is the START of the subobs. Examples:
        # 1. Obsid =1234500000 Duration: 8 => last subobsid = 1234500000
        # 2. Obsid =1234500000 Duration: 16 => last subobsid = 1234500008
        expected_last_subobs_id = (obs_id + duration_sec) - 8

        # Check to see if this subobsid == last subobsid
        self.logger.debug(
            f"{item}: Checking this subobs_id {subobs_id} against expected last subobs_id {expected_last_subobs_id}"
        )
        if subobs_id >= expected_last_subobs_id:
            # Time to stitch up the files
            self.logger.debug(f"{item}: Observation complete. Stitching up beamformer {ext} files...")
            if ext == ".vdif":
                # determine the file_path, rec_chan and beam number
                file_path, _, _, rec_chan, beam = mwax_bf_vdif_utils.get_vdif_filename_components(item)

                # find all the vdif files for this beam, channel and obs
                files = glob.glob(os.path.join(file_path, f"{obs_id}_*_ch{rec_chan:03d}_beam{beam:02d}.vdif"))

                self.logger.debug(
                    f"{item}: Found {len(files)} vdif files to stitch for rec_chan {rec_chan:03d}, beam {beam:02d}: {files}"
                )

                # Now keep the original files if the config file tells us we should
                if self.keep_original_files_after_stitching:
                    self.logger.info(
                        f"{item}: Copying unstitched vdif files to bf_dont_archive_path since keep_original_files_after_stitching=True..."
                    )
                    for f in files:
                        shutil.copy(f, os.path.join(self.bf_dont_archive_path, os.path.basename(f)))

                mwax_bf_vdif_utils.stitch_vdif_files_and_write_hdr(
                    self.logger, metafits_filename, files, self.bf_stitching_path
                )

                # If it worked, remove the files
                for f in files:
                    utils.remove_file(self.logger, f, False)

                return True

            elif ext == ".fil":
                # determine file_path, rec_chan and beam number
                file_path, _, _, rec_chan, beam = mwax_bf_filterbank_utils.get_filterbank_filename_components(item)

                # find all the fil files for this beam, channel and obs
                files = glob.glob(os.path.join(file_path, f"{obs_id}_*_ch{rec_chan:03d}_beam{beam:02d}.fil"))

                self.logger.debug(
                    f"{item}: Found {len(files)} filterbank files to stitch for rec_chan {rec_chan:03d}, beam {beam:02d}"
                )

                # Now keep the original files if the config file tells us we should
                if self.keep_original_files_after_stitching:
                    self.logger.info(
                        f"{item}: Copying unstitched filterbank files to bf_dont_archive_path since keep_original_files_after_stitching=True..."
                    )
                    for f in files:
                        shutil.copy(f, os.path.join(self.bf_dont_archive_path, os.path.basename(f)))

                # Now stitch the files together and write to the stitching path
                mwax_bf_filterbank_utils.stitch_filterbank_files(self.logger, files, self.bf_stitching_path)

                # If it worked, remove the files
                for f in files:
                    utils.remove_file(self.logger, f, False)

                return True
            else:
                raise Exception(f"{item} Extension {ext} is not supported")
        else:
            # Nothing to do
            self.logger.debug(
                f"{item}: waiting for end of observation. This subobs_id {subobs_id} is < {expected_last_subobs_id}"
            )
            return True
