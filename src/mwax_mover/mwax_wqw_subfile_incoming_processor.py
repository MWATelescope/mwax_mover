"""Watch-queue-worker that processes incoming PSRDADA subfiles and routes them based on operating mode.

Reads the PSRDADA header from each .sub subfile to determine the observation mode
(CORRELATOR, VCS, BEAMFORMER, NO_CAPTURE, VOLTAGE_BUFFER) and routes accordingly:
loads into the PSRDADA ring buffer for correlation, copies to voltdata for VCS or
voltage dumps, or signals the beamformer via Redis. Also handles FREDDA-triggered
voltage dump windows, packet statistics extraction, and the always_keep_subfiles
mode. Renames processed subfiles to .free so the ringbuffer slot can be reused.
"""

from mwax_mover.mwax_calvin_utils import get_partial_aocal_filename, get_solution_fits_filename
from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME
from mwax_mover import utils
import os
import glob
import shutil
import sys
import time
import logging

METAFITS_EXPOSURE = "EXPOSURE"
COMMAND_DADA_DISKDB = "dada_diskdb"


logger = logging.getLogger(__name__)


class SubfileIncomingProcessor(MWAXWatchQueueWorker):
    def __init__(
        self,
        sd_ctx,
        subfile_incoming_path: str,
        subfile_ext: str,
        freefile_ext: str,
        keepfile_ext: str,
        voltdata_incoming_path: str,
        bf_cal_path: str,
        bf_redis_host: str,
        bf_redis_queue_key: str,
        packet_stats_dump_dir: str,
        mwax_stats_binary_dir: str,
        corr_devshm_numa_node: int,
        copy_subfile_to_disk_timeout_sec: int,
        corr_ringbuffer_key: str,
        corr_diskdb_numa_node: int,
        psrdada_timeout_sec: int,
        always_keep_subfiles: bool,
        archive_destination_enabled: bool,
        metafits_path: str,
        subfile_dist_mode: utils.MWAXSubfileDistirbutorMode,
    ):
        """Initialise the PSRDADA subfile incoming processor.

        Args:
            sd_ctx: Shared state context for dump management and archiving control.
            subfile_incoming_path: Directory watched for incoming PSRDADA subfiles (.sub).
            subfile_ext: Extension for subfile files (typically .sub).
            freefile_ext: Extension for freed subfiles (typically .free).
            keepfile_ext: Extension for kept subfiles during voltage dumps (typically .keep).
            voltdata_incoming_path: Directory where voltage data files are written.
            bf_cal_path: Directory containing beamformer calibration files.
            bf_redis_host: Redis host for beamformer signalling.
            bf_redis_queue_key: Redis queue key for beamformer messages.
            packet_stats_dump_dir: Directory where packet statistics are written.
            mwax_stats_binary_dir: Directory containing mwax utilities binaries.
            corr_devshm_numa_node: NUMA node for correlator shared memory.
            copy_subfile_to_disk_timeout_sec: Timeout in seconds for disk copy operations.
            corr_ringbuffer_key: PSRDADA key for correlator ring buffer.
            corr_diskdb_numa_node: NUMA node for disk database operations.
            psrdada_timeout_sec: Timeout in seconds for PSRDADA operations.
            always_keep_subfiles: Whether to always keep subfiles to voltdata.
            archive_destination_enabled: Whether archive destination is enabled.
            metafits_path: Directory containing metafits files.
            subfile_dist_mode: Operating mode (CORRELATOR or BEAMFORMER).
        """
        super().__init__(
            "SubfileIncomingProcessor",
            [
                (subfile_incoming_path, ".sub"),
            ],
            mode=MODE_WATCH_DIR_FOR_RENAME,
            requeue_to_eoq_on_failure=False,
        )
        self.sd_ctx = sd_ctx

        self.subfile_incoming_path = subfile_incoming_path
        self.voltdata_incoming_path = voltdata_incoming_path
        self.ext_sub_file = subfile_ext
        self.ext_free_file = freefile_ext
        self.ext_keep_file = keepfile_ext

        self.packet_stats_dump_dir = packet_stats_dump_dir
        self.mwax_stats_binary_dir = mwax_stats_binary_dir

        self.bf_cal_path = bf_cal_path
        self.bf_redis_host = bf_redis_host
        self.bf_redis_queue_key = bf_redis_queue_key

        self.corr_devshm_numa_node = corr_devshm_numa_node
        self.copy_subfile_to_disk_timeout_sec = copy_subfile_to_disk_timeout_sec
        self.corr_ringbuffer_key = corr_ringbuffer_key
        self.corr_diskdb_numa_node = corr_diskdb_numa_node
        self.psrdada_timeout_sec = psrdada_timeout_sec

        self.always_keep_subfiles = always_keep_subfiles
        self.archive_destination_enabled = archive_destination_enabled
        self.metafits_path = metafits_path
        self.subfile_dist_mode = subfile_dist_mode

    def handler(self, item: str) -> bool:
        """Process incoming PSRDADA subfiles and route by observation mode.

        Reads the PSRDADA header to determine observation mode and routes accordingly:
        loads into ring buffer for correlation, copies to voltdata for VCS or voltage
        dumps, or signals beamformer via Redis. Handles FREDDA-triggered voltage dump
        windows, extracts packet statistics, and renames processed files to .free.

        Args:
            item: Full path of the PSRDADA subfile (.sub) to process.

        Returns:
            True if subfile was successfully processed.
            False if an error occurred during processing.
        """
        success = False

        logger.info(f"{item}: SubfileIncomingProcessor.subfile_handler is handling {item}...")

        handler_starttime = time.time()

        # `keep_subfiles_path` is used after doing the main work. If we are
        # running in with `always_keep_subfiles`=1,
        # then we will always keep the voltages and not delete them.
        # If so, we put them into the voltdata path. But if we are in VCS mode
        # there is no need to do this as we
        # already keep the sub files.
        #
        # If not in `always_keep_subfiles`=1 mode, this value will be None and
        # will be ignored.
        #
        keep_subfiles_path = None

        # Read all the things from the subfile header here!
        subfile_header_values = utils.read_subfile_values(
            item,
            [
                utils.PSRDADA_OBS_ID,
                utils.PSRDADA_SUBOBS_ID,
                utils.PSRDADA_TRANSFER_SIZE,
                utils.PSRDADA_MODE,
                utils.PSRDADA_COARSE_CHANNEL,
                utils.PSRDADA_NINPUTS,
            ],
        )

        # Validate each one
        if subfile_header_values[utils.PSRDADA_OBS_ID] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_OBS_ID} not found in {item}")
        obs_id = int(subfile_header_values[utils.PSRDADA_OBS_ID])

        if subfile_header_values[utils.PSRDADA_SUBOBS_ID] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_SUBOBS_ID} not found in {item}")
        subobs_id = int(subfile_header_values[utils.PSRDADA_SUBOBS_ID])

        # Read TRANSFER_SIZE from subfile header
        # We only use this when writing a subfile to disk in case the subfile is
        # bigger than the data
        # transfer_size_str = utils.read_subfile_value(item, utils.PSRDADA_TRANSFER_SIZE)
        if subfile_header_values[utils.PSRDADA_TRANSFER_SIZE] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_TRANSFER_SIZE} not found in {item}")
        transfer_size = int(subfile_header_values[utils.PSRDADA_TRANSFER_SIZE])
        subfile_bytes_to_write = transfer_size + utils.PSRDADA_HEADER_BYTES  # We add the header to the transfer size

        # Get Mode
        if subfile_header_values[utils.PSRDADA_MODE] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_MODE} not found in {item}")
        subfile_mode = subfile_header_values[utils.PSRDADA_MODE]

        # Only do packet stats if packet_stats_dump_dir is not an empty string
        if self.packet_stats_dump_dir != "":
            # For all subfiles we need to extract the packet stats:
            # Ignore failures
            utils.run_mwax_packet_stats(self.mwax_stats_binary_dir, item, self.packet_stats_dump_dir, -1, 3)

        try:
            #
            # We need to check we are not in a voltage dump. If so, whatever
            # observation is happening is ignored and instead we treat this
            # like a VCS observation
            #
            if (
                (self.sd_ctx.dump_start_gps is not None and self.sd_ctx.dump_end_gps is not None)
                and subobs_id >= self.sd_ctx.dump_start_gps
                and subobs_id < self.sd_ctx.dump_end_gps
            ):
                # We ARE in voltage dump and so we go and do a VCS capture instead
                # of whatever else we were doing
                logger.info(
                    f"{item}: ignoring existing mode: {subfile_mode} as we"
                    f" are within a voltage dump ({self.sd_ctx.dump_start_gps} <"
                    f" {self.sd_ctx.dump_end_gps}) for trigger {self.sd_ctx.dump_trigger_id}."
                    " Doing VCS instead."
                )

                # Pause archiving so we have the disk to ourselves
                if self.archive_destination_enabled:  # pylint: disable=line-too-long
                    self.sd_ctx.pause_archiving(True)  # pylint: disable=line-too-long

                # See if there already is a TRIGGER_ID keyword in the subfile- if so
                # don't overwrite it. We must have overlapping triggers happening
                if not utils.read_subfile_trigger_value(item):
                    # No TRIGGER_ID yet, so add it
                    logger.info(
                        f"{item}: injecting {utils.PSRDADA_TRIGGER_ID} {self.sd_ctx.dump_trigger_id} into subfile..."
                    )
                    utils.inject_subfile_header(item, f"{utils.PSRDADA_TRIGGER_ID} {self.sd_ctx.dump_trigger_id}\n")

                success = utils.copy_subfile_to_disk_dd(
                    item,
                    self.corr_devshm_numa_node,
                    self.voltdata_incoming_path,
                    self.copy_subfile_to_disk_timeout_sec,
                    os.path.split(item)[1],
                    subfile_bytes_to_write,
                )
            else:
                # 1. Read header of subfile.
                # 2. If mode==MWAX_CORRELATOR then
                #       load into PSRDADA ringbuffer for correlator input
                #    else if mode==MWAX_VCS then
                #       Ensure archiving to Pawsey is stopped while we capture
                #       copy subfile onto /voltdata where it will eventually
                #       get archived
                #    else if mode==MWAX_BEAMFORMER then
                #       Send obs_subobs info to beamformer via named pipe
                #    else
                #       Ignore the subfile
                # 3. Rename .sub file to .free so that udpgrab can reuse it

                logger.debug(f"{item}: MODE is {subfile_mode}")

                if utils.CorrelatorMode.is_correlator(subfile_mode):
                    # Check if we're in the right mwax_subfile_distributor mode
                    if self.subfile_dist_mode == utils.MWAXSubfileDistirbutorMode.CORRELATOR:
                        # This is a normal MWAX_CORRELATOR obs, continue as normal
                        if self.archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.pause_archiving(False)  # pylint: disable=line-too-long

                        success = utils.load_psrdada_ringbuffer(
                            item,
                            self.corr_ringbuffer_key,
                            -1,
                            self.psrdada_timeout_sec,
                        )

                        if self.always_keep_subfiles:
                            keep_subfiles_path = self.voltdata_incoming_path
                    else:
                        # Ignore
                        logger.warning(
                            f"{item}: ignoring subfile as it's MODE {subfile_mode} is not compatible with mwax_subfiledistributor NOT running in CORRELATOR mode"
                        )
                        success = True  # It's True because that signals the caller to keep going and don't retry

                elif utils.CorrelatorMode.is_vcs(subfile_mode):
                    # Check if we're in the right mwax_subfile_distributor mode
                    if self.subfile_dist_mode == utils.MWAXSubfileDistirbutorMode.CORRELATOR:
                        # Pause archiving so we have the disk to ourselves
                        if self.archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.pause_archiving(True)  # pylint: disable=line-too-long

                        success = utils.copy_subfile_to_disk_dd(
                            item,
                            self.corr_devshm_numa_node,
                            self.voltdata_incoming_path,
                            self.copy_subfile_to_disk_timeout_sec,
                            os.path.split(item)[1],
                            subfile_bytes_to_write,
                        )
                    else:
                        # Ignore
                        logger.warning(
                            f"{item}: ignoring subfile as it's MODE {subfile_mode} is not compatible with mwax_subfiledistributor NOT running in CORRELATOR mode"
                        )
                        success = True  # It's True because that signals the caller to keep going and don't retry

                elif utils.CorrelatorMode.is_beamformer(subfile_mode):
                    if self.subfile_dist_mode == utils.MWAXSubfileDistirbutorMode.BEAMFORMER:
                        # This is a beamformer obs, enable archiving as normal (if configured)
                        if self.archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.pause_archiving(False)

                        # Get number of inputs and coarse channel from header
                        if subfile_header_values[utils.PSRDADA_COARSE_CHANNEL] is None:
                            raise ValueError(f"Keyword {utils.PSRDADA_COARSE_CHANNEL} not found in {item}")
                        rec_chan_no = int(subfile_header_values[utils.PSRDADA_COARSE_CHANNEL])

                        # Get cal_obsid from metafits
                        metafits_filename = os.path.join(self.metafits_path, f"{obs_id}_metafits.fits")
                        METAFITS_CALOBSID = "CALOBSID"
                        METAFITS_CALIBDATA_HDU = "CALIBDATA"
                        try:
                            cal_obs_id_str = utils.get_metafits_value_from_hdu(
                                metafits_filename, METAFITS_CALIBDATA_HDU, METAFITS_CALOBSID
                            )
                        except Exception:
                            logger.warning(
                                f"{item}: key {METAFITS_CALOBSID} not found in metafits file {metafits_filename} hdu {METAFITS_CALIBDATA_HDU}"
                            )
                            cal_obs_id_str = "0"

                        try:
                            cal_obs_id: int = int(cal_obs_id_str)
                        except Exception:
                            logger.warning(
                                f"{item}: value {cal_obs_id_str} for key {METAFITS_CALOBSID} in {metafits_filename} hdu {METAFITS_CALIBDATA_HDU} is not a number"
                            )
                            cal_obs_id = 0

                        success = self.signal_beamformer(item, cal_obs_id, rec_chan_no)
                    else:
                        # Ignore
                        logger.warning(
                            f"{item}: ignoring subfile as it's MODE {subfile_mode} is not compatible with mwax_subfiledistributor NOT running in BEAMFORMER mode"
                        )
                        success = True  # It's True because that signals the caller to keep going and don't retry

                elif utils.CorrelatorMode.is_no_capture(subfile_mode) or utils.CorrelatorMode.is_voltage_buffer(
                    subfile_mode
                ):
                    logger.info(f"{item}: ignoring due to mode: {subfile_mode}")

                    #
                    # This is our opportunity to write any "keep" files to disk
                    # which were held from a voltage buffer dump
                    #
                    if self.sd_ctx.dump_keep_file_queue.qsize() > 0:
                        # Pause archiving
                        if self.archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.pause_archiving(True)  # pylint: disable=line-too-long

                        # Since we're not doing anything with this subfile we can
                        # try and handle any remaining keep files
                        self.handle_next_keep_file()
                    else:
                        # Unpause archiving
                        if self.archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.pause_archiving(False)  # pylint: disable=line-too-long

                    success = True

                else:
                    logger.error(f"{item}: Unknown subfile mode {subfile_mode}, ignoring.")
                    success = True

                # There is a semi-rare case where in between the top of this code and now
                # a voltage trigger has been received. If so THIS subfile may not have been added to
                # the keep list, so deal with it now

                # Commenting out below for now as it is not necessary I think...
                # if (
                #    (
                #        self.dump_start_gps is not None
                #        and self.dump_end_gps is not None
                #    )
                #    and subobs_id >= self.dump_start_gps
                #    and subobs_id < self.dump_end_gps
                # ):
                #    # Rename it now to .keep
                #    keep_filename = item.replace(
                #        self.ext_sub_file, self.ext_keep_file
                #    )
                #
                #    os.rename(item, keep_filename)
                #
                #    # add it to the queue
                #    # Lucky for us it will add it to the bottom.
                #    self.dump_keep_file_queue.put(keep_filename)

            # Check if we need to clear the dump info
            if self.sd_ctx.dump_end_gps is not None:
                if subobs_id >= self.sd_ctx.dump_end_gps:
                    # Reset the dump start and end
                    self.sd_ctx.dump_start_gps = None
                    self.sd_ctx.dump_end_gps = None
                    self.sd_ctx.dump_trigger_id = None

        except Exception as handler_exception:  # pylint: disable=broad-except
            logger.error(f"{item}: {handler_exception}")
            success = False

        finally:
            if success:
                # Check if need to keep subfiles, if so we need to copy
                # them off
                if self.always_keep_subfiles and keep_subfiles_path:
                    # we use -1 for numa node for now as this is really for
                    # debug so it does not matter
                    utils.copy_subfile_to_disk_dd(
                        item,
                        self.corr_devshm_numa_node,
                        keep_subfiles_path,
                        self.copy_subfile_to_disk_timeout_sec,
                        os.path.split(item)[1],
                        subfile_bytes_to_write,
                    )

                # Rename to free but not if we are in MWAX_BEAMFORMER mode as the BF will
                # take care of it when it has finished with it
                if (
                    utils.CorrelatorMode.is_beamformer(subfile_mode)
                    and self.subfile_dist_mode == utils.MWAXSubfileDistirbutorMode.BEAMFORMER
                ):
                    # Don't rename .sub to .free- the beamformer does it
                    pass
                else:
                    # Rename subfile so that udpgrab can reuse it
                    free_filename = item.replace(self.ext_sub_file, self.ext_free_file)

                    try:
                        # Check it exists first- the dump process
                        # may have renamed it already to .keep
                        try:
                            shutil.move(item, free_filename)
                        except FileNotFoundError:
                            pass

                    except Exception as move_exception:  # pylint: disable=broad-except
                        logger.error(f"{item}: Could not rename {item} back to {free_filename}. Error {move_exception}")
                        sys.exit(2)

            handler_elapsed = time.time() - handler_starttime

            logger.info(
                f"{item}: SubfileIncomingProcessor.subfile_handler finished handling in {handler_elapsed:.3f} secs."
            )

        return success

    def signal_beamformer(self, item: str, cal_obs_id: int, rec_chan_no: int) -> bool:
        """Signal the beamformer with calibration data via Redis.

        Searches for the appropriate aocal calibration file and solution FITS file,
        then pushes a message to Redis with subfile path and calibration details.

        Args:
            item: Full path of the subfile being processed.
            cal_obs_id: Calibration observation ID from metafits.
            rec_chan_no: Receiver channel (coarse channel) number.

        Returns:
            True if beamformer signalling succeeded.

        Raises:
            SystemExit: Exits with code 3 if Redis communication fails.
        """

        # We don't know the exact aocal filename as we dont have num_fine_chans
        # so we'll use a wildcard
        aocal_filename = ""
        partial_aocal_filename = os.path.join(self.bf_cal_path, get_partial_aocal_filename(cal_obs_id, rec_chan_no))
        try:
            found_files = glob.glob(partial_aocal_filename)
            if not found_files:
                logger.warning(f"{item}: No aocal file found matching pattern {partial_aocal_filename}.")
            else:
                aocal_filename = found_files[0]
                logger.info(f"{item}: Found aocal file {aocal_filename} for pattern {partial_aocal_filename}.")
        except Exception as e:
            logger.error(f"{item}: Error searching for aocal file with pattern {partial_aocal_filename}. Error: {e}")

        # Get the fits solution file too
        sol_fits_filename = get_solution_fits_filename(self.bf_cal_path, cal_obs_id, rec_chan_no)

        if not sol_fits_filename:
            sol_fits_filename = ""

        # Now signal with what we got
        signal_value = {"subfile": item, "aocalfile": aocal_filename, "calsolfile": sol_fits_filename}
        logger.info(f"{item}: Signalling beamformer with ({signal_value}) via redis {self.bf_redis_host}...")
        try:
            # Write the signal value- if reader disconnects it will auto-reopen unless timeout is hit
            utils.push_message_to_redis(self.bf_redis_host, self.bf_redis_queue_key, signal_value)
            # Success
            logger.info(f"{item}: Signalling beamformer success")
            return True

        except Exception:
            logger.exception(f"{item}: signal_beamformer failed.")
            exit(3)

    def handle_next_keep_file(self) -> bool:
        """Process and write a kept voltage dump subfile to disk.

        Retrieves the next kept subfile from the dump queue, copies it to the
        voltdata incoming directory as a .sub file, then renames it to .free.

        Returns:
            True if the file was successfully copied and renamed.
            False if an error occurred (file is requeued).

        Raises:
            SystemExit: Exits with code 2 if file rename fails.
        """
        success = True

        # Get next keep file off the queue
        keep_filename = self.sd_ctx.dump_keep_file_queue.get()

        logger.info(f"SubfileProcessor.handle_next_keep_file is handling {keep_filename}...")

        # Read TRANSFER_SIZE from subfile header
        # We only use this when writing a subfile to disk in case the subfile is
        # bigger than the data
        transfer_size_str = utils.read_subfile_value(keep_filename, utils.PSRDADA_TRANSFER_SIZE)
        if transfer_size_str is None:
            raise ValueError(f"Keyword {utils.PSRDADA_TRANSFER_SIZE} not found in {keep_filename}")

        transfer_size = int(transfer_size_str)
        subfile_bytes_to_write = transfer_size + utils.PSRDADA_HEADER_BYTES  # We add the header to the transfer size

        # Copy the .keep file to the voltdata incoming dir
        # and ensure it is named as a ".sub" file
        copy_success = utils.copy_subfile_to_disk_dd(
            keep_filename,
            self.corr_diskdb_numa_node,
            self.voltdata_incoming_path,
            self.copy_subfile_to_disk_timeout_sec,
            os.path.basename(keep_filename).replace(self.ext_keep_file, self.ext_sub_file),
            subfile_bytes_to_write,
        )

        if copy_success:
            # Rename kept subfile so that mwax_u2s can reuse it
            free_filename = keep_filename.replace(self.ext_keep_file, self.ext_free_file)

            try:
                shutil.move(keep_filename, free_filename)
            except Exception as move_exception:  # pylint: disable=broad-except
                logger.error(f"Could not rename {keep_filename} back to {free_filename}. Error {move_exception}")
                sys.exit(2)

        else:
            # Reqeuue file to try again later
            self.sd_ctx.dump_keep_file_queue.put(keep_filename)

        logger.info(
            "SubfileProcessor.handle_next_keep_file finished handling"
            f" {keep_filename}. Remaining .keep files:"
            f" {self.sd_ctx.dump_keep_file_queue.qsize()}."
        )

        return success
