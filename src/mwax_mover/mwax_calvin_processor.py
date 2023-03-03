"""
Module hosting the MWAXCalvinProcessor for near-realtime
calibration
"""
import argparse
from configparser import ConfigParser
import datetime
import glob
import json
import logging
import logging.handlers
import os
import queue
import shutil
import signal
import sys
import threading
import time
from astropy import time as astrotime
from mwax_mover import (
    utils,
    version,
    mwax_watcher,
    mwax_queue_worker,
    mwax_calvin_utils,
)
from mwax_mover.mwax_mover import (
    MODE_WATCH_DIR_FOR_NEW,
)
from mwax_mover.mwax_command import (
    run_command_popen,
    check_popen_finished,
)


class MWAXCalvinProcessor:
    """The main class processing calibration solutions"""

    def __init__(
        self,
    ):
        # General
        self.logger = None
        self.log_path = None
        self.hostname = None

        # health
        self.health_multicast_interface_ip = None
        self.health_multicast_interface_name = None
        self.health_multicast_ip = None
        self.health_multicast_port = None
        self.health_multicast_hops = None

        self.running = False
        self.ready_to_exit = False

        self.watchers = []
        self.queue_workers = []
        self.watcher_threads = []
        self.worker_threads = []

        # assembly
        self.incoming_watch_path = None
        self.assembly_watch_queue = queue.Queue()
        self.assemble_path = None
        self.assemble_check_seconds = None
        self.obsid_check_assembled_thread = None

        # processing
        self.processing_path = None
        self.processing_error_path = None
        self.processing_complete_path = None
        self.keep_completed_visibility_files = None
        self.processing_queue = queue.Queue()
        self.source_list_filename = None
        self.source_list_type = None

        # birli
        self.birli_timeout = None
        self.birli_binary_path = None
        self.birli_popen_process = None

        # hyperdrive
        self.hyperdrive_timeout = None
        self.hyperdrive_binary_path = None
        self.hyperdrive_popen_process = None

    def start(self):
        """Start the processor"""
        self.running = True

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(
            name="health_thread", target=self.health_loop, daemon=True
        )
        health_thread.start()

        #
        # Do initial scan for directories to add to the processing
        # queue (in the processing_path)
        #
        scanned_dirs = utils.scan_directory(
            self.logger, self.processing_path, "", False, None
        )
        for item in scanned_dirs:
            if os.path.isdir(item):
                self.add_to_processing_queue(item)

        #
        # Create watchers
        #
        self.logger.info("Creating watchers...")
        # Create watcher for the incoming watch path queue
        new_watcher = mwax_watcher.Watcher(
            name="incoming_watcher",
            path=self.incoming_watch_path,
            dest_queue=self.assembly_watch_queue,
            pattern=".fits",
            log=self.logger,
            mode=MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
            exclude_pattern=None,
        )
        self.watchers.append(new_watcher)

        #
        # Create queue workers
        #

        # Create queueworker for assembly queue
        self.logger.info("Creating workers...")
        new_worker = mwax_queue_worker.QueueWorker(
            name="incoming_worker",
            source_queue=self.assembly_watch_queue,
            executable_path=None,
            event_handler=self.incoming_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=True,
            exit_once_queue_empty=False,
            requeue_on_error=True,
        )
        self.queue_workers.append(new_worker)

        # Create queueworker for assembly queue
        # if we fail, do not requeue, move to processing_error dir
        new_worker = mwax_queue_worker.QueueWorker(
            name="processing_worker",
            source_queue=self.processing_queue,
            executable_path=None,
            event_handler=self.processing_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=False,
            exit_once_queue_empty=False,
            requeue_on_error=False,
        )
        self.queue_workers.append(new_worker)

        self.logger.info("Starting watchers...")
        # Setup threads for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(
                name=f"watch_thread{i}", target=watcher.start, daemon=True
            )
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()

        self.logger.info("Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.watchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.watchers:
                if not watcher.scan_completed:
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        self.logger.info("Watchers are finished scanning.")

        self.logger.info("Starting workers...")
        # Setup threads for processing items
        for i, worker in enumerate(self.queue_workers):
            queue_worker_thread = threading.Thread(
                name=f"worker_thread{i}", target=worker.start, daemon=True
            )
            self.worker_threads.append(queue_worker_thread)
            queue_worker_thread.start()

        # Start obs_id_check_thread
        self.obsid_check_assembled_thread = threading.Thread(
            name="obsid_assemble_thread",
            target=self.obsid_check_assembled_handler,
            daemon=True,
        )
        self.obsid_check_assembled_thread.start()
        self.worker_threads.append(self.obsid_check_assembled_thread)

        self.logger.info("Started...")

        while self.running:
            for worker_thread in self.worker_threads:
                if worker_thread:
                    if worker_thread.is_alive():
                        time.sleep(1)
                    else:
                        self.running = False
                        break

        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            time.sleep(1)

        # Final log message
        self.logger.info("Completed Successfully")

    def incoming_handler(self, item) -> bool:
        """This is triggered each time a new fits file
        appears in the incoming_path"""
        self.logger.info(f"Handling... incoming FITS file {item}...")
        filename = os.path.basename(item)
        obs_id: int = int(filename[0:10])

        obsid_assembly_dir = os.path.join(self.assemble_path, str(obs_id))
        if not os.path.exists(obsid_assembly_dir):
            self.logger.info(
                f"{item} creating obs_id's assembly dir"
                f" {obsid_assembly_dir}..."
            )
            # This is the first file of this obs_id to be seen
            os.mkdir(obsid_assembly_dir)

        # Relocate this file to the obs_id_work_dir
        new_filename = os.path.join(obsid_assembly_dir, filename)
        self.logger.info(
            f"{item} moving file into obs_id's assembly dir {new_filename}..."
        )
        shutil.move(item, new_filename)
        return True

    def check_obs_is_ready_to_process(
        self, obs_id, obsid_assembly_dir
    ) -> bool:
        """This routine checks to see if an observation is ready to be processed
        by hyperdrive"""
        #
        # Check we have a metafits
        #
        metafits_filename = f"{obs_id}_metafits.fits"
        metafits_assembly_filename = os.path.join(
            obsid_assembly_dir, metafits_filename
        )

        if not os.path.exists(metafits_assembly_filename):
            # download the metafits file to the assembly dir for the obs
            try:
                self.logger.info(f"{obs_id} Downloading metafits file...")
                utils.download_metafits_file(obs_id, obsid_assembly_dir)
                self.logger.info(f"{obs_id} metafits downloaded successfully")
            except Exception as catch_all_exception:
                self.logger.error(
                    f"Metafits file {metafits_assembly_filename} did not exist"
                    " and could not download one from web"
                    f" service. {catch_all_exception}"
                )
                return False

        # Get the duration of the obs from the metafits and only proceed
        # if the current gps time is > the obs_id + duration + a constant
        exp_time = int(
            utils.get_metafits_value(metafits_assembly_filename, "EXPOSURE")
        )
        current_gpstime = astrotime.Time(
            datetime.datetime.utcnow(), scale="utc"
        ).gps

        if current_gpstime > (int(obs_id) + exp_time):
            #
            # perform web service call to get list of data files from obsid
            #
            json_metadata = utils.get_data_files_for_obsid_from_webservice(
                obs_id
            )

            #
            # we need a list of files from the web service
            # this should just be the filenames
            #
            web_service_filenames = [filename for filename in json_metadata]
            web_service_filenames.sort()

            # we need a list of files from the work dir
            # this first list has the full path
            # put a basic UNIX pattern so we don't pick up the metafits

            # Check for gpubox files (mwax OR legacy)
            glob_spec = "*.fits"
            assembly_dir_full_path_files = glob.glob(
                os.path.join(obsid_assembly_dir, glob_spec)
            )
            assembly_dir_filenames = [
                os.path.basename(i) for i in assembly_dir_full_path_files
            ]
            assembly_dir_filenames.sort()
            # Remove the metafits file
            assembly_dir_filenames.remove(metafits_filename)

            # How does what we need compare to what we have?
            return_value = web_service_filenames == assembly_dir_filenames

            self.logger.debug(
                f"{obs_id} check_obs_is_ready_to_process() =="
                f" {return_value} (WS: {len(web_service_filenames)},"
                f" assembly_dir: {len(assembly_dir_filenames)})"
            )
            return return_value
        else:
            self.logger.info(
                f"{obs_id} Observation is still in progress:"
                f" {current_gpstime} < ({obs_id} - {int(obs_id)+exp_time})"
            )
            return False

    def obsid_check_assembled_handler(self):
        """This thread sleeps most of the time, but wakes up
        to check if there are any completely assembled sets of
        gpubox files which we should process"""
        while self.running:
            self.logger.debug(
                f"sleeping for {self.assemble_check_seconds} secs"
            )
            time.sleep(self.assemble_check_seconds)

            if self.running:
                self.logger.debug(
                    "Waking up and checking un-assembled observations..."
                )

                obs_id_list = []

                # make a list of all obs_ids in the work path
                for filename in os.listdir(self.assemble_path):
                    full_filename = os.path.join(self.assemble_path, filename)
                    if os.path.isdir(full_filename):
                        obs_id_list.append(filename)

                # sort it
                obs_id_list.sort()

                # Check each one
                for obs_id in obs_id_list:
                    obs_assemble_path = os.path.join(
                        self.assemble_path, obs_id
                    )
                    if self.check_obs_is_ready_to_process(
                        obs_id, obs_assemble_path
                    ):
                        # do processing
                        obs_processing_path = os.path.join(
                            self.processing_path, obs_id
                        )

                        self.logger.info(
                            f"{obs_id} is ready for processing. Moving"
                            f" {obs_assemble_path} to {obs_processing_path}"
                        )

                        # Move the directory to the processing path
                        shutil.move(obs_assemble_path, obs_processing_path)
                        self.add_to_processing_queue(obs_processing_path)
        return True

    def add_to_processing_queue(self, item):
        """Adds a dir containing all the files for an obsid
        to the processing queue"""
        self.processing_queue.put(item)
        self.logger.info(
            f"{item} added to processing_queue."
            f" ({self.processing_queue.qsize()}) in queue."
        )

    def processing_handler(self, item) -> bool:
        """This is triggered when an obsid dir is moved into
        the processing directory, indicating it is ready to
        have birli then hyperdrive run on it"""
        birli_success: bool = False
        success: bool = False

        stdout = ""
        elapsed = -1

        file_no_path = item.split("/")
        obs_id = file_no_path[-1][0:10]
        metafits_filename = os.path.join(item, str(obs_id) + "_metafits.fits")

        # Get only data files
        data_files = glob.glob(os.path.join(item, "*.fits"))
        # Remove the metafits (we specify it seperately)
        data_files.remove(metafits_filename)

        data_file_arg = ""
        for data_file in data_files:
            data_file_arg += f"{data_file} "

        uvfits_filename = os.path.join(item, str(obs_id) + ".uvfits")

        try:
            # Run birli
            cmdline = (
                f"{self.birli_binary_path}"
                f" --metafits {metafits_filename}"
                " --no-draw-progress"
                f" --uvfits-out={uvfits_filename}"
                f" --flag-edge-width={80}"
                f" {data_file_arg}"
            )

            start_time = time.time()

            self.birli_popen_process = run_command_popen(
                self.logger, cmdline, -1, False
            )

            exit_code, stdout, stderr = check_popen_finished(
                self.logger,
                self.birli_popen_process,
                self.birli_timeout,
            )

            # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
            elapsed = time.time() - start_time

            if exit_code == 0:
                ## Success!
                self.logger.info(
                    f"{obs_id}: Birli run successful in {elapsed:.3f} seconds"
                )
                birli_success = True
                self.birli_popen_process = None

                ## Success!
                # Write out a useful file of command line info
                readme_filename = os.path.join(
                    item, f"{obs_id}_birli_readme.txt"
                )
                mwax_calvin_utils.write_readme_file(
                    self.logger,
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )
            else:
                self.logger.error(
                    f"{obs_id}: Birli run FAILED: Exit code of {exit_code} in"
                    f" {elapsed:.3f} seconds: {stderr}"
                )
        except Exception as hyperdrive_run_exception:
            self.logger.error(
                f"{obs_id}: hyperdrive run FAILED: Unhandled exception"
                f" {hyperdrive_run_exception} in {elapsed:.3f} seconds:"
                f" {stderr}"
            )

        if birli_success:
            # If all good run hyperdrive- once per uvfits file created
            # N (where N>1) uvfits are generated if Birli sees the obs is picket fence
            # Therefore we need to run hyperdrive N times too
            #
            # get a list of the uvfits files
            uvfits_files = glob.glob(os.path.join(item, "*.uvfits"))

            self.logger.info(
                f"{obs_id}: {len(uvfits_files)} contiguous bands detected."
                f" Running hyperdrive {len(uvfits_files)} times...."
            )

            # Keep track of the number of successful hyperdrive runs
            hyperdrive_runs_success: int = 0

            for hyperdrive_run, uvfits_file in enumerate(uvfits_files):
                # Take the filename which for picket fence will also have
                # the band info and in all cases the obsid. We will use
                # this as a base for other files we work with
                obsid_and_band = uvfits_file.replace(".uvfits", "")

                try:
                    hyperdrive_solution_filename = (
                        f"{obsid_and_band}_solutions.fits"
                    )
                    bin_solution_filename = f"{obsid_and_band}_solutions.bin"

                    # Run hyperdrive
                    # Output to hyperdrive format and old aocal format (bin)
                    cmdline = (
                        f"{self.hyperdrive_binary_path} di-calibrate"
                        " --no-progress-bars --data"
                        f" {uvfits_file} {metafits_filename} --num-sources 5"
                        " --source-list"
                        f" {self.source_list_filename} --source-list-type"
                        f" {self.source_list_type} --outputs"
                        f" {hyperdrive_solution_filename} {bin_solution_filename}"
                    )

                    start_time = time.time()

                    # run hyperdrive
                    self.hyperdrive_popen_process = run_command_popen(
                        self.logger, cmdline, -1, False
                    )

                    exit_code, stdout, stderr = check_popen_finished(
                        self.logger,
                        self.hyperdrive_popen_process,
                        self.hyperdrive_timeout,
                    )

                    # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
                    elapsed = time.time() - start_time

                    if exit_code == 0:
                        self.logger.info(
                            f"{obs_id}: hyperdrive run"
                            f" {hyperdrive_run + 1}/{len(uvfits_files)} successful"
                            f" in {elapsed:.3f} seconds"
                        )
                        success = True
                        self.hyperdrive_popen_process = None

                        ## Success!
                        # Write out a useful file of command line info
                        readme_filename = (
                            f"{obsid_and_band}_hyperdrive_readme.txt"
                        )

                        mwax_calvin_utils.write_readme_file(
                            self.logger,
                            readme_filename,
                            cmdline,
                            exit_code,
                            stdout,
                            stderr,
                        )

                        hyperdrive_runs_success += 1
                    else:
                        self.logger.error(
                            f"{obs_id}: hyperdrive run"
                            f" {hyperdrive_run + 1}/{len(uvfits_files)} FAILED:"
                            f" Exit code of {exit_code} in"
                            f" {elapsed:.3f} seconds. StdErr: {stderr}"
                        )
                        # exit for loop- since run failed
                        break
                except Exception as hyperdrive_run_exception:
                    self.logger.error(
                        f"{obs_id}: hyperdrive run"
                        f" {hyperdrive_run + 1}/{len(uvfits_files)} FAILED:"
                        " Unhandled exception"
                        f" {hyperdrive_run_exception} in"
                        f" {elapsed:.3f} seconds. StdErr: {stderr}"
                    )
                    # exit for loop since run failed
                    break

        # Did we have N number of successful runs?
        if hyperdrive_runs_success == len(uvfits_files):
            # Processing successful, run stats
            # produce stats/plots
            stats_successful: int = 0

            self.logger.info(
                f"{obs_id}: {len(uvfits_files)} contiguous bands detected."
                f" Running hyperdrive stats {len(uvfits_files)} times...."
            )

            for hyperdrive_stats_run, uvfits_file in enumerate(uvfits_files):
                # Take the filename which for picket fence will also have
                # the band info and in all cases the obsid. We will use
                # this as a base for other files we work with
                obsid_and_band = uvfits_file.replace(".uvfits", "")

                hyperdrive_solution_filename = (
                    f"{obsid_and_band}_solutions.fits"
                )
                stats_filename = f"{obsid_and_band}_stats.txt"

                (
                    stats_success,
                    stats_error,
                ) = mwax_calvin_utils.write_stats(
                    self.logger,
                    obs_id,
                    item,
                    stats_filename,
                    hyperdrive_solution_filename,
                    self.hyperdrive_binary_path,
                    metafits_filename,
                )

                if stats_success:
                    stats_successful += 1
                else:
                    self.logger.warning(
                        f"{obs_id}: hyperdrive stats run"
                        f" {hyperdrive_stats_run + 1}/{len(uvfits_files)} FAILED:"
                        f" {stats_error} in {elapsed:.3f} seconds."
                    )

            if stats_successful == len(uvfits_files):
                self.logger.info(
                    f"{obs_id}: All {stats_successful} hyperdrive stats runs"
                    " successful"
                )
            else:
                self.logger.warning(
                    f"{obs_id}: Not all hyperdrive stats runs were successful."
                )

            # now move the whole dir
            # to the processing_complete_path
            complete_path = os.path.join(self.processing_complete_path, obs_id)
            self.logger.info(
                f"{obs_id}: moving successfull files to"
                f" {complete_path} for analysis"
            )
            shutil.move(item, complete_path)

            if not self.keep_completed_visibility_files:
                visibility_files = glob.glob(
                    os.path.join(item, f"{obs_id}_*_*_*.fits")
                )

                for file_to_delete in visibility_files:
                    os.remove(file_to_delete)

        else:
            # If we are not shutting down,
            # Move the files to an error dir
            #
            # If we are shutting down, then this error is because
            # we have effectively sent it a SIGINT. This should not be a
            # reason to abandon processing. Leave it there to be picked up
            # next run (ie this will trigger the "else" which does nothing)
            if self.running:
                error_path = os.path.join(self.processing_error_path, obs_id)
                self.logger.info(
                    f"{obs_id}: moving failed files to {error_path} for manual"
                    " analysis and writing readme_error.txt"
                )

                # Move the processing dir
                shutil.move(item, error_path)

                # Write out a useful file of error and command line info
                readme_filename = os.path.join(error_path, "readme_error.txt")
                mwax_calvin_utils.write_readme_file(
                    self.logger,
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )

        return success

    def stop(self):
        """Shutsdown all processes"""
        for watcher in self.watchers:
            watcher.stop()

        for queue_worker in self.queue_workers:
            queue_worker.stop()

        # check for a hyperdrive process and kill it
        self.logger.debug("Checking for running hyperdrive process...")
        if self.hyperdrive_popen_process:
            if not self.hyperdrive_popen_process.poll():
                self.logger.debug(
                    "Running hyperdrive process found. Sending it SIGINT..."
                )
                self.hyperdrive_popen_process.send_signal(signal.SIGINT)
                self.logger.debug("SIGINT sent to Hyperdrive")

        # Wait for threads to finish
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                self.logger.debug(f"Watcher {thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join()
                self.logger.debug(f"Watcher {thread_name} Stopped")

        for worker_thread in self.worker_threads:
            if worker_thread:
                thread_name = worker_thread.name
                self.logger.debug(f"QueueWorker {thread_name} Stopping...")
                if worker_thread.is_alive():
                    # Short timeout- everything other than a running hyperdrive
                    # instance should have joined by now.
                    worker_thread.join(timeout=10)
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

        self.ready_to_exit = True

    def health_loop(self):
        """Send health information via UDP multicast"""
        while self.running:
            # Code to run by the health thread
            status_dict = self.get_status()

            # Convert the status to bytes
            status_bytes = json.dumps(status_dict).encode("utf-8")

            # Send the bytes
            try:
                utils.send_multicast(
                    self.health_multicast_interface_ip,
                    self.health_multicast_ip,
                    self.health_multicast_port,
                    status_bytes,
                    self.health_multicast_hops,
                )
            except (
                Exception
            ) as catch_all_exception:  # pylint: disable=broad-except
                self.logger.warning(
                    "health_handler: Failed to send health information."
                    f" {catch_all_exception}"
                )

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
        """Returns status of all process as a dictionary"""
        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
        }

        watcher_list = []

        for watcher in self.watchers:
            status = dict({"name": "data_watcher"})
            status.update(watcher.get_status())
            watcher_list.append(status)

        worker_list = []

        if len(self.queue_workers) > 0:
            for i, worker in enumerate(self.queue_workers):
                status = dict({"name": f"archiver{i}"})
                status.update(worker.get_status())
                worker_list.append(status)

        processor_status_list = []
        processor = {
            "type": type(self).__name__,
            "watchers": watcher_list,
            "workers": worker_list,
        }
        processor_status_list.append(processor)

        status = {"main": main_status, "processors": processor_status_list}

        return status

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()

    def initialise(self, config_filename):
        """Initialise the processor from the command line"""
        # Get this hosts hostname
        self.hostname = utils.get_hostname()

        if not os.path.exists(config_filename):
            print(
                f"Configuration file location {config_filename} does not"
                " exist. Quitting."
            )
            sys.exit(1)

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Parse config file
        config = ConfigParser()
        config.read_file(open(config_filename, "r", encoding="utf-8"))

        # read from config file
        self.log_path = config.get("mwax mover", "log_path")

        if not os.path.exists(self.log_path):
            print(f"log_path {self.log_path} does not exist. Quiting.")
            sys.exit(1)

        # It's now safe to start logging
        # start logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        console_log = logging.StreamHandler()
        console_log.setLevel(logging.DEBUG)
        console_log.setFormatter(
            logging.Formatter(
                "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
            )
        )
        self.logger.addHandler(console_log)

        file_log = logging.FileHandler(
            filename=os.path.join(self.log_path, "main.log")
        )
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(
            logging.Formatter(
                "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
            )
        )
        self.logger.addHandler(file_log)

        self.logger.info(
            "Starting mwax_calvin_processor"
            f" processor...v{version.get_mwax_mover_version_string()}"
        )

        # health
        self.health_multicast_ip = utils.read_config(
            self.logger, config, "mwax mover", "health_multicast_ip"
        )
        self.health_multicast_port = int(
            utils.read_config(
                self.logger, config, "mwax mover", "health_multicast_port"
            )
        )
        self.health_multicast_hops = int(
            utils.read_config(
                self.logger, config, "mwax mover", "health_multicast_hops"
            )
        )
        self.health_multicast_interface_name = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "health_multicast_interface_name",
        )

        # get this hosts primary network interface ip
        self.health_multicast_interface_ip = utils.get_ip_address(
            self.health_multicast_interface_name
        )
        self.logger.info(
            f"IP for sending multicast: {self.health_multicast_interface_ip}"
        )

        #
        # Assembly config
        #

        # Get the watch dir
        self.incoming_watch_path = utils.read_config(
            self.logger,
            config,
            "assembly",
            "incoming_watch_path",
        )

        if not os.path.exists(self.incoming_watch_path):
            self.logger.error(
                "incoming_watch_path location "
                f" {self.incoming_watch_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Get the assemble dir
        self.assemble_path = utils.read_config(
            self.logger,
            config,
            "assembly",
            "assemble_path",
        )

        if not os.path.exists(self.assemble_path):
            self.logger.error(
                "assemble_path location "
                f" {self.assemble_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Set assemble_check_seconds
        # How many secs between waiting for all gpubox files to arrive?
        self.assemble_check_seconds = int(
            utils.read_config(
                self.logger, config, "assembly", "assemble_check_seconds"
            )
        )

        #
        # processing config
        #

        # Get the processing dir
        self.processing_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "processing_path",
        )

        if not os.path.exists(self.processing_path):
            self.logger.error(
                "processing_path location "
                f" {self.processing_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Get the processing error dir
        self.processing_error_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "processing_error_path",
        )

        if not os.path.exists(self.processing_error_path):
            self.logger.error(
                "processing_error_path location "
                f" {self.processing_error_path} does not exist. Quitting."
            )
            sys.exit(1)

        #
        # Hyperdrive config
        #
        self.source_list_filename = utils.read_config(
            self.logger,
            config,
            "processing",
            "source_list_filename",
        )

        if not os.path.exists(self.source_list_filename):
            self.logger.error(
                "source_list_filename location "
                f" {self.source_list_filename} does not exist. Quitting."
            )
            sys.exit(1)

        self.source_list_type = utils.read_config(
            self.logger,
            config,
            "processing",
            "source_list_type",
        )

        # hyperdrive timeout
        self.hyperdrive_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "hyperdrive_timeout",
            )
        )

        # Get the hyperdrive binary
        self.hyperdrive_binary_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "hyperdrive_binary_path",
        )

        if not os.path.exists(self.hyperdrive_binary_path):
            self.logger.error(
                "hyperdrive_binary_path location "
                f" {self.hyperdrive_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Birli timeout
        self.birli_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "birli_timeout",
            )
        )

        # Get the Birli binary
        self.birli_binary_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "birli_binary_path",
        )

        if not os.path.exists(self.birli_binary_path):
            self.logger.error(
                "birli_binary_path location "
                f" {self.birli_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Processing complete path
        self.processing_complete_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "processing_complete_path",
        )

        if not os.path.exists(self.processing_complete_path):
            self.logger.error(
                "processing_complete_path location "
                f" {self.processing_complete_path} does not exist. Quitting."
            )
            sys.exit(1)

        self.keep_completed_visibility_files = utils.read_config_bool(
            self.logger,
            config,
            "processing",
            "keep_completed_visibility_files",
        )

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_processor: a command line tool which is part of the"
            " MWA correlator for the MWA. It will monitor directories on each"
            " mwax server and, upon detecting a calibrator observation, will"
            " execute birli then hyperdrive to generate calibration"
            " solutions. (mwax_mover"
            f" v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument(
            "-c", "--cfg", required=True, help="Configuration file location.\n"
        )

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)


def main():
    """Mainline function"""
    processor = MWAXCalvinProcessor()

    try:
        processor.initialise_from_command_line()
        processor.start()
        sys.exit(0)
    except Exception as catch_all_exception:  # pylint: disable=broad-except
        if processor.logger:
            processor.logger.exception(str(catch_all_exception))
        else:
            print(str(catch_all_exception))


if __name__ == "__main__":
    main()
