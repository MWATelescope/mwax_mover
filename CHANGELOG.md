# Changelog

# 1.7.28,29 12-Jun-2026

* calvin_processor: If <24 coarse channels still insert db records for 24 channels.
* calvin_processor: Fix bug where successful jobs were being updated with "Cancelled: Received SIGTERM" in the calibration_request table.
* mwacache_archive_processor: make rclone copy -> rclone check delay a config file option.

# 1.7.27 12-Jun-2026

* calvin_controller: fix invalid nice value in sbatch script- negatives not allowed in our current slurm config.

# 1.7.26 12-Jun-2026

* calvin_controller: fix invalid nice value in sbatch script.
* calvin_processor: add task name in health packets for untarring.

# 1.7.25 10-Jun-2026

* calvin_processor: No longer splits and copies aocal files.

# 1.7.24 09-Jun-2026

* mwax_subfile_distributor: No longer pass aocal filename to redis / beamformer.

# 1.7.23 03-Jun-2026

* Fixed version code in backport of importlib_metadata.

# 1.7.22 29-May-2026

* calvin_processor: Check for the acacia donwload file existing. If it does't then it means it has expired (we took more than 7 days from ASVO to calibration to start) OR another recent calibration job for this obs occurred and deleted the tar file (in which case fail the job).

# 1.7.21 29-May-2026

* calvin_controller: added requestids to slurm script filename to prevent duplicates.

# 1.7.20 29-May-2026

* calvin_controller: Removed break in loop that was causing false "job not seen by giant squid warnings" too. Added request id to logging to make it more obvious.

# 1.7.19 29-May-2026

* calvin_controller: Introduce a short 5 sec wait after submitting jobs before running giant-squid-list to eliminate false "job not seen by giant squid warnings".

# 1.7.18 29-May-2026

* calvin_controller: added a lock and updated logic to prevent mutation of a list during iteration issue which was causing controller to lose track of jobs.

# 1.7.17 28-May-2026

* calvin_processor: delete tar file from Acacia Projects after successful calibration.

# 1.7.16 27-May-2026

* calvin_processor: fix bug where giant squid could not find the api key env variable.

# 1.7.15 27-May-2026

* calvin_processor: fixed syntax when using proxy.

# 1.7.14 27-May-2026

* calvin_processor: uses haproxy on 127.0.0.1 to proxy to the mwacache servers which proxy to Pawsey so calvin can use the 100G link to download data from MWA ASVO. Proxy currently hardcoded.
* calvin_processor: added download rate to the log message after successfully downloading a tar file.

# 1.7.13 25-May-2026

* calvin: Removed: no_ref tile from the hyperdrive plots command!
* moved all cli commands to the cli subdir.
* Added new CLI to regenerate the calvin plots with the latest hyperdrive plot settings, pull down the index.json, update index.json with new file size, modified and size, then reupload the plots and index.json to S3.

# 1.7.12 21-May-2026

* Fixed logging bug in rclone move in calvin controller

# 1.7.11 21-May-2026

* calvin_controller: reduced verbosity of logging.
* calvin_controller: Fixed bug where ASVO error job was not being removed from job list.
* calvin_controller: Tweaked nice to have greater range.
* calvin_processor: Fixed insert_calibration_fits row bug

# 1.7.10 20-May-2026

* calvin_processor: fixed parsing of `gain_max_cutoff` in config file to include floats.

# 1.7.9 20-May-2026

* calvin_processor: added new db column `gain_max_cutoff` to calibration_fit table.

# 1.7.8 19-May-2026

* calvin: Added missing "Preparing" ASVO state to list of valid states.
* calvin_processor: Disable gains cut off if value is not set or negative.

# 1.7.7 19-May-2026

* calvin_processor: Now sets gains of entire jones matrix to NaN if gain of any part of the matrix is > cut off specified in config file.
* calvin_processor: Hyperdrive amp plot Y max limit set to 5 (was previously unset, and thus used the max gain which can stil be <=cut_off making plots hard to read).

# 1.7.6 18-May-2026

* calvin_processor: Fixed hyperdrive log not being uploaded.
* calvin_processor: Turn down matplotlib logging verbosity.
* calvin: Implemented a very simple retry mechanism with backoff for giant-squid calls.

# 1.7.5 15-May-2026

* calvin_processor: Fixed swapping of width and height in index.json. Bumped index.json version to 2.
* calvin_controller: Fixed debug log which said no files were moved by rclone even when they were.

# 1.7.4 15-May-2026

* calvin_processor: Fixed index.json containing files it shouldn't have!

# 1.7.3 15-May-2026

* calvin_processor: Fixed logging typo.

# 1.7.2 15-May-2026

* calvin_processor: Now sets gains to NaN if gain is > cut off specified in config file.
* calvin_processor: Now solution fitting runs in parallel.

# 1.7.1 14-May-2026

* calvin: Now passing asvo job id to the processor to ensure giant squid knows how to download the obs.

# 1.7.0 13-May 2026

* calvin_processor: add config file items for hyperdrive: extra_args.
* calvin_processor: set number of sources to 1000 (up from 99) in the config file.
* calvin_processor: move phase_fit_niter config item to the processor section.
* calvin_processor: add full hyperdrive command line to calibration_fits table (calibration_command column).
* calvin_controller: add s3 config file items: s3_endpoints, s3_profile, s3_bucket.
  * Now uploads any solution images to S3 so they can be displayed by the Django (WS) server. Once uploaded they are deleted.
* Added standalone util to generate the index.json given a local file path.

# 1.6.24 11-May 2026

* calvin controller: Fixed bug where any MWA ASVO job that had an error would increment the error count every refresh interval.

# 1.6.23 07-May 2026

* Created standalone vdif combiner util.

# 1.6.22 07-May 2026

* calvin_processor: Fixed bug where tar file was not being deleted.

# 1.6.21 06-May 2026

* Ensure "bulk" calvin requests get lowest priority.

# 1.6.20 05-May 2026

* Created stand alone aocal file splitter.

# 1.6.19 01-May 2026

* calvin_processor: Fixed bug with untaring.

# 1.6.15-18 29-Apr-2026

* calvin_processor: If MWA ASVO download has expired, fail the job, but requeue it in the db.

# 1.6.14 29-Apr-2026

* calvin_processor: Minor fix for giant-squid download and untar to ensure tar file is always cleaned up.

# 1.6.13 23-Apr-2026

* calvin_processor: Get giant-squid to do the downloading (not wget) and use the retryable option (--keep-tar) which also means we need to run tar to untar it and delete the tar file when done.

# 1.6.10-12 15-Apr-2026

* calvin_controller: Fixed bug where asvo helper was removing old jobs from a list while iterating over it causing issues.
* calvin_processor: Fixed Unhandled Exception: cannot reshape array of size 524288 into shape (1,128,24,21,4,2)- code was not handling picket fence correctly
* calvin_controller: Give asvo jobs more wall time as they have to download large files from Pawsey

# 1.6.2-9 14-Apr-2026

* calvin_controller: limit number of ASVO jobs pulled in based on comparing the in progress ASVO download jobs to the config file value (max_in_progress_asvo_jobs).
* calvin_controller: send realtime jobs to the priority partition or if full, the gpu partition. Send ASVO jobs to the gpu partition with a higher nice value to lower priority.
* calvin_controller: added 3 new health attributes: 
  * "slurm_queue" (the number of slurm jobs queued or running)
  * "mwa_asvo_calibration_requests_queued" (the number of MWA ASVO calibration requests which have been held back due to the configured mwax in progress job limit)
  * "mwa_asvo_vis_jobs_in_progress" (the number of MWA ASVO visibility jobs for calvin which are not completed, error or cancelled)
* calvin_processor: fixed space in one of the health packet keys.

# 1.6.1 10-Apr-2026

* calvin: Finally populating the gains_quality fields.

# 1.6.0 09,10-Apr-2026

* Added tests for mwax_calvin_utils and mwax_calvin_solutions.
* Lots of fixes for tests
* Removed coloredlogs from dependencies
* conftest.py: Added pytest_configure hook that sets WARNING level on noisy third-party loggers (PIL, matplotlib, scipy, pandas, urllib3, asyncio, numexpr) so only mwax_mover output appears at DEBUG level during test runs
* mwax_calvin_utils:
  * fit_phase_line: now respects the niter parameter (TODO: needs testing!)
  * get_solns: refant index 0 falsy check. if not ref_tile_idx: → if ref_tile_idx is None: so that refant at DataFrame index 0 is handled correctly and not treated as False.
  * weights property: copy before mutating. results = self.results → results = self.results.copy() so the out-of-range filtering (< 0 and > 1e-4) actually takes effect on the weights calculation rather than being silently discarded.
  * GainFitInfo: replace magic number 24. Added module-level constant MWA_NUM_COARSE_CHANS = 24. GainFitInfo.nan() and GainFitInfo.default() now accept n_coarse: int = MWA_NUM_COARSE_CHANS parameter.
  * write_readme_file: rename misleading parameter names
  * ensure_system_byte_order: fix incorrect numpy call. arr.newbyteorder(system_byte_order) → np.frombuffer(arr.tobytes(), dtype=arr.dtype.newbyteorder("=")) — .newbyteorder() is a dtype method, not an ndarray method; frombuffer correctly reinterprets the bytes as native order.
* mwax_calvin_solutions:
  * Fix picket fence calibration gains- solution files are now sorted by their correct channel number.
  * Clean up of logging of NaNs

# 1.5.13-15 02-Apr-2026

* mwacache: If rclone copy or check fails, just return false and it will requeue and retry
* mwacache: Now that we're using haproxy, we need a small gap in time between rclone copy and rclone check since we may use different vss servers for both calls. So we just put in a small hold between copy and check.
* mwacache: Tweaked some of rclone's settings to boost throughput and added a 3 retry loop for rclone check to take into account vss's syncing with each other.
* voltage buffer dump: Added unit tests to check voltage buffer dumps work.
* unit tests: for watcher and priority_watcher.
* watchers: fix for inotify corner cases.

# 1.5.12 01-Apr-2026

* mwacache: now uses haproxy to try all available VSS endpoints.
  * Removed `ceph_endpoints` from mwacache.cfg config file.

# 1.5.11 27-Mar-2026-31-Mar-2026

* subfile_dist: Subfile distributor doesn't exit when there are leftover files.
* utils: refactor to fix a few corner cases.
* All: module docstrings added.
* All: function docstrings added.
* All: updated logging to include module/func names. Removed hardcoded module/func names from logging.
* mwax_bf_vdif_utils: Change to use mwalib >=2.0.3 to get target_name and start_ra and start_dec for voltage beams for the VDIF header.
* README.md: updated.
* Old broken tests removed.
* mwax_wqw_checksum_and_db: refactored to simplify handler logic. Added tests.
* Added FakeMWAXDBHandler to allow tests to mock SQL SELECTs and INSERTs.
* Added hacky check for archive_file_rclone to know if it is running under pytest and if so, just return true. This should be replaced by a proper Mock pattern in the future.
* Added tests for mwax_calvin_controller.

# 1.5.10 20-Mar-2026

* calvin_processor: Fixed AttributeError: 'MWAXDBHandler' object has no attribute 'execute_dml_row_within_transaction'

# 1.5.9 20-Mar-2026

* calvin_processor: Fixed bug which caused the aocal splitting to fail.
* mwacache_archiver: Fixed naming of outgoing workers so you can tell them apart in logs.

# 1.5.8 20-Mar-2026

* More unit tests
* calvin/subfile_distributor: renamed aocal_export_dir to cal_export_dir as it is now for aocal and FITS solutions
* calvin: Now copying in FITS solution files to cal_export dir
* subfile_dist: Added new key / value for solution files to be passed to redis queue
* mwax_calvin_utils: Fixing linter errors

# 1.5.7 19-Mar-2026

* Subfile_distributor: Fixed bug where pausing archiving at the start of a VCS obs breaks things
* Queueworkers: Added a small sleep in the while loop if paused
* VisStatsProcessor: Fixed archiving==1 vs archiving == True
* VDIF: Fixed retrieval of target_name from VOLTAGEBEAMS HDU in metafits

# 1.5.6 19-Mar-2026

* PriorityQueueWorker: fix inconsistent statuses.
* All: fix clean shutdown of psycopg pool.

# 1.5.5 18-Mar-2026

* BfStitchingProcessor: Fix bug where bf_stitching watcher picks up files other than .fil and .vdif
* SubfileDistributor: cfg_corr_archive_destination_enabled now is bool everywhere
* SubfileDistributor: Added extra logging

# 1.5.4 18-Mar-2026

* watchers: Suppress iNotify logging unless critical
* update tests

# 1.5.3 18-Mar-2026

* subfile_distributor: Removed the calibration_destination_enabled option as it wasn't being used.
* all: Removed passing loggers everywhere anti-pattern.
* mwacache_archiver: replaced individual workers, queues and watchers with a watch_queue_worker class.

# 1.5.2 12-Mar-2026

* calvin: Removed the "--max-amp 2" argument from hyperdrive plots

# 1.5.1 11-Mar-2026

* Unit tests and bug fixes for subfile distributor

# 1.5.0 11-Mar-2026

* Refactor to clean up all the watcher, queue and worker mess

# 1.4.4 20-Feb-2026

* archive_processor: Fix to ensure leftover files in bf/incoming don't crash subfile_distributor.

# 1.4.4 20-Feb-2026

* archive_processor: unstitched files that are kept are moved into dont_archive dir

# 1.4.3 20-Feb-2026

* subfile_processor: Fixed bug where redis message was being rpushed instead of lpushed
* archive_processor: Provide config option to keep unstitched files (beamformer)

# 1.4.2 18-Feb-2026

* archive_processor: More debugging for stitching process.
* archive_processor: will not delete beamformer files after stitching (TODO- change to config option)

# 1.4.1 17-Feb-2026

* Dependencies: now requires mwalib >= v2.0.1
* Updated test metafits files to have MWAX_BEAMFORMER mode

# 1.4.0 17-Feb-2026

* Dependencies: now requires mwalib >= v2.0.0
* archive_processor: using mwalib voltagebeam class we can now populate more of the VDIF hdr
* archive_processor: fixed bug where dont_archive queue was of the wrong type
* archive_processor: clean up pre-stitched vdif and file files

# 1.3.9 17-Feb-2026

* archive_processor: Fixed BF stitching when run for a host whos archiving is disabled.

# 1.3.8 16-Feb-2026

* subfile_processor: changed redis queue name it is now: "bfq_mwax26"- so "bfq_" will be read from config file and "mwax26" will be the hostname.

# 1.3.5-7 13-Feb-2026

* archive_processor: fixed handling of dont_archive_bf
* subfile_processor: fixed bug where redis json is single quoted.
* subfile_processor: fixed bug where all redis messages were sent thrice.


# 1.3.4 12-Feb-2026

* subfile_processor: another refactor- using redis to signal the beamformer now. Better testing.

# 1.3.3 11-Feb-2026

* subfile_processor: reworked the named_pipe handling to be MUCH simpler. Also aocal obsid is not worked out from the CALIBDATA / CALOBSID from the metafits file.

# 1.3.0-2 11-Feb-2026

* subfile_distributor: Now has a --mode param (c/C or b/B) to ensure it handles beamformer obs when b or correlator obs when c and ignores the other. Fixed error message.

# 1.2.17 11-Feb-2026

* subfile_processor: fixed int/str bug preventing subfileprocessor from handling beamformer subfiles

# 1.2.15-16 10-Feb-2026

* archive_processor: Added extra debug and fixed duplicate watcher_incoming_vis bug

# 1.2.11-14 06-Feb-2026

* archive_processor (beamformer): when end of an obs detected, stitch together filterbank and vdif files before archiving
* calvin_processor: copy_file_rsync now correctly displays the throughput of rsyncing files from the mwax boxes
* utils: the function which converts GB to GiB now divides instead of multiplies the conversion factor(!)

# 1.2.11-13 02-Feb-2026

* SubfileDistributor: Added better logging for unexpected termination of subprocessor threads

# 1.2.10 02-Feb-2026

* SubfileProcessor: Fixed named pipe so it only opens on first beamformer observation but then stays open

# 1.2.5-9 30-Jan-2026

* SubfileDistributor: fixed endpoint methods and shutdown code for web server

# 1.2.4 30-Jan-2026

* SubfileDistributor: changed status endpoint to GET

# 1.2.3 30-Jan-2026

* SubfileDistributor: switched from HTTPServer web server to Flask

## 1.2.1 - 2 30-Jan-2026

* Bug fix for subflie_distributor circular reference
* Bug fix for logging issue with archive_processor

## 1.2.0 30-Jan-2026

* MWAX Realtime Beamformer support:
  * SubfileProcessor will detect a BF observation and send the subfile filename plus a space plus the aocal filename to a named pipe (/home/mwa/bf_pipe).
  * SubfileProcessor will send EOF to named pipe on shutdown
    * If error writing to named pipe, then die (for now)
    * Beamformer will rename .sub to .free once finished (not mwax_mover)
    * Beamformer will output files for archiving to /voltdata/bf/outgoing    
  * MWAXArchiveProcessor will watch the /voltdata/bf/outgoing and then:
    * Checksum file
    * Insert row into database
    * Send file to mwacache boxes
  * mwacache_archiver will handle VDIF and Filterbank files for archiving
* Old "Beamformer" (aka the Breakthrough listen pipeline) is removed from mwax_mover codebase (mwax_filterbank_processor.py and beamformer mode of subfile_distributor)
* All logging is now to console/stdout instead of named log files:
  * mwax_subfile_distributor
  * mwacache_archiver
* Calvin: increased the wait time for realtime observations to be finished (and database records inserted)

## 1.1.4 29-Jan-2026

* SubfileDistributor: Added more debug and more shutdown code to prevent hanging on shutdown. Fixes #31.

## 1.1.3 27-Jan-2026

* Calvin: Reverted x_gains_pol0, x_gains_pol1, x_gains_sigma_resid (and same for y) back to populating 0s. x and y gains_quality is now reverted back to 1.0 for now.

## 1.1.2 22-Jan-2026

* Calvin: Reverted x_intercept and y_intercept so they get inserted into calibration_solutions rows as radians (they are back to being radains!).

## 1.1.1 22-Jan-2026

* Calvin: Fixed bug where x_intercept and y_intercept were being inserted into calibration_solutions rows as radians not degrees.

## 1.1.0 20-Jan-2026

* Calvin: Remove step that uses phase_diff.txt as it is no longer useful.
* Calvin: Invert the gains since the phases are negated and calculate pol0, pol1, sigma_res and quality for gains.

## 1.0.90 19-Jan-2026

* Calvin: Fix bug where the deletion of stale aolcal files was not picking up any files for deletion

## 1.0.90 17-Jan-2026

* Calvin: Fixed another bug with 24 band picket fence edge case (missed the path)

## 1.0.89 17-Jan-2026

* Calvin: Fixed another bug with 24 band picket fence edge case

## 1.0.88 17-Jan-2026

* Calvin: Fixed Silly bug when getting the rec chan number from the aocal file

## 1.0.87 17-Jan-2026

* Calvin: Fix for when there are already 1 aocal per coarse channel (24-band picket fence)- now the code renames them properly!

## 1.0.86 16-Jan-2026

* Calvin: Fix for when there are already 1 aocal per coarse channel (24-band picket fence)

## 1.0.85 16-Jan-2026

* calvin: Another attempt to fix the splitting of aocal files.
* calvin: Fix for calvin not producing hyperdrive stats txt file
* calvin: Logic change to calvin_controller to always start a new cal job even if it has seen this obs before 

## 1.0.84 16-Jan-2026

* calvin: Another attempt to fix the splitting of aocal files.
* calvin: If receiving a calibration request for one just process, reprocess it anyway.

## 1.0.84 15-Jan-2026

* calvin: Fixed bug where multiple copies of the requestid were being added to the calvin_controller list.

## 1.0.83 15-Jan-2026

* calvin: Fixed bug where aocal splitting was not working.

## 1.0.82 15-Jan-2026

* calvin: provide mechanism to save calibration aocal file to NFS share/dir and ensure no more than 24 hours of files are stored.
  * If config option [processing]->`aocal_export_path` exists in the config file, Hyperdrive created aocal.bin file is split into 1 file per coarse channel and then copied there. If not exist, ignore.
  * If config option [processing]->`aocal_export_path` exists in the config file, then [processing]->`aocal_max_age_hours` is used to determine how old the oldest aocal files can be before removal.
* calvin: fixed bug where the hyperdrive stats txt file was being written to the source code dir instead of the output dir.

## 1.0.81 13-Nov-2025

* Updated deps. Fixed bug where CORR_MODE_CHANGE would cause subfile to not be renamed to .free


## 1.0.80 24-Sep-2025

* calvin_processor: When trying to call the URL to release calibration files, try indefinitely since the mwax boxes may be in the processing of stopping/starting due to change from oversampling to critically sampling and so might be down for ~10-15 mins.

## 1.0.79 23-Sep-2025

* mwax_subfile_distributor: mwax_stats will only be run on visibilities which end in "_000.fits" (we ignore the 001,002, etc).

## 1.0.78 17-Sep-2025

* Calvin processor: More accurately calculates output size of Birli.

## 1.0.77 10-Sep-2025

* Calvin processor: slurm job walltime is now 4 hours (for slow to download/large ASVO jobs).
* Calvin processor: added slurm directive to send a USR1 signal on 5 mins before walltime hit to error the job properly in the database.

## 1.0.76 15-Aug-2025

* Birli now uses the default "auto" pfb_gains instead of "none" for oversampled observations.

## 1.0.75 01-Aug-2025

* Fixed bug in mwax_subfile_processor where the dd command was not correctly trimming subfiles to the TRANSFER_SIZE from the PSRDADA header.

## 1.0.74 22-Jul-2025

* mwax_calvin_controller: fixed SQL to not try to pick up SUN calibrators for realtime calibration

## 1.0.73 22-Jul-2025

* mwax_archive_processor: fixed logic so that C123 calibrator obs don't wait for calibration.

## 1.0.72 11-Jul-2025

* Calvin/mwax_archive_processor: major refactor to support HPC

## 0.23.7 1-May-2025

* Calvin: fixed passing correct passband option to Birli

## 0.23.6 14-Apr-2025

* Calvin: fix for critically sampled observations (which don't have the OVERSAMP keyword in metafits).

## 0.23.5 14-Apr-2025

* Calvin: If obs is oversampled, ensure Birli doesn't flag edge with and does not correct passband.

## 0.23.4 17-Mar-2025

* Changed config of mwax_stats_executable to mwax_stats_dir - now that 2 binaries live in that dir.

## 0.23.0-0.23.3 14-Mar-2025

* Replaced Python packet_stats code with calling rust binary.

## 0.23.0-0.23.2 11-Mar-2025

* Upgraded to use Python 3.12.9
* Added new watcher, queue and worker to handle 2 stage writing of packet stats. Packet stats will get written locally and then in a seperate thread will be moved to vulcan NFS share.
* Added new config file option `log_level` to set logging level. Values are Python logging constants. E.g. DEBUG, INFO, WARNING, ERROR. If not specified, defaults to DEBUG.
* Changed sleep behaviour in subfile_distributor

## 0.22.0 07-Mar-2025

* Upgraded calvin_downloader to use giant-squid 2.x

## 0.21.10 26-Feb-2025

* Added request_checksum_calculation and response_checksum_validation to resolve upload error in versions of Boto3 gt 1.36

## 0.21.5 - 0.21.9  14-Feb-2025

* Merged in ensuring CALIBSRC="SUN" are not treated as calibrators (and not sent to calvins)
* Fixes to numactl usage
* Fixes to display of seconds to round off to 3dp

## 0.21.4  12-Feb-2025

* Implemented copy_subfile_to_disk_dd function.

## 0.21.3  07-Feb-2025

* Fixed Calvin timezone issue.

## 0.21.2 07-Jan-2025

* More logging, plus optimisation to summarise_packet_stats to remove loop and looped function call.

## 0.21.1 07-Jan-2025

* Added more debug to summarise_packet_stats.

## 0.21.0 07-Jan-2025

* Added logging to debug slow VCS when enabling packet stats.

## 0.20.6 05-Dec-2024

* For mwax_subfile_distributor, if a calibrator has "SUN" as the calibrator source, do not treat it as a calibrator.

## 0.20.5 04-Dec-2024

* If packet_stats_dump_dir config value is blank, then do not write packet stats.

## 0.20.4 04-Dec-2024

* Fixed bug where packet stats filename incorrectly included a space.

## 0.20.0 29-Nov-2024

* Added packet map extraction for M&C to mwax_subfile_processor
  * NOTE: changed correlator config file item "mwax mover"->"packet_stats_dump_dir"
* Added support for location=4 (acacia_mwa)
  * NOTE: new mwacache config file section "acacia_mwa"
  * NOTE: changed mwacache config file section: "acacia" is now "acacia_ingest"

## 0.19.5 20-Nov-2024

* Testing: Added more unit tests for mwax_db module.

## 0.19.4 19-Nov-2024

* Minor bug fix: Fixed SQL error in insert_data_file_row- missing deleted column

## 0.19.3 19-Nov-2024

* Minor bug fix: Fixed SQL error in insert_data_file_row

## 0.19.2 19-Nov-2024

* Minor bug fix: Fixed get_data_file_row method, added some more tests

## 0.19.1 19-Nov-2024

* Minor bug fix: Fixed bug where population of hostname was being done after it was needed

## 0.19.0 19-Nov-2024

* First release after merging `calvin_changes3` branch.
* Start of CHANGELOG
