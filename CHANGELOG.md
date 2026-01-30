# Changelog

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
