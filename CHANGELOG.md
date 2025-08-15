# Changelog

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
