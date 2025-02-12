# Changelog

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
