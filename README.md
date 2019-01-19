# mwax-mover
A command line tool which is part of the mwax correlator for the MWA. 

* In WATCH_DIR_FOR_NEW mode: Watches a directory and file pattern. On new file, launches a command. Runs until killed.
* In WATCH_DIR_FOR_RENAME mode: Watches a directory and file pattern. On rename of file, launches a command. Runs until killed.
* In PROCESS_DIR mode: Retrieves a file list from a directory and file pattern. For each file, launches a command. Exits once done. 