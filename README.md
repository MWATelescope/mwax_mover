# mwax-sub2db
A command line tool which is part of the mwax correlator for the
MWA. 

In CORRELATOR mode: Watches a directory and file pattern (for sub files). On new file, launches diskdb to add the file to the designated PSRDADA
ringbuffer. 

In OFFLINE CORRELATOR mode: Retrieves a file list from a directory and file pattern. For each file, launches diskdb to add the file to the
designated PSRDADA ringbuffer. 

In VOLTAGE_CAPTURE mode: Watches a directory and file pattern. On new file, copy the sub file to local disk, where the
archiver daemon will send to Pawsey/NGAS. *.sub files are sub-observations of
8 seconds containing high time res data.