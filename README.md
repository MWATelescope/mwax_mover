# mwax-mover
A command line tool which is part of the MWAX correlator for the MWA. 

Two executable python scripts:
* **mwax_mover** - this is a simple command line tool to watch a directory and execute an arbirary command.
* **mwax_subfile_distributor** - this is an essential part of the MWAX correlator and beamformer which is responsible for 
sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities.
Output from the beamformer gets sent to another host running FREDDA (FRB detection pipeline). FREDDA can then signal
this process to dump subfiles to disk if a detection is made.

### Installing
Instructions:
```
# Clone the repository
git clone https://this_repository_url mwax_mover
cd mwax_mover

# Create a virtual environment (Python 3.8.3)
virtualenv -p /usr/bin/python3.8.3 env

# Source the environment
source env/bin/activate

# Install prerequisities
pipenv install 

# Install
python setup.py install    
```

## mwax_mover
### Running
```
./mwax_mover.py [-h] -w WATCHDIR -x WATCHEXT -e EXECUTABLEPATH -m {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}
```

Parameters:
* -h, --help
  * show this help message and exit
* -w WATCHDIR, --watchdir WATCHDIR
  * Directory to watch for files with watchext extension
* -x WATCHEXT, --watchext WATCHEXT
  * Extension to watch for e.g. .sub
* -e EXECUTABLEPATH, --executablepath EXECUTABLEPATH
  * Absolute path to executable to launch. __FILE__ will be substituted with the abs path of the filename being 
    processed.__FILENOEXT__ will be replaced with the filename but not extenson.
* -m {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}, --mode {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}
  * Mode to run: 
    * WATCH_DIR_FOR_NEW: Watch watchdir for new files forever. Launch executable.
    * WATCH_DIR_FOR_RENAME: Watch watchdir for renamed files forever. Launch executable.
    * PROCESS_DIR: For each file in watchdir, launch executable. Exit.

## mwax_subfile_distrubutor
### Running
```
./mwax_subfile_distributor --cfg path_to_cfg/config.cfg
```

### Interacting via Web Services
```
# Example call:
http://host:port/command[?param1&param2]
```
Web service commands:
* /status
  * Reports status of all processes in JSON format
* /pause_archiving
  * Pauses all archiving processes in order to reduce disk contention. (This is called automaticallly whenever a 
  VOLTAGE_START observation is running, if in CORRELATOR mode)  
* /resume_archiving
  * Resuming archiving processes. (This is called automatically once the correlator is no longer running in 
  VOLTAGE_START mode)
* /dump_voltages?start=X&end=X
  * This will pause archiving and rename all *.free subfiles to *.keep, then write the .keep files to disk. Once 
  written successfully, all *.keep files are renamed back to *.free so mwax_udpgrab can continue to use them. This 
  webservice call is generally trigged by something like FREDDA if a transient detection occurs. 

### mwax_subfile_distributor has two distinct jobs
#### Process Incomming Subfiles
mwax_subfile_distributor can be run in either of two modes:
1. **Correlator mode** In this mode it will:    
   1. Watch the directory where mwax_udpgrab writes new subfiles and either:     
      1. If the subfile is for an observation in HW_LFILES mode (Correlator), load into a PSRDADA ringbuffer
         via dada_diskdb.
      2. If the subfile is for an observation in VOLTAGE_START mode (Voltage Capture), then copy the file to a voltage 
         data directory.
      3. Either way, once done, rename the subfile *.free so it can be reused.
   2. The mwax_correlator will then process any data from the PSRDADA ringbuffer, pass it on to dada_dbfits to write 
      out MWAX correlator visibilities to a visibility output directory.     
2. **Beamformer mode** In this mode it will:
   1. Watch the directory where mwax_udpgrab writes new subfiles.
   2. When a new .sub file appears, execute dada_diskdb to load the subfile into a PSRDADA ringbuffer.
   3. The subfile is then renamed to *.free to indicate to mwax_udpgrab that this file can be reused (there is a 
      performance gain to reusing existing files). 
   4. The mwax_beamformer will then beamform the data in the ringbuffer, pass the data to dada_dbfil to write out 
      filterbank files.

#### Archiving Data To Pawsey
mwax_subfile_distributor can be run in either of two modes:
1. **Correlator mode** In this mode it will:
    1. Watch the visibility output directory for new fits files (raw visibilities generated by the mwax_correlator).
        1. Insert a new row into the mwa.data_files table to indicate a new file was generated (however the 
        remote_archived flag is false until it gets to Pawsey). 
        2. Then bbcp archive them to one of the mwacache servers running NGAS. From there NGAS will send them to Pawsey 
        for long term storage.
    2. Watch the voltage output directory for new subfiles. 
        1. Insert a new row into the mwa.data_files table to indicate a new file was generated (however the 
        remote_archived flag is false until it gets to Pawsey).
        2. Bbcp archive them to one of the mwacache servers running NGAS. From there NGAS will send them to Pawsey for 
        long term storage.

2. **Beamformer mode** In this mode it will:
    1. This process also watches the output directory where filterbank files are written and whenever there is a new 
       fil file, it will transfer the data via bbcp to a host  which is running FREDDA.
    2. If FREDDA makes a detection, then FREDDA will call the /dump_voltages web service which will dump out the subfiles 
       to disk for later analysis. (See /dump_voltages for more info).
