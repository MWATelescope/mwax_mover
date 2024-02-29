# mwax-mover

A suite of command line tools which are part of the MWAX correlator for the MWA.

Three executable python scripts:

* **mwax_mover** - this is a simple command line tool to watch a directory and execute an arbirary command.
* **mwax_subfile_distributor** - this is an essential part of the MWAX correlator and beamformer which is responsible for
sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities to
the mwacache servers.
* **mwacache_archiver** - this runs on the mwacache servers at Curtin. It monitors for new files sent from MWAX servers
and then sends them to Pawsey's Long Term Storage and updates the MWA metadata db to confirm they were archived.

## Installing

Instructions:

```bash
# Clone the repository
git clone https://this_repository_url mwax_mover
cd mwax_mover

# Create a virtual environment (Python 3.11.6)
virtualenv -p /usr/bin/python3.11.6 env

# Source the environment
source env/bin/activate

# Install
pip install .
```

## mwax_mover

### Running

```bash
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
  * Absolute path to executable to launch. **FILE** will be substituted with the abs path of the filename being
    processed.**FILENOEXT** will be replaced with the filename but not extenson.
* -m {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}, --mode {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}
  * Mode to run:
    * WATCH_DIR_FOR_NEW: Watch watchdir for new files forever. Launch executable.
    * WATCH_DIR_FOR_RENAME: Watch watchdir for renamed files forever. Launch executable.
    * PROCESS_DIR: For each file in watchdir, launch executable. Exit.

## mwax_subfile_distrubutor

### Running mwax_subfile_distrubutor

```bash
./mwax_subfile_distributor --cfg path_to_cfg/config.cfg
```

## mwacache_archiver

### Running mwacache_archiver

```bash
./mwacache_archiver --cfg path_to_cfg/config.cfg
```

### Interacting via Web Services

```bash
# Example call:
http://host:port/command[?param1&param2]
```

Web service commands:

* /status
  * Reports status of all processes in JSON format
* /pause_archiving
  * Pauses all archiving processes in order to reduce disk contention. (This is called automaticallly whenever a
  MWAX_VCS observation is running, if in CORRELATOR mode)  
* /resume_archiving
  * Resuming archiving processes. (This is called automatically once the correlator is no longer running in
  MWAX_VCS mode)
* /dump_voltages?start=X&end=X&trigger_id=X
  * This will pause archiving and rename all *.free subfiles to*.keep, add the trigger_id to the subfile header,
  then write the .keep files to disk. Once written successfully, all *.keep files are renamed back to*.free so
  mwax_u2s can continue to use them. This webservice call is generally trigged by the M&C system.
