# mwax-mover

A command line tool which is part of the MWAX correlator for the MWA.

Three executable python scripts:

* **mwax_mover** - this is a simple command line tool to watch a directory and execute an arbirary command.
* **mwax_subfile_distributor** - this is an essential part of the MWAX correlator and beamformer which is responsible for
sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities.
Output from the beamformer gets sent to another host running FREDDA (FRB detection pipeline). FREDDA can then signal
this process to dump subfiles to disk if a detection is made. It also facilitates the `voltage buffer dump` mechanism which will be called via webservice from the M&C system.
* **mwacache_archiver_processor** - this runs on the mwacache servers at Curtin. It monitors for new files sent from MWAX servers
and then sends them to Pawsey's Long Term Storage and updates the MWA metadata db to confirm they were archived.
* **mwax_calvin_processor** - this runs on the Calvin1 server at the MRO. MWAX servers send any FITS files from calibrator observations
directly to Calvin1 into an 'incoming' directory. The mwax_calvin_processor monitors that directory and starts assembling all the files from the same obs_id into an 'assembly' directory. Once all the files for an observation arrive, Birli is run to flag the data and output a UVFITS file. Then Hyperdrive is run on the one or more UVFITS file outputs (one per contiguous band) to produce one or more calibration solutions files. The solution(s) are then analysed and used to create data which then gets inserted into the MWA database at the MRO. The secondary function of the mwax_calvin_processor is to detect any calibration_requests from the MWA database, download the data from the MWA ASVO and extract the files into the 'incoming' directory. From there the processing of the observation is the same as when it comes direct from MWAX, except that there is an extra step to update the database to mark that calibration request as being completed.

## Installing

Instructions:

```bash
# Clone the repository
git clone https://this_repository_url mwax_mover
cd mwax_mover

# Create a virtual environment (Python 3.8.3)
virtualenv -p /usr/bin/python3.8.3 env

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

## mwacache_archiver

### Running

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

## mwax_calvin_processor

### Running

```bash
./mwax_calvin_processor --cfg path_to_cfg/config.cfg
```

### Interacting via Web Services

```bash
# Example call:
http://host:port/command[?param1&param2]
```

Web service commands:

* /status
  * Reports status of all processes in JSON format

## mwax_subfile_distrubutor

### Running

```bash
./mwax_subfile_distributor --cfg path_to_cfg/config.cfg
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
  VOLTAGE_START observation is running, if in CORRELATOR mode)  
* /resume_archiving
  * Resuming archiving processes. (This is called automatically once the correlator is no longer running in
  VOLTAGE_START mode)
* /dump_voltages?start=X&end=X
  * This will pause archiving and rename all *.free subfiles to*.keep, then write the .keep files to disk. Once
  written successfully, all *.keep files are renamed back to*.free so mwax_udpgrab can continue to use them. This
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

#### Archiving Data To mwacache servers

mwax_subfile_distributor can be run in either of two modes:

1. **Correlator mode** In this mode it will:
    1. Watch the visdata/incoming and voltdata_incoming directories for new metafits, fits (MWAX visibilities) and subobservation files.
    2. When a new file appears in the visdata/incoming dir, call checksum_and_db_handler which calclates a checksum of the file and then inserts a new row into the mwa.data_files table to indicate a new file was generated (however the remote_archived flag is false until it gets to Pawsey).
    3. If the file is an MWAX visibility file, it then gets moved to the visdata/processing_stats dir.
       If the file is a subfile or metafits file, it gets moved to the visdata / voltdata outgoing dir (jump to step 7)
    4. When a new MWAX visibility file appears in the visdata/processing_stats dir, run [mwax_stats](https://github.com/MWATelescope/mwax_stats) to write out PPD and fringe data for calibrator observations to a /vulcan NFS share (which MWA M&C uses to produce plots when testing).
    5. Any files are then moved on to the visdata/cal_outgoing dir.
    6. When a new MWAX visibility file appears in visdata/cal_outgoing dir, check the metafits file to see if it is a calibrator. If so send it to one of the calvin servers via xrootd. Either way, now move the file into the visdata outgoing dir.
    7. For all files that appear in the visdata / voltdata outgoing dirs, xrootd archive them to one of the mwacache servers. From there mwacache_archiver process will send them to Pawsey for long term storage using Pawsey's S3 interface to Acacia.

    The above can be summarised with the following flows:
    * MWAX Visibilities:
      * Incoming -> Checksum & insert into DB -> Produce Stats -> Outgoing send to calvin -> Outgoing send to mwacache
    * MWAX Subfiles:
      * Incoming -> Checksum & insert into DB -> Outgoing send to mwacache
    * Metafits files:
      * Incoming -> Checksum & insert into DB -> Outgoing send to mwacache

2. **Beamformer mode** In this mode it will:
    1. This process also watches the output directory where filterbank files are written and whenever there is a new
       fil file, it will transfer the data via bbcp to a host  which is running FREDDA.
    2. If FREDDA makes a detection, then FREDDA will call the /dump_voltages web service which will dump out the subfiles
       to disk for later analysis. (See /dump_voltages for more info).

## testing mwax_calvin on calvin1

```
# change into parent dir of mwax_mover
cd ..

# setup sourcelists
git clone https://github.com/JLBLine/srclists.git

# setup rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# setup cargo
echo "source \${HOME}/.cargo/env" >> ~/.bashrc

# setup nvcc
echo "export PATH=\"\${PATH}:/usr/local/cuda/bin/\" >> ~/.bashrc
echo "export INCLUDES=\${INCLUDES} -I/usr/local/cuda/include\" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=\"\${LD_LIBRARY_PATH}:/usr/local/cuda/lib/\" >> ~/.bashrc

# setup hyperdrive
git clone https://github.com/MWATelescope/mwa_hyperdrive.git
cd mwa_hyperdrive
cargo install --path . --features=cuda-single
# --features=gpu-single,cuda on hyperdrive >=0.3
cd ..

# setup Birli
git clone https://github.com/MWATelescope/Birli.git
cd Birli
cargo build --release
cargo install --path .
cd ..

# setup beam
mkdir beam
cd beam
wget http://ws.mwatelescope.org/static/mwa_full_embedded_element_pattern.h5
echo "export MWA_BEAM_FILE=${PWD}/mwa_full_embedded_element_pattern.h5" >> ~/.bashrc
cd ..

# setup mwax_stats
# TODO

# setup locale for casa / perl
echo "export LC_ALL=en_US.UTF-8" >> ~/.bashrc
echo "export LANG=en_US.UTF-8" >> ~/.bashrc

# run tests
cd mwax_mover
pytest
```
