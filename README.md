# mwax-mover

A suite of command line tools which are part of the MWAX correlator for the MWA.

Three executable python scripts:

* **mwax_mover** - this is a simple command line tool to watch a directory and execute an arbirary command.
* **mwax_subfile_distributor** - this is an essential part of the MWAX correlator and beamformer which is responsible for
sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities to the mwacache servers..
Output from the beamformer gets sent to another host running FREDDA (FRB detection pipeline). FREDDA can then signal
this process to dump subfiles to disk if a detection is made.
* **mwacache_archiver** - this runs on the mwacache servers at Curtin. It monitors for new files sent from MWAX servers
and then sends them to Pawsey's Long Term Storage and updates the MWA metadata db to confirm they were archived.
* **mwax_calvin_processor** - this runs on the Calvin1 server at the MRO. MWAX servers send any FITS files from calibrator observations
directly to Calvin1 into an 'incoming' directory. The mwax_calvin_processor monitors that directory and starts assembling all the files from the same obs_id into an 'assembly' directory. Once all the files for an observation arrive, Birli is run to flag the data and output a UVFITS file. Then Hyperdrive is run on the one or more UVFITS file outputs (one per contiguous band) to produce one or more calibration solutions files. The solution(s) are then analysed and used to create data which then gets inserted into the MWA database at the MRO. The secondary function of the mwax_calvin_processor is to detect any calibration_requests from the MWA database, download the data from the MWA ASVO and extract the files into the 'incoming' directory. From there the processing of the observation is the same as when it comes direct from MWAX, except that there is an extra step to update the database to mark that calibration request as being completed.

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
  MWAX_VCS observation is running, if in CORRELATOR mode)
* /resume_archiving
  * Resuming archiving processes. (This is called automatically once the correlator is no longer running in
  MWAX_VCS mode)
* /dump_voltages?start=X&end=X&trigger_id=X
  * This will pause archiving and rename all *.free subfiles to*.keep, add the trigger_id to the subfile header,
  then write the .keep files to disk. Once written successfully, all *.keep files are renamed back to*.free so
  mwax_u2s can continue to use them. This webservice call is generally trigged by the M&C system.

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
cargo install --path . --features=cuda,gpu-single
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
