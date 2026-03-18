# mwax-mover

A suite of command line tools which are part of the MWAX correlator for the MWA.

Three executable python scripts:

* **mwax_mover** - this is a simple command line tool to watch a directory and execute an arbirary command.
* **mwax_subfile_distributor** - this is an essential part of the MWAX correlator and beamformer which is responsible for
sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities to the mwacache servers.
Output from the beamformer gets sent to another host running FREDDA (FRB detection pipeline). FREDDA can then signal
this process to dump subfiles to disk if a detection is made.
* **mwacache_archiver** - this runs on the mwacache servers at Curtin. It monitors for new files sent from MWAX servers
and then sends them to Pawsey's Long Term Storage and updates the MWA metadata db to confirm they were archived.
* **mwax_calvin_controller** - this runs on the Calvin SLURM cluster at the MRO. MWAX servers keep any FITS files from calibrator observations
in a 'cal_outgoing' directory. The mwax_calvin_controller detects a new calibration is required and then submits a SLURM job to the cluster which runs
the mwax_calvin_processor. 
* **mwax_calvin_processor** - The mwax_calvin_processor runs on a calvin host when SLURM commands it, copies the calibrator visibility files from all of the MWAX hosts,
then performs calibration, uploading the solution to the MWA database. Once completed, the calvin host calls "release_cal_obs" on each of the MWAX host's web service endpoints 
which then tells calvin to either move the files to the vis_outgoing dir for archiving or to the dont_archive directory.

## Installing

Instructions:

```bash
# Clone the repository
$ git clone https://this_repository_url mwax_mover
$ cd mwax_mover

# Create a virtual environment (Python 3.11.6)
$ uv sync

# Source the environment
$ source .venv/bin/activate

# Now run a command line too e.g.
$ mwax_subfile_distributor --help
```

## mwax_mover command line tool

### Running mwax_mover command line tool

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
usage: mwax_subfile_distributor [-h] -c CFG --mode {c,b,C,B}

mwax_subfile_distributor: a command line tool which is part of the mwax suite for the MWA. It will perform different tasks based on the configuration file. 
In addition, it will automatically archive files in /voltdata and /visdata to the mwacache servers at the Curtin Data Centre.

options:
  -h, --help         show this help message and exit
  -c CFG, --cfg CFG  Configuration file location.
  --mode {c,b,C,B}   Mode of operation: C (correlator) or B (beamformer)
```


### Interacting with mwax_subfile_distrubutor via Web Services

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
* /release_cal_obs?obs_id=X
  * This will be called by a calvin server when it has finished calibration of an obs_id. It triggers the MWAX
    server to release the visibility file(s) for that obs_id so they can be archived.


### mwax_subfile_distributor Health Packet Format
```json
{
  "main": {
    "unix_imestamp": 1773726592.618505,
    "process": "MWAXSubfileDistributor",
    "version": "1.5.3",
    "host": "mwax99",
    "running": true,
    "mode": "C",
    "archiving": true,
    "cmdline": "--cfg=/path/to/config.cfg"
  },
  "workers": [
    {
      "name": "SubfileIncomingProcessor",
      "watchers": [
        {
          "name": "SubfileIncomingProcessor_dev_shm_mwax",
          "watch_path": "/dev/shm/mwax"
        }
      ],
      "queue_worker": {
        "name": "SubfileIncomingProcessor_worker",
        "current_item": "/dev/shm/mwax/1234567890_1234567890_123.sub",
        "queue_size": 1
      }
    },
    {
      "name": "PacketStatsProcessor",
      "watchers": [
        {
          "name": "PacketStatsProcessor_vulcan_packet_stats_dump",
          "watch_path": "/vulcan/packet_stats_dump"
        }
      ],
      "queue_worker": {
        "name": "PacketStatsProcessor_worker",
        "current_item": null,
        "queue_size": 0
      }
    },
    {
      "name": "ChecksumAndDBProcessor",
      "watchers": [
        {
          "name": "ChecksumAndDBProcessor_visdata_incoming",
          "watch_path": "/visdata/incoming"
        },
        {
          "name": "ChecksumAndDBProcessor_voltdata_incoming",
          "watch_path": "/voltdata/incoming"
        },
        {
          "name": "ChecksumAndDBProcessor_voltdata_bf_stitching",
          "watch_path": "/voltdata/bf/stitching"
        }
      ],
      "queue_worker": {
        "name": "ChecksumAndDBProcessor_worker",
        "current_item": null,
        "queue_size": 0
      }
    },
    {
      "name": "VisStatsProcessing",
      "watchers": [
        {
          "name": "VisStatsProcessing_visdata_processing_stats",
          "watch_path": "/visdata/processing_stats"
        }
      ],
      "queue_worker": {
        "name": "VisStatsProcessing_worker",
        "current_item": null,
        "queue_size": 0
      }
    },
    {
      "name": "BfStitchingProcessor",
      "watchers": [
        {
          "name": "BfStitchingProcessor_voltdata_bf_incoming",
          "watch_path": "/voltdata/bf/incoming"
        }
      ],
      "queue_worker": {
        "name": "BfStitchingProcessor_worker",
        "current_item": null,
        "queue_size": 0
      }
    },
    {
      "name": "VisSCalOutgoingProcessor",
      "watchers": [
        {
          "name": "VisSCalOutgoingProcessor_visdata_cal_outgoing",
          "watch_path": "/visdata/cal_outgoing"
        }
      ],
      "queue_worker": {
        "name": "VisSCalOutgoingProcessor_worker",
        "current_item": null,
        "queue_size": 0
      }
    },
    {
      "name": "OutgoingProcessor",
      "watchers": [
        {
          "name": "OutgoingProcessor_visdata_outgoing",
          "watch_path": "/visdata/outgoing"
        },
        {
          "name": "OutgoingProcessor_voltdata_outgoing",
          "watch_path": "/voltdata/outgoing"
        },
        {
          "name": "OutgoingProcessor_voltdata_bf_outgoing",
          "watch_path": "/voltdata/bf/outgoing"
        }
      ],
      "queue_worker": {
        "name": "OutgoingProcessor_worker",
        "current_item": null,
        "queue_size": 0
      }
    }
  ]
}
```

## mwacache_archiver

### Running mwacache_archiver

```bash
usage: mwacache_archiver [-h] -c CFG

mwacache_archive_processor: a command line tool which is part of the MWA correlator for the MWA. It will monitor various directories on each mwacache server and, upon detecting a file, send it to Pawsey's LTS. It will then remove the file from the
local disk.

options:
  -h, --help         show this help message and exit
  -c CFG, --cfg CFG  Configuration file location.
```

### mwacache_archiver Health Packet Format
```json

{
  "main": {
    "unix_timestamp": 1773725279.8661675,
    "process": "MWACacheArchiveProcessor",
    "version": "1.5.3",
    "host": "mwacache99",
    "running": true,
    "cmdline": "--cfg /path/to/config.cfg"
  },
  "workers": [
    {
      "name": "PawseyOutgoingProcessor1",
      "watchers": [
        {
          "name": "PawseyOutgoingProcessor1_volume1_incoming",
          "watch_path": "/volume1/incoming"
        }
      ],
      "queue_worker": {
        "name": "PawseyOutgoingProcessor_worker",
        "current_item": "/volume1/incoming/1234567890_20260317090000_109_000.fits",
        "queue_size": 11
      }
    },
    {
      "name": "PawseyOutgoingProcessor2",
      "watchers": [        
        {
          "name": "PawseyOutgoingProcessor2_volume2_incoming",
          "watch_path": "/volume2/incoming"
        },        
      ],
      "queue_worker": {
        "name": "PawseyOutgoingProcessor2_worker",
        "current_item": "/volume2/incoming/1234567890_20260317090000_111_000.fits",
        "queue_size": 9
      }
    },
    {
      "name": "PawseyOutgoingProcessor3",
      "watchers": [        
        {
          "name": "PawseyOutgoingProcessor3_volume3_incoming",
          "watch_path": "/volume3/incoming"
        },
      ],
      "queue_worker": {
        "name": "PawseyOutgoingProcessor3_worker",
        "current_item": "/volume3/incoming/1234567890_20260317090000_120_000.fits",
        "queue_size": 10
      }
    }
  ]
}
```

## mwax_calvin_controller

### Running mwax_calvin_controller

```bash
usage: mwax_calvin_controller [-h] -c CFG

mwax_calvin_controller: a command line tool which is part of the MWA correlator for the MWA. It will submit SBATCH jobs as needed to process real time or MWA ASVO calibration jobs.

options:
  -h, --help         show this help message and exit
  -c CFG, --cfg CFG  Configuration file location.
```

### mwax_calvin_controller Health Packet Format
```json
{
  "main": {
    "unix_timestamp": 1773790384.4741197,
    "process": "MWAXCalvinController",
    "version": "1.5.3",
    "host": "hobbes",
    "running": true,
    "realtime_slurm_jobs_submitted": 1,
    "mwa_asvo_slurm_jobs_submitted": 2,
    "giant_squid_errors": 3,
    "mwa_asvo_errors": 4,
    "database_errors": 5,
    "slurm_errors": 6
  }
}
```

## mwax_calvin_processor

### Running mwax_calvin_processor

```bash
usage: mwax_calvin_processor [-h] -c CFG -o OBS_ID -s SLURM_JOB_ID -r REQUEST_IDS -j {CalvinJobType.realtime,CalvinJobType.mwa_asvo} [-u MWA_ASVO_DOWNLOAD_URL]

A command line tool which is part of the MWA correlator for the MWA. It will be launched via a SLURM job and either download a realtime calibrator obs from MWAX or download data from an MWA ASVO URL. Either way it will then run Birli and Hyperdrive
and then upload the calibration solution.

options:
  -h, --help            show this help message and exit
  -c CFG, --cfg CFG     Configuration file location.
  -o OBS_ID, --obs-id OBS_ID
                        ObservationID.
  -s SLURM_JOB_ID, --slurm-job-id SLURM_JOB_ID
                        This Slurm Job ID.
  -r REQUEST_IDS, --request-ids REQUEST_IDS
                        A comma separated list of one or more request ids.
  -j {CalvinJobType.realtime,CalvinJobType.mwa_asvo}, --job-type {CalvinJobType.realtime,CalvinJobType.mwa_asvo}
                        MWA ASVO or Realtime job.
  -u MWA_ASVO_DOWNLOAD_URL, --mwa-asvo-download-url MWA_ASVO_DOWNLOAD_URL
                        For MWA ASVO processing- the download URL for the MWA ASVO job.
```

### mwax_calvin_processor Health Packet Format

```json
{
  "main": {
    "unix_timestamp": 1773790384.4741197,
    "process": "MWAXCalvinProcessor",
    "version": "1.5.3",
    "host": "calvin99",
    "running": true,
    "slurm_job_id": 123,
    "obs_id": 1234567890,
    "job_type": "realtime",
    "task": "Birli",
    "requests": "123,456",
  }
}
```