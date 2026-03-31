# mwax-mover

A suite of command line tools which are part of the MWAX correlator for the MWA.

The `mwax_mover` suite manages data ingestion, distribution, archiving, and near-realtime calibration for the **MWAX correlator** — part of the **Murchison Widefield Array (MWA)** radio telescope at the Murchison Radio-observatory (MRO) in Western Australia.

Five executable command line tools:

* **mwax_mover** - a simple command line tool to watch a directory and execute an arbitrary command on each new file.
* **mwax_subfile_distributor** - the main real-time data handling engine of the MWAX correlator and beamformer. Responsible for sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities to the mwacache servers. Output from the beamformer gets sent to another host running FREDDA (FRB detection pipeline). FREDDA can then signal this process to dump subfiles to disk if a detection is made.
* **mwacache_archiver** - runs on the mwacache servers at Curtin. Monitors for new files sent from MWAX servers and then sends them to Pawsey's Long Term Storage and updates the MWA metadata db to confirm they were archived.
* **mwax_calvin_controller** - runs on the Calvin SLURM cluster at the MRO. MWAX servers keep any FITS files from calibrator observations in a `cal_outgoing` directory. The mwax_calvin_controller detects a new calibration is required and then submits a SLURM job to the cluster which runs the mwax_calvin_processor.
* **mwax_calvin_processor** - runs on a Calvin HPC node when SLURM commands it, copies the calibrator visibility files from all of the MWAX hosts, then performs calibration, uploading the solution to the MWA database. Once completed, the calvin host calls `release_cal_obs` on each of the MWAX host's web service endpoints which then tells calvin to either move the files to the vis_outgoing dir for archiving or to the dont_archive directory.

## Installing

Instructions:

```bash
# Clone the repository
$ git clone https://this_repository_url mwax_mover
$ cd mwax_mover

# Create a virtual environment (Python 3.12)
$ uv sync

# Source the environment
$ source .venv/bin/activate

# Now run a command line tool e.g.
$ mwax_subfile_distributor --help
```

---

## Architecture Overview

Every major processor in `mwax_mover` uses a common **Watch → Queue → Worker** pipeline:

1. One or more **Watcher** threads use Linux `inotify` to monitor directories for new or renamed files.
2. On startup, watchers also perform a one-shot scan to enqueue any pre-existing files before entering the live event loop.
3. File paths are deposited into either a plain `Queue` or a `PriorityQueue` (where high-priority MWA project IDs are processed first).
4. A **QueueWorker** thread dequeues items and calls a `handler()` function per file, with configurable backoff and retry behaviour on failure.
5. The abstract `MWAXWatchQueueWorker` and `MWAXPriorityWatchQueueWorker` base classes compose watcher(s) and worker into a single manageable unit — concrete processor classes implement only the `handler()` method.

All processors broadcast a JSON health status packet periodically via UDP multicast and handle `SIGINT`/`SIGTERM` for graceful shutdown.

---

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
    processed. **FILENOEXT** will be replaced with the filename but not extension.
* -m {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}, --mode {WATCH_DIR_FOR_NEW,WATCH_DIR_FOR_RENAME,PROCESS_DIR}
  * Mode to run:
    * WATCH_DIR_FOR_NEW: Watch watchdir for new files forever. Launch executable.
    * WATCH_DIR_FOR_RENAME: Watch watchdir for renamed files forever. Launch executable.
    * PROCESS_DIR: For each file in watchdir, launch executable. Exit.

### How it works

```
parse args: -w <watchdir> -x <watchext> -e <executable> -m <mode>

create QueueWorker with executable command template
create Watcher on <watchdir> for files matching <watchext>

Watcher detects file → enqueues path
QueueWorker dequeues path → substitutes __FILE__ / __FILENOEXT__ into command → runs command
```

---

## mwax_subfile_distributor

### Running mwax_subfile_distributor

```bash
usage: mwax_subfile_distributor [-h] -c CFG --mode {c,b,C,B}

mwax_subfile_distributor: a command line tool which is part of the mwax suite for the MWA. It will perform different tasks based on the configuration file.
In addition, it will automatically archive files in /voltdata and /visdata to the mwacache servers at the Curtin Data Centre.

options:
  -h, --help         show this help message and exit
  -c CFG, --cfg CFG  Configuration file location.
  --mode {c,b,C,B}   Mode of operation: C (correlator) or B (beamformer)
```

### How it works

On startup, `MWAXSubfileDistributor` reads its config file, connects to the MRO metadata database, starts a Flask web server for health/control endpoints, and launches all worker processors.

```
initialise_from_command_line()
  └─ parse -c <config> --mode {C|B}
  └─ initialise(config)
       ├─ read config (mode, paths, DB, Redis, multicast, etc.)
       ├─ connect to MRO metadata DB
       ├─ set up Flask web server (health/control endpoints)
       └─ create workers:
            ├─ SubfileIncomingProcessor  (watches raw subfile incoming dir)
            ├─ ChecksumAndDBProcessor    (watches vis/volt/bf incoming dirs)
            ├─ BfStitchingProcessor      (watches bf incoming dir for stitching)
            ├─ VisStatsProcessor         (watches vis processing/stats dir)
            ├─ VisCalOutgoingProcessor   (watches vis cal outgoing dir)
            └─ OutgoingProcessor         (watches vis/volt/bf outgoing dirs)

start()
  ├─ start DB pool
  ├─ start Flask web server thread
  ├─ start health multicast thread (UDP, every 1s)
  ├─ start all workers
  └─ main loop: monitor worker health, handle cal obs release requests
        └─ release_cal_obs(): moves cal files from outgoing_cal → outgoing (archive) or dont_archive
```

**Data flow — CORRELATOR mode:**

```
.sub subfile arrives → SubfileIncomingProcessor
  → loads into PSRDADA ring buffer (→ external correlator process produces .fits)
  → .fits arrives → ChecksumAndDBProcessor
      → MD5 + DB insert
      → if calibrator → visdata_processing_stats → VisStatsProcessor
                           → mwax_stats (statistics)
                           → visdata_outgoing_cal → VisCalOutgoingProcessor (adds to cal list for calvin)
      → if not calibrator → visdata_processing_stats → VisStatsProcessor
                                → mwax_stats
                                → visdata_outgoing → OutgoingProcessor
                                    → xrootd → mwacache server
                                    → delete local file
```

**Data flow — VCS / voltage dump mode:**

```
.sub subfile → SubfileIncomingProcessor
  → if voltage dump active: inject TRIGGER_ID, copy to voltdata_incoming
  → if VCS: copy to voltdata_incoming
  → ChecksumAndDBProcessor → MD5 + DB → voltdata_outgoing → OutgoingProcessor → xrootd → mwacache
```

**Data flow — BEAMFORMER mode:**

```
.sub subfile → SubfileIncomingProcessor
  → signal beamformer via Redis (→ external beamformer produces .vdif or .fil subobs files)
  → subobs files arrive → BfStitchingProcessor
      → waits for final subobs of observation
      → stitches all subobs → single .vdif/.fil output
      → ChecksumAndDBProcessor → MD5 + DB → bf_outgoing → OutgoingProcessor → xrootd → mwacache
```

### Interacting with mwax_subfile_distributor via Web Services

```bash
# Example call:
http://host:port/command[?param1&param2]
```

Web service commands:

* /status
  * Reports status of all processes in JSON format
* /pause_archiving
  * Pauses all archiving processes in order to reduce disk contention. (This is called automatically whenever a
  MWAX_VCS observation is running, if in CORRELATOR mode)
* /resume_archiving
  * Resuming archiving processes. (This is called automatically once the correlator is no longer running in
  MWAX_VCS mode)
* /dump_voltages?start=X&end=X&trigger_id=X
  * This will pause archiving and rename all *.free subfiles to *.keep, add the trigger_id to the subfile header,
  then write the .keep files to disk. Once written successfully, all *.keep files are renamed back to *.free so
  mwax_u2s can continue to use them. This webservice call is generally triggered by the M&C system.
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

---

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

### How it works

`MWACacheArchiveProcessor` connects to both the MRO metadata database (read/write) and a remote metadata database (read-only, used to verify expected file sizes and checksums). It creates one `PawseyOutgoingProcessor` worker per configured watch directory.

```
initialise_from_command_line()
  └─ parse -c <config>
  └─ initialise(config)
       ├─ read config (archive_to_location: Acacia/Banksia, S3 profile, ceph endpoints, watch dirs)
       ├─ connect to MRO metadata DB (read/write) and remote metadata DB (read-only)
       ├─ clean up stale .part* temp files older than 1 hour
       └─ create PawseyOutgoingProcessor per watch directory

start()
  ├─ start DB pools
  ├─ start health multicast thread
  ├─ start all PawseyOutgoingProcessor workers
  └─ main loop: monitor worker health

PawseyOutgoingProcessor.handler(file):
  ├─ validate filename
  ├─ stat file to get size on disk
  ├─ query remote DB for expected size and checksum
  ├─ if size 0 or mismatch → delete file and drop item
  ├─ compute MD5 and compare to DB value
  ├─ if mismatch → requeue
  ├─ determine S3 bucket name from obs_id
  ├─ rclone copyto → Acacia or Banksia (with rclone check verification, multiple endpoints)
  ├─ update MRO metadata DB (mark archived with location + bucket)
  └─ delete local file
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
        "name": "PawseyOutgoingProcessor1_worker",
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
        }
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
        }
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

---

## mwax_calvin_controller

### Running mwax_calvin_controller

```bash
usage: mwax_calvin_controller [-h] -c CFG

mwax_calvin_controller: a command line tool which is part of the MWA correlator for the MWA. It will submit SBATCH jobs as needed to process real time or MWA ASVO calibration jobs.

options:
  -h, --help         show this help message and exit
  -c CFG, --cfg CFG  Configuration file location.
```

### How it works

`MWAXCalvinController` polls the metadata database on a configurable interval, auto-creates calibration requests for unattempted calibrator observations, then dispatches SLURM jobs for both realtime and MWA ASVO calibration paths.

```
initialise_from_command_line()
  └─ parse -c <config>
  └─ initialise(config)
       ├─ read config (check_interval, script_path, oldest_cal_obs_id, giant-squid settings)
       ├─ connect to MRO metadata DB
       └─ initialise MWAASVOHelper (giant-squid binary path + timeouts)

start()
  ├─ start DB pool
  ├─ start health multicast thread
  └─ main loop (every check_interval_seconds):
       ├─ realtime_create_requests_for_unattempted_cal_obs()
       │    └─ query DB for calibrator obs with no calibration request → insert request rows
       ├─ get_new_calibration_requests()
       │    └─ query DB for unassigned requests → split into realtime list and mwa_asvo list
       ├─ for each realtime request:
       │    └─ realtime_submit_to_slurm()
       │         ├─ create_sbatch_script() (calls mwax_calvin_processor)
       │         ├─ submit_sbatch() → SLURM
       │         └─ update DB with slurm_job_id
       ├─ for each mwa_asvo request:
       │    └─ mwa_asvo_add_new_asvo_job()
       │         ├─ giant-squid submitvis → MWA ASVO (submit download job)
       │         └─ update DB with asvo job_id
       ├─ mwa_asvo_update_tracked_jobs()
       │    └─ giant-squid list → update in-memory job states
       └─ mwa_asvo_submit_ready_asvo_jobs_to_slurm()
            └─ for jobs in Ready state: create_sbatch_script() + submit_sbatch() → SLURM
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

---

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

### How it works

`MWAXCalvinProcessor` is launched by SLURM (via a script created by `mwax_calvin_controller`). It downloads observation data, preprocesses it with Birli, calibrates with hyperdrive, uploads the solution to the database, then signals MWAX hosts to release or discard the calibrator visibility files.

```
initialise_from_command_line()
  └─ parse -c <config> --obs-id --job-type [--request-ids] [--mwa-asvo-download-url]

start()
  └─ for each request_id:
       ├─ update DB: mark download started (assign hostname)
       │
       ├─ [if mwa_asvo]: download from MWA ASVO URL → local working dir (via Birli)
       │
       ├─ [if realtime]: rsync .fits files from all MWAX boxes → local working dir
       │
       ├─ update DB: mark download complete
       ├─ update DB: mark calibration started
       │
       ├─ run Birli (preprocessing + flagging → uvfits)
       ├─ run hyperdrive (calibration → solutions.fits)
       │
       ├─ process_solutions()
       │    ├─ load HyperfitsSolution + Metafits
       │    ├─ determine reference antenna
       │    ├─ fit phases and gains per coarse channel
       │    ├─ insert_calibration_fits_row() → DB
       │    └─ insert_calibration_solutions_row() → DB
       │
       ├─ update DB: mark calibration complete
       │
       ├─ [if realtime]: call /release_cal_obs on each MWAX host's Flask endpoint
       │    └─ MWAX moves cal .fits files to vis_outgoing (archive) or dont_archive
       │
       └─ clean up working directory
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
    "requests": "123,456"
  }
}
```

---

## Module Reference

A reference for all Python modules in `src/mwax_mover/`.

### Constants and Version

**`mwax_mover.py`**
Module-level constants only. Defines the `__FILE__` and `__FILENOEXT__` token strings used for command substitution, and the three watch-mode string constants (`WATCH_DIR_FOR_NEW`, `WATCH_DIR_FOR_RENAME`, `WATCH_DIR_FOR_RENAME_OR_NEW`).

**`version.py`**
Provides `get_mwax_mover_version_string()` and `get_pmwax_mover_version_number()`. Reads the installed package version via `importlib_metadata`.

### Database

**`mwax_db.py`** — `MWAXDBHandler`
Wraps a `psycopg` + `psycopg_pool` connection pool to a PostgreSQL database. Provides `select_one_row_postgres`, `select_many_rows_postgres`, `execute_single_dml_row`, and `execute_dml`, all decorated with `tenacity` retry logic for transient connection failures. Also contains all domain-specific query functions used by the rest of the codebase (e.g. `insert_data_file_row`, `get_unattempted_calsolution_requests`, `update_calibration_request_slurm_status`, etc.).

### Command Execution

**`mwax_command.py`**
Thin wrappers around `subprocess`:
- `run_command_ext()` — runs a command synchronously, optionally pinned to a NUMA node, returns `(success: bool, stdout: str)`.
- `run_command_popen()` — starts a command asynchronously, returns a `Popen` object.
- `check_popen_finished()` — waits for a `Popen` process to finish and returns `(exit_code, stdout, stderr)`.

### Utilities

**`utils.py`**
Broad utility module. Key enums and classes:
- `MWAXSubfileDistirbutorMode` — `UNKNOWN`, `BEAMFORMER`, `CORRELATOR`.
- `CorrelatorMode` — all MWAX operating modes (`MWAX_CORRELATOR`, `MWAX_VCS`, `MWAX_BEAMFORMER`, `MWAX_BUFFER`, etc.) with static helpers `is_correlator()`, `is_vcs()`, `is_beamformer()`, etc.
- `MWADataFileType` — maps file types to their MWA metadata database IDs (voltages, visibilities, VDIF, filterbank, PPD, etc.).
- `ArchiveLocation` — Pawsey LTS destinations (`AcaciaIngest`, `Banksia`, `AcaciaMWA`).
- `ValidationData` — result struct from `validate_filename()`, containing `obs_id`, `project_id`, `filetype_id`, `calibrator` flag, and `valid`.

Key functions include filename validation, metafits creation/reading, MD5 checksumming, PSRDADA header parsing, Redis-based beamformer signalling, multicast sending, and config file helpers.

### Archiving

**`mwa_archiver.py`**
Stateless file transfer functions:
- `copy_file_rsync()` — copies a file to a remote host via rsync over SSH with AES128-CTR cipher.
- `archive_file_xrootd()` — uploads a file to a remote xrootd server using a `.part` temporary filename, then atomically renames it via SSH `mv` on success.
- `archive_file_rclone()` — uploads a file to Pawsey S3 (Acacia/Banksia) via `rclone copyto`, with checksum verification via `rclone check`. Iterates over multiple configured endpoints on failure.
- `ceph_get_s3_md5_etag()` — computes the Ceph-style multipart ETag (MD5 of MD5s) for S3 integrity verification.

### Watch / Queue Pipeline

**`mwax_watcher.py`** — `Watcher`
Uses Linux `inotify` to watch a directory for file events (`IN_CLOSE_WRITE`, `IN_MOVED_TO`, or both). On startup, performs a one-shot scan of pre-existing files before entering the live event loop. Deposits file paths into a plain `queue.Queue`.

**`mwax_priority_watcher.py`** — `PriorityWatcher`
Same as `Watcher` but deposits into a `queue.PriorityQueue`. Reads the associated metafits file to determine each observation's project ID and assigns a numeric priority so that high-priority projects are processed first.

**`mwax_priority_queue_data.py`** — `MWAXPriorityQueueData`
A wrapper for file paths used as `PriorityQueue` payloads. Overrides comparison operators so that equal-priority items are sorted by filename only (ignoring directory path), giving consistent ordering.

**`mwax_queue_worker.py`** — `QueueWorker`
Processes items from a `queue.Queue`, calling either a provided `event_handler` callable or running a shell command with token substitution. Implements exponential backoff on failure with three configurable strategies: requeue to end of queue, keep retrying the same item, or drop failed items entirely.

**`mwax_priority_queue_worker.py`** — `PriorityQueueWorker`
Identical logic to `QueueWorker` but operates on a `PriorityQueue`. When requeueing a failed item to the end of the queue, increments its priority number so it sinks toward the back.

**`mwax_watch_queue_worker.py`** — `MWAXWatchQueueWorker`, `MWAXPriorityWatchQueueWorker`
Abstract base classes that compose a watcher (or priority watcher) with a queue worker into a single manageable unit. On `start()`, all watcher threads are launched first and the queue worker thread is held until all watchers have completed their initial directory scan, ensuring prioritisation is applied across the full backlog before processing begins. Subclasses implement only the abstract `handler(item: str) -> bool` method.

### WQW Processor Implementations

These are concrete subclasses of `MWAXWatchQueueWorker` or `MWAXPriorityWatchQueueWorker`, instantiated by `MWAXSubfileDistributor` or `MWACacheArchiveProcessor`.

**`mwax_wqw_subfile_incoming_processor.py`** — `SubfileIncomingProcessor`
Handles raw PSRDADA `.sub` subfiles arriving from the MWAX DSP hardware. Reads the subfile header to determine the operating mode and routes accordingly: loads into the PSRDADA ring buffer (correlator), copies to volt data path (VCS/voltage dump), or signals the beamformer via Redis. Also handles voltage dump triggering (FREDDA detection events), packet statistics extraction, and the `always_keep_subfiles` mode.

**`mwax_wqw_checksum_and_db.py`** — `ChecksumAndDBProcessor`
Receives output files after correlation/beamforming. Computes the MD5 checksum, inserts a record into the MWA metadata database, then routes the file to the correct outgoing or don't-archive directory based on file type (visibilities, voltages, PPD, VDIF, filterbank) and whether the project should be archived.

**`mwax_wqw_bf_stitching_processor.py`** — `BfStitchingProcessor`
Waits for beamformer subobservation files (`.vdif` or `.fil`). After the final expected subobs for an observation arrives, globs all matching subobs files and stitches them into a single complete observation file using the appropriate format utility. Optionally keeps originals before stitching.

**`mwax_wqw_vis_stats.py`** — `VisStatsProcessor`
Runs the external `mwax_stats` binary on the first visibility file (index `_000.fits`) of each observation. Then routes files: non-archived projects go to `dont_archive`, calibrator observations go to `outgoing_cal` (for calvin), and all others go to `outgoing` for archiving.

**`mwax_wqw_vis_cal_outgoing.py`** — `VisCalOutgoingProcessor`
Simple pass-through: appends calibrator FITS file paths to a shared thread-safe list that `MWAXSubfileDistributor` uses to track calibration observations ready for the calvin pipeline.

**`mwax_wqw_outgoing.py`** — `OutgoingProcessor`
Archives visibility, voltage, and beamformer files from MWAX boxes to the mwacache servers via `archive_file_xrootd()`, then deletes the local copy.

**`mwax_wqw_pawsey_outgoing.py`** — `PawseyOutgoingProcessor`
Runs on mwacache servers. Validates files, checks size and MD5 checksum against the remote metadata database, archives to Pawsey LTS (Acacia/Banksia) via `archive_file_rclone()`, updates the MRO metadata database to mark the file as archived, then deletes the local copy.

**`mwax_wqw_packet_stats_processor.py`** — `PacketStatsProcessor`
Copies packet statistics dump files to a remote destination host (e.g. `vulcan`) using `shutil.copy2`, then deletes the local file.

### Calvin Calibration Pipeline

**`mwax_asvo_helper.py`** — `MWAASVOHelper`, `MWAASVOJob`, `MWAASVOJobState`
Manages interaction with the MWA ASVO data download service via the `giant-squid` CLI. `MWAASVOJob` tracks a single download job including its state, request IDs, submission timestamp, and download URL. `MWAASVOHelper` maintains the list of in-flight jobs, calls `giant-squid submitvis` to submit new jobs, and calls `giant-squid list` to poll job states. Raises typed exceptions for outages (`GiantSquidMWAASVOOutageException`) and duplicate submissions (`GiantSquidJobAlreadyExistsException`).

**`mwax_calvin_utils.py`**
Calibration support utilities. Contains `CalvinJobType` enum (`realtime` / `mwa_asvo`); data structures `Tile`, `Metafits`, `HyperfitsSolution`, `HyperfitsSolutionGroup`, `GainFitInfo`, `PhaseFitInfo`; AOCAL binary format constants; `create_sbatch_script()` and `submit_sbatch()` for SLURM job management; phase and gain fit processing functions; and `estimate_birli_output_bytes()` for storage estimation.

**`mwax_calvin_solutions.py`** — `process_solutions()`
Post-processes calibration solutions produced by `hyperdrive`. Loads hyperfits solution files and metafits, determines a reference antenna, fits phases and gains per coarse channel, and inserts the results into the calibration database via `insert_calibration_fits_row` and `insert_calibration_solutions_row`.

### Beamformer Format Utilities

**`mwax_bf_filterbank_utils.py`**
Low-level utilities for the Sigproc filterbank format. Parses and modifies the variable-length binary header, reads/writes the `datalen` field, and concatenates multiple subobs filterbank files into a single output file.

**`mwax_bf_vdif_utils.py`** — `VDIFHeader`
Utilities for the VDIF beamformer format. `VDIFHeader` reads pointing, frequency, and timing information from a metafits file to populate a VDIF header. Provides functions to stitch multiple subobs VDIF files into a complete observation output file.
