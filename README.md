# mwax-mover

A suite of command line tools which are part of the MWAX correlator for the MWA.

The `mwax_mover` suite manages data ingestion, distribution, archiving, and near-realtime calibration for the **MWAX correlator** — part of the **Murchison Widefield Array (MWA)** radio telescope at the Murchison Radio-observatory (MRO) in Western Australia.

Four long-running services:

* **mwax_subfile_distributor** - the main real-time data handling engine of the MWAX correlator and beamformer. Responsible for sending new subobservations to the correlator, beamformer or to disk; and archiving subfiles or correlated visibilities to the mwacache servers. Output from the beamformer gets sent to another host running FREDDA (FRB detection pipeline). FREDDA can then signal this process to dump subfiles to disk if a detection is made.
* **mwacache_archiver** - runs on the mwacache servers at Curtin. Monitors for new files sent from MWAX servers and then sends them to Pawsey's Long Term Storage and updates the MWA metadata db to confirm they were archived.
* **mwax_calvin_controller** - runs on the Calvin SLURM cluster at the MRO. MWAX servers keep any FITS files from calibrator observations in a `cal_outgoing` directory. The mwax_calvin_controller detects a new calibration is required and then submits a SLURM job to the cluster which runs the mwax_calvin_processor.
* **mwax_calvin_processor** - runs on a Calvin HPC node when SLURM commands it, copies the calibrator visibility files from all of the MWAX hosts, then performs calibration, uploading the solution to the MWA database. Once completed, the calvin host calls `release_cal_obs` on each of the MWAX host's web service endpoints which then tells calvin to either move the files to the vis_outgoing dir for archiving or to the dont_archive directory.

Plus these command line utilities, for diagnostics and one-off maintenance:

* **cal_utils** - runs the same flagging/plotting pipeline as mwax_calvin_processor against solution file(s) you already have, without touching the database. Useful for investigating a fit or trying different thresholds. See CALVIN.md.
* **update_calvin_plots_and_index** - regenerates the plots and `index.json` for fits which have already been processed, and stages them for upload.
* **generate_index_json** - builds the `index.json` manifest for a single fit directory.
* **fits_inspect** - prints the header keys and data of a given HDU in any FITS file.
* **print_metafits_info** - prints the fine and coarse channel layout of a metafits file.
* **vdif_cat** - stitches beamformer VDIF subobservation files into complete observation files.

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

## Developing

```bash
# Enable the ruff format pre-commit hook (once per clone)
$ uv run pre-commit install

# The checks CI runs, which you can run locally before pushing:
$ uv run ruff format --check src/ tests/   # blocking in CI
$ uv run ty check src/                     # blocking in CI
$ uv run pytest                            # blocking in CI
$ uv run ruff check .                      # informational in CI (for now)
```

CI (`.github/workflows/ci.yml`) runs the above on every pull request and on
pushes to `main`, against Python 3.12 and 3.13.

Tests that need network access or real external binaries are marked
`integration` and are deselected by default. Run them explicitly with
`uv run pytest -m integration`.

The test suite creates its scratch directories under the system temp directory.
Set `MWAX_MOVER_TEST_DIR` to put them somewhere else.

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

## mwax_subfile_distributor

### Running mwax_subfile_distributor

```bash
usage: mwax_subfile_distributor [-h] -c CFG

mwax_subfile_distributor: a command line tool which is part of the mwax suite for the MWA. It will perform different tasks based on the configuration file.
In addition, it will automatically archive files in /voltdata and /visdata to the mwacache servers at the Curtin Data Centre.

options:
  -h, --help         show this help message and exit
  -c CFG, --cfg CFG  Configuration file location.
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
# Read-only endpoints answer GET:
curl http://host:port/status

# Everything else changes state and is POST-only:
curl -X POST "http://host:port/command[?param1&param2]"
```

Web service commands. **`/status` is the only endpoint that accepts GET**; every
other endpoint below changes state and answers `POST` only, so that an
accidental GET (a crawler, a link checker, an over-eager monitoring probe)
cannot stop the correlator or dump the voltage buffer. A GET to any of them
returns `405 Method Not Allowed`.

* /status `[GET]`
  * Reports status of all processes in JSON format
* /shutdown `[POST]`
  * Shuts the processor down.
* /pause_archiving `[POST]`
  * Pauses all archiving processes in order to reduce disk contention. (This is called automatically whenever a
  MWAX_VCS observation is running, if in CORRELATOR mode)
* /resume_archiving `[POST]`
  * Resuming archiving processes. (This is called automatically once the correlator is no longer running in
  MWAX_VCS mode)
* /dump_voltages?start=X&end=X&trigger_id=X `[POST]`
  * This will pause archiving and rename all *.free subfiles to *.keep, add the trigger_id to the subfile header,
  then write the .keep files to disk. Once written successfully, all *.keep files are renamed back to *.free so
  mwax_u2s can continue to use them. This webservice call is generally triggered by the M&C system.
* /release_cal_obs?obs_id=X `[POST]`
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
    "mode": "MWAX_CORRELATOR",
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
    "slurm_queue": 10,
    "mwa_asvo_calibration_requests_queued": 150,
    "mwa_asvo_vis_jobs_in_progress": 10,
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

A reference for all Python modules in `src/mwax_mover/`, organised by package
to match the current source tree (see `docs/RESTRUCTURE.md` for the history
of how it got this shape).

### Constants and Version

**`constants.py`**
Module-level constants only. Defines the `__FILE__` and `__FILENOEXT__` token strings used for command substitution, and the three watch-mode string constants (`WATCH_DIR_FOR_NEW`, `WATCH_DIR_FOR_RENAME`, `WATCH_DIR_FOR_RENAME_OR_NEW`).

**`version.py`**
Provides `get_mwax_mover_version_string()`, which reads the installed package version via the stdlib `importlib.metadata`.

### Core (`core/`)

**`core/command.py`**
Thin wrappers around `subprocess`: `run_command_ext()` runs a command synchronously (optionally pinned to a NUMA node), `run_command_popen()` starts one asynchronously and returns a `Popen` object, `check_popen_finished()` waits for it and returns `(exit_code, stdout, stderr)`. Also `write_readme_file()`, a generic command-log writer used by `calvin/birli.py` and `calvin/hyperdrive.py`.

**`core/config.py`**
INI config-file reading helpers built on `configparser`: `read_config()` (required values), `read_optional_config()`, `read_config_list()` (comma-separated), `read_config_bool()`. All accept an optional Base64-decode step.

**`core/units.py`**
Unit conversions (bytes/gigabytes/gibibytes/gigabits), throughput calculation (`get_gbps()`), and `is_int()` for validating CLI string arguments.

**`core/gpstime.py`**
`get_gpstime_of_datetime()` converts a UTC datetime to integer GPS seconds; `get_gpstime_of_now()` is the current-time wrapper.

**`core/env.py`**
`get_hostname()` returns the machine's short hostname; `running_under_pytest()` detects a test run.

### Database (`db/`)

**`db/handler.py`** — `MWAXDBHandler`
Wraps a `psycopg` + `psycopg_pool` connection pool to a PostgreSQL database. Provides `select_one_row_postgres`, `select_many_rows_postgres`, `execute_single_dml_row`, and `execute_dml`, all decorated with `tenacity` retry logic for transient connection failures.

**`db/data_files.py`**
Query/DML functions for the `data_files` table: reading a file's recorded size/checksum, inserting a new row on receipt of a file, marking a row archived once shipped to Pawsey.

**`db/calibration.py`**
Query/DML functions for the calibration tables: `calibration_request` (queuing and status through ASVO submission, Slurm, download, calibration), `calibration_fits` (the per-obsid calibration "header"), `calibration_solutions` (per-tile solutions).

### FITS (`fits/`)

**`fits/metafits.py`**
`download_metafits_file()` fetches a metafits file for an observation from the MWA web services. The `get_metafits_value*` functions read individual FITS header keywords from its primary or a named HDU.

**`fits/subfile.py`**
PSRDADA subfile header reading/writing (`read_subfile_value(s)`, `inject_subfile_header()`, `inject_beamformer_headers()`), mock-subfile builders for tests, and the external stats/ringbuffer tool wrappers that act on subfiles (`process_mwax_stats()`, `load_psrdada_ringbuffer()`, `run_mwax_packet_stats()`, `copy_subfile_to_disk_dd()`). Also `CorrelatorMode` and the `PSRDADA_*` header keyword constants.

### Filesystem (`filesystem/`)

**`filesystem/naming.py`**
`validate_filename()` is the central check: classifies a filename, cross-references its metafits file, and reports project ID and calibrator status. Also `MWADataFileType`, `ArchiveLocation`, `ValidationData` (its result struct), `determine_bucket()`/`get_bucket_name_from_*` (archive bucket names), and `get_priority()` (archiving order).

**`filesystem/scan.py`**
`scan_directory()` returns glob matches as a list; `scan_for_existing_files_and_add_to_queue()` scans and enqueues them onto a plain `queue.Queue` in sorted order.

**`filesystem/files.py`**
Generic file operations that aren't subfile-specific: `remove_file()`, `delete_files_older_than()`, `do_checksum_md5()` (runs `md5sum`), `extract_tar()`, `get_png_dimensions()`.

### Network (`net/`)

**`net/multicast.py`**
`send_multicast()` sends a UDP datagram to a multicast group; `get_ip_address()` resolves a local interface name to its IPv4 address.

**`net/webservice.py`**
`call_webservice()` tries each URL in an ordered list, retrying the whole list on failure — the generic building block `fits/metafits.py`'s metafits download is built on.

**`net/redis.py`**
`push_message_to_redis()` JSON-serialises a message and `LPUSH`es it onto a Redis list, with a small retry loop.

**`net/s3.py`**
rclone wrappers for S3-compatible remotes (Acacia, Banksia): `rclone_move()`, `rclone_delete_file()`, `check_remote_file_exists()`.

**`net/asvo.py`**
`run_giant_squid()` shells out to the `giant-squid` binary, retrying transient failures and raising a specific exception for a known ASVO outage or server-side error code. `extract_filename_from_mwa_asvo_signed_url()` pulls the filename out of an ASVO presigned download URL.

### Archiving (`archive/`)

**`archive/archiver.py`**
Stateless file transfer functions: `copy_file_rsync()` (host-to-host via SSH/rsync, AES128-CTR), `archive_file_xrootd()` (uploads to xrootd with atomic temp-file rename), `archive_file_rclone_haproxy()` (uploads to Pawsey S3 via rclone through a local HAProxy instance, verified with `rclone check`).

### Watch / Queue Pipeline (`queues/`)

**`queues/watcher.py`** — `Watcher`
Uses Linux `inotify` to watch a directory for file events (`IN_CLOSE_WRITE`, `IN_MOVED_TO`, or both). On startup, performs a one-shot scan of pre-existing files before entering the live event loop. Deposits file paths into a plain `queue.Queue`.

**`queues/priority_watcher.py`** — `PriorityWatcher`
Same as `Watcher` but deposits into a `queue.PriorityQueue`. Reads the associated metafits file to determine each observation's project ID and assigns a numeric priority so that high-priority projects are processed first.

**`queues/priority_queue_data.py`** — `MWAXPriorityQueueData`
A wrapper for file paths used as `PriorityQueue` payloads. Overrides comparison operators so that equal-priority items are sorted by filename only (ignoring directory path), giving consistent ordering.

**`queues/queue_worker.py`** — `QueueWorker`
Processes items from a `queue.Queue`, calling either a provided `event_handler` callable or running a shell command with token substitution. Implements exponential backoff on failure (`calculate_backoff_seconds()`: `initial * factor**(n-1)`, capped at `backoff_limit_seconds`) with three configurable strategies: requeue to end of queue, keep retrying the same item, or drop failed items entirely.

**`queues/priority_queue_worker.py`** — `PriorityQueueWorker`
Identical logic to `QueueWorker` but operates on a `PriorityQueue`. When requeueing a failed item to the end of the queue, increments its priority number so it sinks toward the back.

**`queues/watch_queue_worker.py`** — `MWAXWatchQueueWorker`, `MWAXPriorityWatchQueueWorker`
Abstract base classes that compose a watcher (or priority watcher) with a queue worker into a single manageable unit. On `start()`, all watcher threads are launched first and the queue worker thread is held until all watchers have completed their initial directory scan, ensuring prioritisation is applied across the full backlog before processing begins. Subclasses implement only the abstract `handler(item: str) -> bool` method.

### WQW Processor Implementations (`processors/`)

These are concrete subclasses of `MWAXWatchQueueWorker` or `MWAXPriorityWatchQueueWorker`, instantiated by `MWAXSubfileDistributor` or `MWACacheArchiveProcessor`.

**`processors/subfile_incoming.py`** — `SubfileIncomingProcessor`
Handles raw PSRDADA `.sub` subfiles arriving from the MWAX DSP hardware. Reads the subfile header to determine the operating mode and routes accordingly: loads into the PSRDADA ring buffer (correlator), copies to volt data path (VCS/voltage dump), or signals the beamformer via Redis. Also handles voltage dump triggering (FREDDA detection events), packet statistics extraction, and the `always_keep_subfiles` mode.

**`processors/checksum_and_db.py`** — `ChecksumAndDBProcessor`
Receives output files after correlation/beamforming. Computes the MD5 checksum, inserts a record into the MWA metadata database, then routes the file to the correct outgoing or don't-archive directory based on file type (visibilities, voltages, PPD, VDIF, filterbank) and whether the project should be archived.

**`processors/bf_stitching.py`** — `BfStitchingProcessor`
Waits for beamformer subobservation files (`.vdif` or `.fil`). After the final expected subobs for an observation arrives, globs all matching subobs files and stitches them into a single complete observation file using the appropriate format utility. Optionally keeps originals before stitching.

**`processors/vis_stats.py`** — `VisStatsProcessor`
Runs the external `mwax_stats` binary on the first visibility file (index `_000.fits`) of each observation. Then routes files: non-archived projects go to `dont_archive`, calibrator observations go to `outgoing_cal` (for calvin), and all others go to `outgoing` for archiving.

**`processors/vis_cal_outgoing.py`** — `VisCalOutgoingProcessor`
Simple pass-through: appends calibrator FITS file paths to a shared thread-safe list that `MWAXSubfileDistributor` uses to track calibration observations ready for the calvin pipeline.

**`processors/outgoing.py`** — `OutgoingProcessor`
Archives visibility, voltage, and beamformer files from MWAX boxes to the mwacache servers via `archive_file_xrootd()`, then deletes the local copy.

**`processors/pawsey_outgoing.py`** — `PawseyOutgoingProcessor`
Runs on mwacache servers. Validates files, checks size and MD5 checksum against the remote metadata database, archives to Pawsey LTS (Acacia/Banksia) via `archive_file_rclone_haproxy()`, updates the MRO metadata database to mark the file as archived, then deletes the local copy.

**`processors/packet_stats.py`** — `PacketStatsProcessor`
Copies packet statistics dump files to a remote destination host (e.g. `vulcan`) using `shutil.copy2`, then deletes the local file.

### Calibration Domain (`calibration/`)

Shared data structures and pure numeric functions used by the Calvin pipeline — no dependency on `calvin/`, so nothing here ever imports it.

**`calibration/models.py`**
`Tile`, `Input`, `ChanInfo`, `TimeInfo`, `Metafits` (wraps `mwalib.MetafitsContext` and is the source of the others), `PhaseFitInfo` and `GainFitInfo` (fit results).

**`calibration/solutions.py`**
Raw hyperdrive solution-file HDU array readers: `read_solutions_hdu_complex()`, `read_results_hdu()`, `read_tiles_hdu()`, `read_baseline_tile_flags()`.

**`calibration/fitting.py`**
`fit_phase_line()` (linear phase-ramp fit via an exact analytic Hessian) and `fit_gain()` (gain amplitude vs. frequency), plus the numeric helpers they depend on. `poly_str()` formats fit results for display.

**`calibration/outliers.py`**
`reject_outliers()` is the core robust (MAD-based) threshold test. `annotate_phase_outliers()` is the single shared definition of "phase outlier" used everywhere in the Calvin pipeline. `iterative_poly_clip_batch()` fits a robust, sigma-clipped polynomial (batched across tiles) and flags outliers.

### Calvin Calibration Pipeline (`calvin/`)

See `CALVIN.md` for the full pipeline description.

**`calvin/asvo.py`** — `MWAASVOHelper`, `MWAASVOJob`, `MWAASVOJobState`
Manages interaction with the MWA ASVO data download service via the `giant-squid` CLI (built on `net/asvo.py`'s lower-level wrapper). `MWAASVOJob` tracks a single download job including its state, request IDs, submission timestamp, and download URL. `MWAASVOHelper` maintains the list of in-flight jobs, calls `giant-squid submitvis` to submit new jobs, and calls `giant-squid list` to poll job states. Raises typed exceptions for outages (`GiantSquidMWAASVOOutageException`) and duplicate submissions (`GiantSquidJobAlreadyExistsException`).

**`calvin/pipeline.py`** — `CalvinJobType`, `process_solutions()`
`CalvinJobType` distinguishes a realtime job from an MWA ASVO download job. `process_solutions()` loads the hyperfits solution files and metafits, determines a reference antenna, runs the full flagging pipeline, generates before/after plots and the per-tile stats file, commits the flagged solutions to disk, fits final phases and gains, and inserts the results into the calibration database.

**`calvin/slurm.py`**
`create_sbatch_script()` renders a Slurm batch script (partition/priority/walltime depend on `CalvinJobType`); `submit_sbatch()` writes and submits it; `count_slurm_asvo_jobs()` queries the Slurm queue directly.

**`calvin/birli.py`**
`run_birli()` shells out to the Birli binary to preprocess visibility data. `estimate_birli_output_bytes()` is a pre-flight storage-size estimate.

**`calvin/hyperdrive.py`** — `HyperfitsSolution`, `HyperfitsSolutionGroup`
`run_hyperdrive()` shells out to the hyperdrive binary to produce calibration solutions; `write_hyperdrive_stats()` writes a convergence summary for a just-produced solution file. `HyperfitsSolution` reads a single hyperdrive FITS solutions file; `HyperfitsSolutionGroup` holds one solution file per contiguous coarse-channel band plus the observation's metafits, and owns the whole flagging pipeline via `run_flagging_pipeline()`: `apply_tile_flags()`, `enforce_whole_jones_nan()`, `flag_gain_max_cutoff()`, `flag_amplitude_outliers()`, `flag_mostly_bad_tiles()`, then report-only `detect_phase_outliers()`. `commit()` writes the result back to disk.

**`calvin/solution_files.py`**
Solution-file naming (`get_solution_fits_filename()`, `parse_solution_channels()`, `get_sorted_solution_files()`), export (`export_calibration_solutions()`), and staged/atomic publishing of a fit's plots and stats (`upload_plot_files()`, `get_staging_path()`, `reap_orphaned_staging_dirs()`).

**`calvin/plots/`** — plotting and plot-adjacent reporting
- `layout.py` — figure-sizing helpers (`plot_dpi()`, `plot_figsize()`) shared by every plot here.
- `phase_fits.py` — phase-fit diagnostic plots (intercepts, residuals, per-tile fits) and `write_debug_phase_fit_plots()`, the `HyperfitsSolutionGroup`-level entry point.
- `hyperdrive_plots.py` — `generate_hyperdrive_plots(_for_files)`, which run hyperdrive's own `solutions-plot` subcommand.
- `gains.py` — the paged, paginated amplitude-outlier plots (`plot_combined_gains()`, `plot_outlier_gains()`), stitching multiple picket-fence files onto one continuous x-axis and budgeting concurrent rendering workers against available memory.
- `stats_table.py` — the before/after per-tile stats table (`build_tile_stats_rows()`, `write_tile_stats_table()`, `write_before_after_stats()`).
- `index.py` — `index.json` manifest generation for a fit's uploaded files (`generate_plot_index_file()`, `populate_index_json_entry()`).

### Beamformer Format Utilities (`beamformer/`)

**`beamformer/filterbank.py`**
Low-level utilities for the Sigproc filterbank format. Parses and modifies the variable-length binary header, reads/writes the `datalen` field, and concatenates multiple subobs filterbank files into a single output file.

**`beamformer/vdif.py`** — `VDIFHeader`
Utilities for the VDIF beamformer format. `VDIFHeader` reads pointing, frequency, and timing information from a metafits file to populate a VDIF header. Provides functions to stitch multiple subobs VDIF files into a complete observation output file.
