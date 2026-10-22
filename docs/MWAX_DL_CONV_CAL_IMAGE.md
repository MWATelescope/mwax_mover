# mwax_dl_conv_cal_image

Status: **implemented.** The calvin host checks (see the end) are still to do.

A command line tool that downloads, converts, calibrates and images one MWA
ASVO observation on one machine, for testing. It takes an MWA ASVO signed URL
and a directory. It downloads and extracts the data, runs Birli (unless the
download already has CASA measurement sets), hyperdrive di-calibrate, the
solution processing, hyperdrive solutions-apply and WSClean, and makes a PNG
preview of the image. The name is the steps: **d**own**l**oad, **conv**ert,
**cal**ibrate, **image**.

---

## Rules

The tool must **not** touch:

- the database,
- the upload (no `plot_upload_path`, no S3 or rclone, no `index.json`, no
  controller),
- the MWAX servers (no `release_mwax_files`, no realtime download),
- Slurm.

How this is checked (all in `tests/cli/test_mwax_dl_conv_cal_image.py`):

- Every test runs with the database pool, `rclone_move`, `upload_plot_files`,
  `upload_image_files`, the two database inserts and `process_solutions()`
  patched to fail the test if they are called.
- A test reads the imports of the tool module and checks that it does not
  import `mwax_mover.db`, `net`, `archive`, `calvin.slurm`,
  `calvin.solution_files`, the processor, the controller, `psycopg` or `redis`.
- The tool does import `calvin.pipeline`, which imports the database module at
  the top (only to insert fits in `process_solutions()`). Importing is not using:
  the tool calls only `process_solutions_files()`.

The only network use besides the signed URL is the MWA web service, for the
metafits file. It is the same call as the processor makes (`download_metafits_file()`),
for consistency. The `_vis.tar` also holds a metafits, but it is not used.

---

## Command line

```
mwax_dl_conv_cal_image --cfg CFG --url SIGNED_URL --dir DIR
                       [--work-dir WORK] [--skip-download] [--stop-after STEP]
```

| Argument | Meaning |
| --- | --- |
| `--cfg` | A `calvin_processor.cfg`. It gives the program paths and options. |
| `--url` | The MWA ASVO signed URL. |
| `--dir` | The directory to download and process in. It is made if it does not exist. |
| `--work-dir` | Where the measurement sets go. The default is `DIR/work`. Use a disk directory (for example `/data/x`) or a RAM-backed directory (for example `/tmp/x`). |
| `--skip-download` | Use the data that is already in `DIR/input`. For running again with other options. |
| `--stop-after STEP` | Stop after `download`, `metafits`, `birli`, `hyperdrive`, `solutions` or `imaging`. |

The file name in the URL decides what happens:

| File name | Meaning |
| --- | --- |
| `<obs_id>_<job_id>_vis.tar` | Raw visibilities. Birli runs. |
| `<obs_id>_<job_id>_ms.tar` | CASA measurement sets. Birli is **skipped**. |

Any other name (or an obs ID that is not 10 digits) stops the tool at once.

Exit codes: 0 if every step worked, 1 if a step failed, 2 for a bad command line
or config (before anything is downloaded).

The tool always runs imaging, whatever `[wsclean] enabled` says. It still needs
the other `[wsclean]` keys.

---

## Directory layout

```
DIR/
  input/    the extracted download, and <obs_id>_metafits.fits
  output/   solutions, plots, stats, readmes, WSClean FITS files, the PNG
  work/     the measurement sets (or --work-dir)
  mwax_dl_conv_cal_image.log
```

- `input` and `output` match the processor, so the existing functions work with
  no change. The tool never deletes `input/` (except the tar after it is
  extracted, unless `keep_completed_visibility_files=1`).
- The tool looks at the mount table (`core.env.is_ram_backed()`). If the work
  directory is on tmpfs or ramfs, the room needed is also RAM that Birli and
  WSClean cannot use. It is taken off `[birli] max_mem_gib` and
  `[wsclean] max_mem_gib`, as the processor does for its RAM-backed
  `temp_working_path`. On a disk directory, the memory limits are used as they are.
- The tool checks the free space in the work directory before Birli and stops
  if there is not enough. The need is `WORKING_SPACE_FACTOR_IMAGING` times the
  measurement set size (the Birli estimate for `_vis.tar`, the real size for
  `_ms.tar`).

---

## Steps

| Step | What it does | Function used |
| --- | --- | --- |
| `download` | Download the URL, extract it. For `_ms.tar`, move the measurement sets to the work directory. | `download_signed_url()`, `extract_tar()` |
| `metafits` | Get the metafits from the MWA web service (unless it is already in `input/`). | `download_metafits_file()` |
| disk space | Check the room and set the memory limits. | `estimate_birli_output_bytes()`, `is_ram_backed()` |
| `birli` | Make the measurement sets. Skipped for `_ms.tar`. | `run_birli()` |
| `hyperdrive` | di-calibrate each measurement set. | `run_hyperdrive()` |
| `solutions` | Flag, plot, write the stats and the modified solutions. No database. | `process_solutions_files()` |
| `imaging` | solutions-apply, WSClean and the PNG. | `run_imaging_steps()` |
| cleanup | Remove the measurement sets that are not needed. Only after a full run. | |

The run stops at the first failed step, and everything stays on disk. A failed
PNG is a warning, not a failure. If `solutions` finds no unflagged tiles, that
is a failure here (there is nothing to apply).

The tool prints a summary (step, status, seconds, detail), and writes it to the log.

### Download

- `download_signed_url()` uses `requests` (streaming). HTTP 403 means that the
  URL has expired: the tool says so and does not retry. HTTP 404 is not
  retried. Other failures are retried. A file that is shorter than the server
  said is a failure.
- `[downloading] download_retries` is the number of attempts,
  `download_retry_wait` the wait between them, `mwaasvo_download_obs_timeout`
  the time limit of one attempt.
- Proxy: `[downloading] https_proxy` and `no_proxy` are used **if they are in the
  cfg**. The processor uses `http://localhost:3128` if `https_proxy` is missing.
  The tool uses no proxy then, because it can run on a machine that has none.
- giant-squid is not used. It downloads by job ID with an MWA ASVO API key. It
  cannot take a signed URL.
- Not in the first version: resume, and stream-untar. `--skip-download` is the
  workaround for re-runs.

### `_ms.tar` downloads

- The measurement sets must be named like the ones Birli makes: `<obs_id>.ms`, or
  `<obs_id>_chN.ms` / `<obs_id>_chN-M.ms` for a picket fence observation. The
  solutions are found by that name. Any other name stops the run with an error
  that lists the names. (This is checked on the names Birli makes. We have not seen
  the names that MWA ASVO uses.)
- The measurement sets are the expensive input, so they are **never deleted**.
  Only the calibrated copy (`_cal.ms`) is removed at the end. A second run with
  `--skip-download` uses the measurement sets that are already in `work/`.
- The Birli resolution and edge settings in the cfg are not applied: the
  measurement sets are as the MWA ASVO job made them.
- Birli does not have to exist for an `_ms.tar` run.

---

## Configuration

The tool reads only the keys it needs (`calvin/local_config.py`). The other
sections can stay in the file.

| Section | Keys used |
| --- | --- |
| `[birli]` | `binary_path`, `timeout`, `max_mem_gib`, `freq_res_khz`, `int_time_res_sec`, `edge_width_khz` |
| `[hyperdrive]` | `binary_path`, `timeout`, `apply_timeout`, `num_sources`, `source_list_filename`, `source_list_type`, `extra_args` |
| `[processing]` | `phase_fit_niter`, `phase_outlier_nstd`, `gains_cut_off_max` (optional), `gain_outlier_poly_degree`, `gain_outlier_mad_residual_threshold`, `gain_outlier_modify_gains`, `gain_outlier_plot_n_tiles_per_page`, `tile_bad_channel_fraction`, `keep_completed_visibility_files` |
| `[wsclean]` | `binary_path`, `timeout`, `max_mem_gib` and all the option keys |
| `[downloading]` | `download_retries`, `download_retry_wait`, `mwaasvo_download_obs_timeout`, `https_proxy` and `no_proxy` (optional) |

Not read: `[mwax mover]`, `[mwa database]`, `[giant squid]`, `[plots upload]`, and
in `[processing]` the `job_input_path`, `job_output_path`, `temp_working_path`,
`cal_export_path`, `plot_upload_path`, `plot_front_end_url` and `acacia_*` keys.
The tool does not read `plot_upload_path`, so it cannot publish by mistake.

A missing or bad key is an error that names the section and key, with exit code 2.

---

## Code

- `calvin/pipeline.py`: `process_solutions_files()` is the processing from
  `process_solutions()` (load, flag, plots, stats, commit, fits), with no
  database. `process_solutions()` keeps its signature and calls it, then
  inserts in the database. It stays in `pipeline.py` because the existing slow
  tests patch names in that module.
- `calvin/imaging.py`: `run_imaging_steps()` and `ImagingSettings`, shared with
  the processor (`MWAXCalvinProcessor.run_imaging()` is a thin wrapper).
- `calvin/local_config.py`: `read_local_config()`, `check_files_exist()`.
- `mwa_asvo/signed_url.py`: `parse_signed_url()`, `download_signed_url()`.
- `core/env.py`: `is_ram_backed()`.
- `cli/mwax_dl_conv_cal_image.py` and the `pyproject.toml` entry point.

---

## Decisions

- Download the signed URL directly over HTTP, then untar.
- `--cfg` reuses the `calvin_processor.cfg` sections.
- No database, no upload, no MWAX servers.
- The imaging step is shared with the processor.
- The work directory is an argument (disk or RAM-backed).
- The name is `mwax_dl_conv_cal_image`.
- The metafits comes from the MWA web service.
- `_ms.tar` skips Birli.

## Checks on a calvin host (still to do)

- Run one `_vis.tar` observation and one `_ms.tar` observation.
- Look at the measurement set names inside an `_ms.tar`. If they are not
  `<obs_id>.ms` or `<obs_id>_chN-M.ms`, the check in `collect_measurement_sets()`
  must change.
- Check that di-calibrate works on the MWA ASVO measurement sets (they may have
  a different time and frequency resolution and flagging from the Birli
  settings in the cfg).
- Check that the proxy setting in the cfg is right for the machine you run on.
