# mwax_calvin_local: design

Status: **design only.** No code is written. Open decisions are at the end.

A command line tool that runs the Calvin pipeline on one MWA ASVO download,
on one machine, for testing. It takes an MWA ASVO signed URL and a
directory. It downloads and extracts the data into the directory, then runs
Birli, hyperdrive di-calibrate, the solution processing, hyperdrive
solutions-apply and WSClean (with the preview PNG). Everything stays in the
directory.

---

## Rules

The tool must **not** touch:

- the database (no `mwax_mover.db`, no `psycopg`),
- the upload (no `plot_upload_path`, no S3/rclone, no `index.json`, no
  controller),
- the MWAX servers (no `release_mwax_files`, no realtime download),
- Slurm.

How we make sure of this:

- The CLI module and every new module it imports must not import
  `mwax_mover.db`, `mwax_mover.net` or `mwax_mover.archive`. A test starts a
  new Python process, imports the CLI module, and checks that none of them
  (and not `psycopg`) is in `sys.modules`.
- `calvin/pipeline.py` imports `mwax_mover.db.calibration` at the top. So the
  new database-free processing code cannot live in that file (see "Code
  changes").
- The only network access besides the signed URL is the MWA web service, for
  the metafits file (see open decision O4).

---

## Command line

```
mwax_calvin_local --cfg CFG --url SIGNED_URL --dir DIR [--skip-download] [--stop-after STEP]
```

| Argument | Meaning |
| --- | --- |
| `--cfg` | A `calvin_processor.cfg`. Gives the binary paths and options. See "Configuration". (Decided.) |
| `--url` | The MWA ASVO signed URL (a `..._vis.tar` file on Acacia). |
| `--dir` | The directory to download and process in. It is made if it does not exist. |
| `--skip-download` | Use the data that is already in `DIR/input`. Used to run again with different options. |
| `--stop-after STEP` | Stop after `download`, `birli`, `hyperdrive`, `solutions`, `apply` or `wsclean`. |

- The observation ID is the first part of the tar file name in the URL
  (`1444927824_1021186_vis.tar` gives `1444927824`), using the existing
  `extract_filename_from_mwa_asvo_signed_url()`. If it is not 10 digits, the
  tool stops with an error before it does anything.
- The tool always runs imaging, whatever `[wsclean] enabled` says in the cfg.
  That is the point of the tool. It still needs the other `[wsclean]` keys.
- The exit code is 0 if every step worked, 1 if a step failed, 2 for a bad
  command line or config.

---

## Directory layout

```
DIR/
  input/    the extracted download, and <obs_id>_metafits.fits
  work/     the Birli measurement sets, then the calibrated ones (_cal.ms)
  output/   solutions, plots, stats, readmes, WSClean FITS files, the PNG
  calvin_local.log
```

- The names `input` and `output` match the processor, so the existing
  functions work with no change. The processor uses one directory pair per
  Slurm job. Here `DIR` is the job.
- `work` is on the same disk as the rest (not in the RAM-backed
  `temp_working_path`). So nothing is subtracted for RAM: Birli uses
  `[birli] max_mem_gib` and WSClean uses `[wsclean] max_mem_gib` as they are.
  The tool checks the free space in `DIR` before Birli, using
  `estimate_birli_output_bytes()` times `WORKING_SPACE_FACTOR_IMAGING`, and
  stops if it is not enough.
- A second run in the same `DIR` does not delete anything. Birli, hyperdrive
  and WSClean outputs are overwritten or fail as they do now. To start clean,
  use a new directory. (`--skip-download` is the only reuse we support.)

---

## Steps

| # | Step | Function that is used | Notes |
| --- | --- | --- | --- |
| 0 | Check | new `read_local_config()` | Read the cfg subset. Check that the Birli, hyperdrive, WSClean binaries and the source list exist. Work out the obs ID. |
| 1 | Download | new `download_signed_url()`, then `extract_tar()` | See "Download". |
| 2 | Metafits | `download_metafits_file()` | Same call as the processor. Written to `input/`. |
| 3 | Birli | `run_birli()` | Same arguments as `MWAXCalvinProcessor.run_birli()`. Output to `work/`. |
| 4 | di-calibrate | `run_hyperdrive()` | One run per MS. Solutions to `output/`. |
| 5 | Solutions | new `process_solutions_files()` | The processor's `process_solutions()` without the database. Writes the modified `*_solutions.fits`, plots and stats to `output/`. |
| 6 | solutions-apply | `run_solutions_apply()` | Uses the modified solutions. |
| 7 | WSClean | `run_wsclean()` and `make_wsclean_preview()` | Same options and `-abs-mem` rule as the processor. |
| 8 | Summary | new | Log and print each step, its time, and the paths of the outputs and the PNG. |

A step that fails stops the run, and everything stays on disk for
inspection. The exception is the preview PNG: if it fails, the tool logs a
warning and the run is still a success.

If step 5 finds no unflagged tiles, `process_solutions()` returns success
with no fit. The tool treats that as a failure of the run (there is nothing
to apply).

### Download

- `requests.get(url, stream=True)` writes to `input/<tar name>`. The tool
  checks the HTTP status first. Status 403 on a signed URL means that the URL
  has expired: the tool says so and does not retry.
- Retries and the wait between them come from `[downloading] download_retries`
  and `download_retry_wait`. The time limit for the whole download is
  `mwaasvo_download_obs_timeout`. The proxy is `[downloading] https_proxy` and
  `no_proxy` (the processor sets the same values for giant-squid, with the
  same defaults).
- Then `extract_tar()` (it rejects unsafe paths) and the tar is deleted,
  unless `keep_completed_visibility_files=1`.
- giant-squid is not used. It can only download by job ID or obs ID with MWA
  ASVO credentials. It cannot take a signed URL.
- Not in the first version: resume, and stream-untar (which saves the disk
  space of the tar). `--skip-download` is the workaround for the first.

---

## Configuration

The tool reads only the keys it needs. It does not need the other sections,
so a cfg file that still has them is fine.

| Section | Keys used |
| --- | --- |
| `[birli]` | `binary_path`, `timeout`, `max_mem_gib`, `freq_res_khz`, `int_time_res_sec`, `edge_width_khz` |
| `[hyperdrive]` | `binary_path`, `timeout`, `apply_timeout`, `num_sources`, `source_list_filename`, `source_list_type`, `extra_args` |
| `[processing]` | `phase_fit_niter`, `phase_outlier_nstd`, `gains_cut_off_max` (optional), `gain_outlier_poly_degree`, `gain_outlier_mad_residual_threshold`, `gain_outlier_modify_gains`, `gain_outlier_plot_n_tiles_per_page`, `tile_bad_channel_fraction`, `keep_completed_visibility_files` |
| `[wsclean]` | `binary_path`, `timeout`, `max_mem_gib` and all the option keys (read with the existing `wsclean_args_from_config()`) |
| `[downloading]` | `download_retries`, `download_retry_wait`, `mwaasvo_download_obs_timeout`, `https_proxy` and `no_proxy` (optional) |

Not read: `[mwax mover]`, `[mwa database]`, `[giant squid]`,
`[plots upload]`, and in `[processing]` the `job_input_path`,
`job_output_path`, `temp_working_path`, `cal_export_path`,
`plot_upload_path`, `plot_front_end_url` and `acacia_*` keys. The tool does
not read `plot_upload_path`, so it cannot publish by mistake.

`read_local_config()` returns one dataclass of settings. A missing or bad key
gives a clear error and exit code 2, before any download starts.

---

## Code changes

1. **Split `process_solutions()`.** The processing (load, flag, plots, stats,
   `commit()`, the gain and phase fits) goes into a new module
   `calvin/solution_processing.py` that has no database import. It returns a
   dataclass with what the database insert needs: `gain_fits`, `phase_fits`,
   `soln_tile_ids`, `solution_coarse_chans`, `all_metafits_coarse_chans`,
   `n_metafits_coarse`. `calvin/pipeline.py` keeps `process_solutions()` with
   the same signature and return value: it calls the new function, then does
   the database transaction. The processor and its tests do not change. This
   is the only change to code that runs in production for the existing
   pipeline. It needs the slow `tests/calvin/test_pipeline.py` to pass on your
   side, because I cannot run it to the end in my sandbox.
2. **New `cli/mwax_calvin_local.py`** (layer 5 in `tests/test_architecture.py`)
   with `main()`, argument parsing, `read_local_config()`, and the step
   sequence. Download code (`download_signed_url()`) goes in
   `mwa_asvo/` (layer 2), next to `extract_filename_from_mwa_asvo_signed_url()`.
3. **Entry point** `mwax_calvin_local` in `pyproject.toml`.
4. **Imaging steps** (step 6 and 7). See open decision O1.
5. Logging: the shared `LOG_FORMAT`, to the console and to
   `DIR/calvin_local.log`. No health reporting and no daemon base class.

---

## Failure handling

- Each step is a function that returns success or failure. The tool logs the
  step, its time and its result, and stops at the first failure.
- A Birli or hyperdrive failure leaves its readme in `output/`. The existing
  functions already write them. The processor's "move the failed files to the
  error dir" behaviour is used as it is.
- Ctrl-C: the tool stops. The external program (Birli, hyperdrive, WSClean)
  may keep running as a child process. This is the same as the processor
  today. The first version does not add process-group handling.
- The tool never deletes `input/`. It deletes the Birli MS after
  solutions-apply unless `keep_completed_visibility_files=1` (the same rule as
  the processor), and the MS directories at the end of a successful run.

---

## Tests

New tests only.

- Argument parsing: missing arguments, bad URL (not 10-digit obs ID), unknown
  `--stop-after`.
- `read_local_config()`: a full cfg, a cfg with the database, upload and
  giant-squid sections missing, a missing key (names the key).
- `download_signed_url()` with a local test HTTP server: success, status 403
  (no retry), a failure that retries, the time limit.
- The step sequence with every external function patched: order, `--stop-after`,
  `--skip-download`, stop at the first failure, a failed PNG is a warning,
  exit codes.
- `process_solutions_files()` gives the same files and the same fit data as the
  first part of `process_solutions()` (an existing test data set is used if one
  is small enough).
- The "no database, upload or MWAX" import test from the Rules section.
- `tests/test_architecture.py` (the new modules must be classified).

---

## Order of work

Each is a small diff that applies on its own.

1. Split `process_solutions()` (code change 1) and its tests.
2. Shared imaging step, if O1 is yes.
3. `read_local_config()`, `download_signed_url()` and their tests.
4. The CLI, the entry point and the sequence tests.

---

## Decisions made

- **D1.** Download the signed URL directly over HTTP, then untar. (Greg)
- **D2.** The tool takes a `--cfg` argument and reuses the `calvin_processor.cfg`
  sections. (Greg)
- **D3.** No database, no upload, no MWAX servers. (Greg)

## Open decisions

- **O1. Share the imaging step with the processor?** `MWAXCalvinProcessor.run_imaging()`
  holds the logic for steps 6 and 7 (find the MS, the `-abs-mem` check,
  solutions-apply, WSClean, the PNG). Options:
  - **A (recommended):** move it into a function `run_imaging_steps()` in a new
    `calvin/imaging.py`. The processor method calls it. Then the tool tests the
    real code, and the two cannot drift. Cost: about 10 existing tests in
    `tests/cli/test_mwax_calvin_processor.py` patch
    `mwax_mover.cli.mwax_calvin_processor.run_solutions_apply`, `run_wsclean`
    and `make_wsclean_preview`. Those patch targets move, so I need your
    approval to edit those tests.
  - **B:** copy about 40 lines into the tool. No change to existing code or tests.
    The copy can drift from the processor.
- **O2. `work/` on disk (as above) or in a RAM-backed directory?** On disk is
  simpler and safe. A `--work-dir` option could be added later.
- **O3. Name.** `mwax_calvin_local` matches `mwax_calvin_processor` and
  `mwax_calvin_controller`. Say if you want another name.
- **O4. The metafits download.** The processor gets the metafits from the MWA web
  service, not from the tar. I kept that. It is the only network use besides the
  URL. If you want no web service at all, the tool would need a `--metafits`
  argument, or the metafits in the tar (I have not checked that the ASVO
  `_vis.tar` always has one).
