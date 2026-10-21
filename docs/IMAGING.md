# mwax_mover calvin imaging plan

Status: **planned.** No phase is implemented yet.

Add a WSClean imaging step to `mwax_calvin_processor`. To do this, Birli
and hyperdrive change from UVFITS to CASA measurement sets (MS), and
hyperdrive `solutions-apply` makes a calibrated MS for WSClean to image.

Findings are against commit `0f12248` on branch `imaging`
("cleanup of wsclean function prior to adding all the options").

---

## Ground rules

Same as the previous plans in `docs/`: never push/PR, ask before actions
that affect other systems, `ruff check`/`ty check` clean and `ruff format`
(line length 120) on every touched file, Google-style docstrings, one
phase per diff, run the relevant test module(s) for touched files, new
logic gets new unit tests. Do not change existing tests without approval.

---

## Background (from discussion, recorded here for the record)

- WSClean is at `/usr/local/bin/wsclean` on each calvin host. It reads MS
  only, not UVFITS.
- An MS is a directory. All files in it must stay together. Use
  `shutil.move` / `shutil.rmtree`, not `remove_file()`.
- Birli MS names (from the Birli source, `run_ranges()`):
  - Not picket fence: `OBSID.ms`.
  - Picket fence, multi-channel band: `OBSID_ch75-76.ms`.
  - Picket fence, single-channel band: `OBSID_ch75.ms` (no range).
- WSClean syntax is `wsclean [options] <a.ms> [<b.ms> ..]`. The MS paths
  are separate arguments. The current `run_wsclean()` joins them with
  commas. This is a bug.
- WSClean default `-name` prefix is `wsclean`, in the process working
  directory. `start_command()` does not set `cwd`. So `-name` must be an
  absolute path in the job output dir.
- Birli writes uncalibrated data to `DATA`. `di-calibrate` writes only
  solutions. `hyperdrive solutions-apply` writes a new output vis file
  with calibrated `DATA`. It does not change the input MS.
- `process_solutions()` overwrites `*_solutions.fits` in place when it
  modifies gains, after it copies the hyperdrive output to
  `*_solutions.original.fits`. So `*_solutions.fits` is the solution that
  is in the database.
- `upload_plot_files()` publishes the fit dir atomically. No file can be
  added after it runs. So WSClean must finish before index.json is made.

---

## Decisions (recorded)

- **D1.** Run `hyperdrive solutions-apply` for each band. Apply
  `*_solutions.fits` (the modified solutions), not `*.original.fits`.
- **D2.** Get the coarse-channel count of a band from the MS filename
  suffix. If there is no suffix, use the metafits count. No new
  dependency. Approved: replace the 3 `_uvfits_num_coarse_chans` tests in
  `tests/calvin/test_hyperdrive.py`.
- **D3.** `-name {job_output_path}/{obs_id}_wsclean`. Keep and upload all
  WSClean FITS outputs (`-image`, `-dirty`, `-psf`, `-residual`,
  `-model`).
- **D4.** Add a `job_succeeded` flag. After `succeed_job_processing()`,
  `signal_handler` must not call `fail_job_processing()`. Also estimate
  and add the extra wall time.
- **D5.** Add `[wsclean] enabled=0|1`.
- **D6.** Fixed WSClean args: `-no-update-model-required` and
  `-temp-dir={working_path}`. Add the extra disk use to the working-path
  size rule.
- **D7.** Fix the success cleanup. It globs `*.uvfits` in
  `job_input_path`, but Birli writes to `working_path`. Remove the MS
  dirs with `shutil.rmtree`.

## Open decisions

- 🔴 **O1. Wall-time mechanism.** Recommended:
  - (a) Controller: replace the hard-coded `"04:00:00"` / `"10:00:00"`
    in `create_sbatch_script()` with config keys
    `realtime_wall_time_secs` / `mwa_asvo_wall_time_secs` (defaults as
    constants). Increase the defaults by `apply_timeout + wsclean
    timeout`. First values: realtime 6 h, MWA ASVO 12 h. Change them
    after the first test runs.
  - (b) Controller passes `--wall-time-secs=N` to the processor in the
    sbatch script. The processor calculates a deadline (start + N − the
    `USR1@360` lead − a reserve for index, upload and cleanup). The
    processor sets the WSClean timeout to
    `min(cfg timeout, deadline − now)`. If the remaining time is less
    than a minimum, it skips WSClean with a warning. This makes sure that
    a slow WSClean run cannot cause the plots to be lost.
- 🔴 **O2. Delete the Birli MS after a successful `solutions-apply`?**
  Recommended: yes, unless `keep_completed_visibility_files=1`. The peak
  disk use is then 2 × the estimate (calibrated MS + WSClean reorder
  files), not 3 ×.
- 🔴 **O3. Is `temp_working_path` (`/tmp`) RAM-backed (tmpfs)?** The code
  reduces `birli_max_mem_gib` by the output size, which suggests yes. If
  yes, extra MS data in `/tmp` reduces the RAM for hyperdrive and
  WSClean. The WSClean memory limit (`-abs-mem`) then becomes a Phase 6
  parameter that we must set.
- 🔴 **O4. `solutions-apply` concurrency.** Recommended: run the bands one
  after the other (sequentially) first. Use the existing Pool pattern only
  if the measured time is too long.

---

## Target sequence

```
Birli (--ms-out) → hyperdrive di-calibrate (--data *.ms)
→ process_solutions → succeed_job_processing (job_succeeded = True)
→ export solutions → release_mwax_files
→ [wsclean enabled] solutions-apply per band → run_wsclean
→ generate_plot_index_file → upload_plot_files → cleanup (rmtree *.ms)
```

Any failure in `solutions-apply` or `run_wsclean` logs a warning. The job
stays successful. If `solutions-apply` fails for one or more bands,
WSClean does not run. (We do not image a partial band set.)

---

## Phase 1: change UVFITS to MS

- `constants.py`: add `EXT_MS = ".ms"`. Remove `EXT_UVFITS` if no code
  uses it after this phase.
- `calvin/birli.py`: change `uvfits_filename` to `ms_filename`, and
  `--uvfits-out=` to `--ms-out=`. Update the docstrings.
- `cli/mwax_calvin_processor.py`:
  - Rename `uvfits_filename` → `ms_filename` and `estimated_uvfits_GB` →
    `estimated_vis_GB`. Update the log text.
  - `run_hyperdrive()`: glob `*{EXT_MS}` in `working_path`.
  - D7 cleanup: glob `*{EXT_MS}` in `working_path`. Remove each dir with
    `shutil.rmtree`. Keep the `keep_completed_visibility_files` rule.
- `filesystem/naming.py`: add `extract_channels_from_ms_filename()`.
  Regex `_ch(\d{1,3})(?:-(\d{1,3}))?\.ms$`. Do not change
  `extract_channels_from_filename()` (it needs a trailing `_`).
- `calvin/hyperdrive.py`:
  - Replace `_uvfits_num_coarse_chans()` with `_ms_num_coarse_chans()`:
    range → `end − start + 1`, single → `1`, no suffix →
    `metafits_context.num_metafits_coarse_chans`.
  - `_run_hyperdrive_one()`: get `obsid_and_band` with `EXT_MS`.
  - Failure path: do not move an MS that is already in
    `job_output_path`. (When `working_path == job_output_path`,
    `shutil.move` raises an error because the destination exists.)
  - Update the docstrings that refer to UVFITS.
- Tests: replace the 3 `_uvfits_num_coarse_chans` tests (D2). Add tests
  for `extract_channels_from_ms_filename()`.
- Check on a calvin host (Greg): Birli MS size against
  `estimate_birli_output_bytes()` (`bytes_per_r_and_i=13` was for
  UVFITS). Hyperdrive solutions from MS are the same as from UVFITS.

## Phase 2: calibrated MS

- `calvin/hyperdrive.py` (or a new `calvin/hyperdrive_apply.py`): add
  `run_solutions_apply(ms_files, solution_files, metafits_filename, ...)`.
  - Match each band MS to its solution by stem:
    `{stem}.ms` ↔ `{stem}_solutions.fits`. A missing match is a failure
    for that band.
  - Command:
    `hyperdrive solutions-apply --no-progress-bars --data {stem}.ms
    {metafits} --solutions {stem}_solutions.fits --outputs
    {stem}_cal.ms`. Output to `working_path`.
  - Write `{stem}_solutions_apply_readme.txt` in `job_output_path`.
  - Return the list of calibrated MS, or a failure.
- Constants: `CALIBRATED_MS_SUFFIX = "_cal.ms"`.
- Config `[hyperdrive] apply_timeout` (seconds).
- O2: delete the Birli MS after a successful apply, if approved.
- The D7 cleanup glob must also remove `*_cal.ms`.
- Size rule (D6): compare `estimated_vis_GB × WORKING_SPACE_FACTOR` with
  the 500 GB rule. Also move `500` to a constant. The factor is 2 if O2
  is approved, 3 if not.
- Tests: band matching (stem pairing, missing solution) and command
  building.

## Phase 3: WSClean step

- `constants.py`: `SECTION_WSCLEAN = "wsclean"`,
  `WSCLEAN_NAME_SUFFIX = "_wsclean"`,
  `WSCLEAN_FITS_GLOB = "*_wsclean-*.fits"`, and
  `WSCLEAN_OUTPUT_SUFFIXES` (`-image.fits`, `-dirty.fits`, `-psf.fits`,
  `-residual.fits`, `-model.fits`).
- Config `[wsclean]`: `enabled`, `binary_path`, `timeout`. Add the
  section to `cfg/calvin_processor.cfg`, `cfg/calvin_processor_test.cfg`
  and the test cfgs in `tests/data/mwax_calvin_processor/`.
- `calvin/wsclean.py`:
  - Add a `WSCleanArgs` dataclass. In this phase it holds only the fixed
    args: `name`, `temp_dir`, `no_update_model_required`. Add
    `to_cmdline_args() -> list[str]`.
  - `run_wsclean(ms_filenames, job_output_path, obs_id, binary_path,
    timeout, args: WSCleanArgs)`. Give the MS paths as separate
    arguments.
  - Failure path: do not move the MS dirs and do not write
    `readme_error.txt`. Always write `{obs_id}_wsclean_readme.txt`.
    Return `False`. The caller logs a warning.
  - Fix the module docstring. It refers to an
    `estimate_wsclean_output_bytes()` function, which does not exist.
- `cli/mwax_calvin_processor.py`:
  - Read the `[wsclean]` config.
  - D4: set `self.job_succeeded = True` after `succeed_job_processing()`.
    In `signal_handler`, if `job_succeeded` is true, log a warning and
    stop with exit code 0. Do not update the database.
  - Insert the apply + WSClean step after `release_mwax_files()` and
    before `generate_plot_index_file()`. Only run it if `enabled`.
  - O1(b), if approved: `--wall-time-secs` argument, deadline and
    timeout cap.
- Tests: `WSCleanArgs.to_cmdline_args()`, run_wsclean failure (no move,
  readme written), signal_handler after success.

## Phase 4: publish the WSClean outputs

- `calvin/plots/index.py`:
  - `populate_index_json_entry()`: accept `.fits` files that end with
    one of the `WSCLEAN_OUTPUT_SUFFIXES` and contain
    `WSCLEAN_NAME_SUFFIX`.
  - `get_file_description()`: add descriptions for each WSClean output
    and for `wsclean_readme.txt` / `solutions_apply_readme.txt`.
- `calvin/solution_files.py` `upload_plot_files()`: add
  `WSCLEAN_FITS_GLOB`. Copy (not move) the files, the same as the
  solutions, so the originals stay in the job folder.
- Tests: index entries for each WSClean output; upload copies, not moves.

## Phase 5: wall time (controller)

- O1(a): config keys and constants in `mwax_calvin_controller` /
  `calvin/slurm.py`. Format the seconds as `HH:MM:SS` for `--time`.
- O1(b): add `--wall-time-secs` to the sbatch script.
- Update `cfg/calvin_controller*.cfg`.
- Tests: `create_sbatch_script()` wall time from config.

## Phase 6: WSClean parameters

- Greg selects the parameters. Add each one to `WSCleanArgs`, to the
  `[wsclean]` config, and to `to_cmdline_args()`.
- Check that `-channels-out > 1` names (`-MFS-image.fits`,
  `-0000-image.fits`) still match the Phase 4 filters.

---

## Test impact

- Replaced (approved, D2): 3 `_uvfits_num_coarse_chans` tests in
  `tests/calvin/test_hyperdrive.py`.
- All other changes add new tests only.
