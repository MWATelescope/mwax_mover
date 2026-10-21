# mwax_mover calvin imaging plan

Status: **planned.** No phase is implemented yet.

Add a WSClean imaging step to `mwax_calvin_processor`. To do this, Birli
and hyperdrive change from UVFITS to CASA measurement sets (MS), and
hyperdrive `solutions-apply` makes a calibrated MS for WSClean to image.
The solutions and plots are published before imaging starts. The WSClean
outputs are published in a second, separate step.

Findings are against commit `a17fc70` on branch `imaging`
("Add calvin imaging plan (docs/IMAGING.md)").

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
- `upload_plot_files()` publishes `<plot_upload_path>/<fit_id>`
  atomically (staging dir + rename). It stops if that dir exists. It
  moves `*.png`, `*.txt`, `*.tsv`, `*.json` out of the job folder and
  copies the solutions.
- The controller uploads each published dir with `rclone move` to
  `<bucket>/<dir name>`, then removes the empty dir. `fit_dir_sort_key()`
  sorts integer dir names newest first.
- `temp_working_path` (`/tmp`) is RAM-backed (tmpfs). Data in it reduces
  the RAM for Birli, hyperdrive and WSClean.
- `/shared/data/calvinNN` exports each host's `/data` only, not `/tmp`.
- A running low-priority SLURM job is not preempted by priority alone.
  Cal jobs use `--exclusive`.

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
  `signal_handler` must not call `fail_job_processing()`.
- **D5.** Add `[wsclean] enabled=0|1`.
- **D6.** Fixed WSClean args: `-no-update-model-required` and
  `-temp-dir={working_path}`. Add the extra disk use to the working-path
  size rule.
- **D7.** Fix the success cleanup. It globs `*.uvfits` in
  `job_input_path`, but Birli writes to `working_path`. Remove the MS
  dirs with `shutil.rmtree`.
- **D8 (O1).** Make the SLURM wall times config-driven (controller). No
  deadline cap on the WSClean timeout. The `[wsclean] timeout` is the
  only bound.
- **D9 (O2).** Delete the Birli MS after a successful `solutions-apply`,
  unless `keep_completed_visibility_files=1`. Peak working space is then
  2 × the estimate (calibrated MS + WSClean reorder files).
- **D10 (O3).** `/tmp` is RAM-backed. So `-abs-mem` is a required
  WSClean parameter. Its value must allow for the MS data in `/tmp`.
- **D11 (O4).** Run `solutions-apply` for one band after the other.
- **D12 (two-stage upload).** Publish the solutions and plots before
  imaging. Publish the WSClean outputs after a successful WSClean run, to
  `<plot_upload_path>/<fit_id>_images`, with an index.json that lists
  all files (first index + WSClean entries).
- **D13 (N1).** The controller maps `<fit_id>_images` to S3 path
  `<bucket>/<fit_id>/`. It uploads `<fit_id>_images` only when
  `<fit_id>` does not exist in the same path. This stops an old
  index.json from replacing the new one.
- **D14 (N2).** Do not use a separate SLURM job for WSClean now. Measure
  the WSClean run time first. See "Deferred".
- **D15 (N3).** If `signal_handler` runs when `job_succeeded` is set, it
  removes the MS dirs as a best effort, then stops with exit code 0. It
  does not update the database.

## Open decisions

None.

---

## Target sequence

```
Birli (--ms-out) → hyperdrive di-calibrate (--data *.ms)
→ process_solutions → succeed_job_processing (job_succeeded = True)
→ export solutions → release_mwax_files
→ index.json + upload_plot_files → <fit_id>            (stage 1)
→ [wsclean enabled]
    solutions-apply per band → delete Birli MS (D9)
    → run_wsclean
    → extended index.json + publish → <fit_id>_images   (stage 2)
→ cleanup (rmtree *.ms)
```

Any failure after stage 1 logs a warning. The job stays successful. If
`solutions-apply` fails for one or more bands, WSClean does not run. (We
do not image a partial band set.) If WSClean fails, there is no stage 2.

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

- New `calvin/hyperdrive_apply.py` (keeps `hyperdrive.py` for
  di-calibrate): `run_solutions_apply(ms_files, metafits_filename,
  job_output_path, working_path, obs_id, binary_path, timeout)`.
  - Match each band MS to its solution by stem:
    `{stem}.ms` ↔ `{job_output_path}/{stem}_solutions.fits`. A missing
    solution is a failure for that band.
  - Command:
    `hyperdrive solutions-apply --no-progress-bars --data {stem}.ms
    {metafits} --solutions {stem}_solutions.fits --outputs
    {stem}_cal.ms`. Output to `working_path`.
  - Write `{stem}_solutions_apply_readme.txt` in `job_output_path`.
  - Return `(success, list of calibrated MS)`. Run the bands one after
    the other (D11).
- Constants: `CALIBRATED_MS_SUFFIX = "_cal.ms"`.
- Config `[hyperdrive] apply_timeout` (seconds).
- D9: after a successful apply, remove the Birli MS dirs unless
  `keep_completed_visibility_files=1`.
- The D7 cleanup glob must also remove `*_cal.ms`. (`*{EXT_MS}` already
  matches it.)
- Size rule (D6, D9): compare `estimated_vis_GB × WORKING_SPACE_FACTOR`
  (= 2) with the working-path limit. Move the `500` GB limit to a
  constant. Reduce `birli_max_mem_gib` by the same scaled value, because
  `/tmp` is tmpfs (D10). Only apply the factor when `[wsclean] enabled`.
- Tests: band matching (stem pairing, missing solution), command
  building, the size rule with and without WSClean enabled.

## Phase 3: WSClean step and stage-1 move

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
  - Move the index.json + `upload_plot_files()` block so that it is the
    stage-1 publish (no change to what it publishes). Keep the returned
    index dict for stage 2.
  - D4/D15: set `self.job_succeeded = True` after
    `succeed_job_processing()`. In `signal_handler`, if `job_succeeded`
    is set: log a warning, remove the MS dirs in `working_path` (best
    effort, log and continue on error), stop with exit code 0, no DB
    update.
  - After stage 1, if `enabled`: `run_solutions_apply()` →
    `run_wsclean()`. Log a warning on failure.
- Tests: `WSCleanArgs.to_cmdline_args()`, run_wsclean failure (no move,
  readme written), `signal_handler` before and after success.

## Phase 4: stage-2 publish (processor and controller)

- `calvin/plots/index.py`:
  - `populate_index_json_entry()`: accept `.fits` files that contain
    `WSCLEAN_NAME_SUFFIX` and end with one of the
    `WSCLEAN_OUTPUT_SUFFIXES`.
  - `get_file_description()`: add descriptions for each WSClean output
    and for `wsclean_readme.txt` / `solutions_apply_readme.txt`.
  - Add `extend_plot_index(index, files, fit_id, plot_front_end_url)`:
    return a copy of the stage-1 index dict with the new entries added
    and a new `generated_at`.
- Constants: `IMAGES_DIR_SUFFIX = "_images"`.
- `calvin/solution_files.py`: add `upload_image_files(job_output_path,
  upload_path, index)`. Same atomic staging + rename as
  `upload_plot_files()`. Copy (not move) the `WSCLEAN_FITS_GLOB` files,
  so the originals stay in the job folder. Move the WSClean and apply
  readmes. Write the extended `index.json` into the staging dir. Target
  `<plot_upload_path>/<fit_id>_images`.
- `reap_orphaned_staging_dirs()` matches by `STAGING_DIR_PREFIX` only, so
  it already removes `.staging-<fit_id>_images`. Add a test for it.
- `cli/mwax_calvin_controller.py` (D13):
  - `fit_dir_sort_key()`: parse `<fit_id>_images`. Sort it directly
    after `<fit_id>`.
  - `upload_published_fit_dirs()`: for `<fit_id>_images`, use
    `dest_subpath=<fit_id>`. Skip it (debug log) while `<fit_id>` exists
    in the same path.
- Tests: index entries for each WSClean output, `extend_plot_index()`,
  upload copies (not moves) the images, sort key, dest_subpath mapping,
  skip while `<fit_id>` exists.

## Phase 5: wall time (controller)

- D8: replace the hard-coded `"04:00:00"` / `"10:00:00"` in
  `create_sbatch_script()` with config keys `realtime_wall_time_secs` /
  `mwa_asvo_wall_time_secs`. Defaults as constants: realtime 6 h, MWA
  ASVO 12 h. Format the seconds as `HH:MM:SS` for `--time`.
- Update `cfg/calvin_controller*.cfg`.
- Change the comment in `solution_files.py` that says that a job is at
  most 10 hours.
- Tests: `create_sbatch_script()` wall time from config and defaults.

## Phase 6: WSClean parameters

- Greg selects the parameters. `-abs-mem` is required (D10). Add each one
  to `WSCleanArgs`, to the `[wsclean]` config, and to
  `to_cmdline_args()`.
- Check that `-channels-out > 1` names (`-MFS-image.fits`,
  `-0000-image.fits`) still match the Phase 4 filters.

---

## Deferred

- **Separate SLURM job for WSClean (D14).** Look at this again after the
  WSClean run time is measured. Things it must handle:
  - Priority does not preempt a running job. Use SLURM preemption or a
    cap on the number of imaging jobs at the same time.
  - The cal job must copy the calibrated MS from `/tmp` to `/data`
    before it exits. The imaging job can then run on any host and read
    `/shared/data/calvinNN` over NFS, with `-temp-dir` on a local disk.
  - A new CLI entry point or job type, how the job is submitted, and a
    reaper for MS dirs that are left behind.

---

## Test impact

- Replaced (approved, D2): 3 `_uvfits_num_coarse_chans` tests in
  `tests/calvin/test_hyperdrive.py`.
- All other changes add new tests only.
