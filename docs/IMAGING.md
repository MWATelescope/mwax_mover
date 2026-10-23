# mwax_mover calvin imaging plan

Status: **implemented, phases 1 to 9.** The calvin host checks (see the
notes of each phase) are still to do before `[wsclean] enabled=1` is used.

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

Status: **implemented**, applied by Greg. Calvin host checks are still
to do (see the end of this phase).

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
- Implementation notes:
  - `EXT_UVFITS` is removed. Nothing else used it.
  - `_run_hyperdrive_one()` gets the band name with
    `os.path.splitext()` of the basename, not with `EXT_MS`. This keeps
    the 3 other existing `run_hyperdrive` tests, which use `.uvfits`
    placeholder names, unchanged.
  - Test edits (approved): the 3 `_uvfits_num_coarse_chans` tests were
    replaced (D2). The patch target in
    `test_run_hyperdrive_worker_count_comes_from_max_hyperdrive_workers`
    changed to `_ms_num_coarse_chans`, with one docstring line.
  - New tests: `_ms_num_coarse_chans`, `run_hyperdrive` band names, MS
    moved whole on failure, MS already in `job_output_path`, and
    `extract_channels_from_ms_filename()`.
  - The slow tests (`test_pipeline.py`, `test_hyperfits_solution*.py`,
    `calvin/plots`) were not run to the end in the sandbox. They do not
    import any changed module. Run the full suite before the commit.
- Check on a calvin host (Greg): Birli MS size against
  `estimate_birli_output_bytes()` (`bytes_per_r_and_i=13` was for
  UVFITS). Hyperdrive solutions from MS are the same as from UVFITS.
  A picket-fence run gives `OBSID_chN-M.ms` names, and hyperdrive
  accepts them.

## Phase 2: calibrated MS

Status: **implemented**. The new function is not called yet. Phase 3 wires it
into the processor.

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
- Note: `extract_channels_from_ms_filename()` returns `None` for
  `*_cal.ms`, because the channel suffix is not directly before `.ms`.
  Do not use it on the calibrated sets. If WSClean needs the channel
  range of a calibrated set, take it from the Birli MS name before the
  rename, or extend the function then.
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
- Implementation notes:
  - Decision: the `[wsclean] enabled` flag is read in this phase (config,
    all four cfg files, `cfg_wsclean_enabled`), because the size rule needs
    it. Phase 3 adds the other `[wsclean]` keys. `enabled` and
    `[hyperdrive] apply_timeout` are required keys, so deployed cfg files
    must be updated.
  - Constants: `CALIBRATED_MS_SUFFIX`, `SOLUTIONS_APPLY_README_SUFFIX`,
    `SECTION_WSCLEAN`, `WORKING_PATH_MAX_GB` (500) and
    `WORKING_SPACE_FACTOR_IMAGING` (2).
  - `run_solutions_apply(..., remove_input_ms)` stops at the first failed
    band (a partial band set is not imaged). It removes the partial
    `_cal.ms` of the failed band. It writes the readme for success and
    failure. `remove_input_ms` removes each Birli MS right after its own
    apply succeeds (D9). The caller sets it from
    `not keep_completed_visibility_files`.
  - Size rule: `estimate_working_space_gb()` in `calvin/birli.py`. The
    processor uses it for both the 500 GB test and the Birli memory
    reduction.
  - New tests only. Existing tests are unchanged.

## Phase 3: WSClean step and stage-1 move

Status: **implemented**.

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

- Implementation notes:
  - The stage-1 publish (index.json + `upload_plot_files()`) was already
    before the cleanup, so it did not move. The index dict is not kept
    yet. Phase 4 keeps it when it needs it.
  - Only `WSCLEAN_NAME_SUFFIX` is added in this phase. `WSCLEAN_FITS_GLOB`
    and `WSCLEAN_OUTPUT_SUFFIXES` are added in Phase 4, where they are
    first used.
  - `[wsclean]` has `enabled`, `binary_path` and `timeout`. All three are
    required keys, so deployed cfg files must be updated. The binary must
    exist only if `enabled=1`.
  - Flow: remove raw visibility files (unless keep) -> `run_imaging()` (if
    enabled) -> `remove_measurement_sets()`. `run_imaging()` runs
    solutions-apply, then WSClean on the calibrated sets. Any failure or
    exception is logged only.
  - `remove_measurement_sets()` does nothing if
    `keep_completed_visibility_files` is set. This also applies on the
    signal path (D15) and the unhandled-exception path.
  - The top-level `except` also checks `job_succeeded`. After success it
    logs, removes the MS dirs and exits 0. It does not fail the job.
  - `-abs-mem` is not set yet (Phase 6). Until then WSClean can use all
    RAM. Do not enable imaging on a calvin host before Phase 6.
  - `tests/tests_common.py`: added a `wsclean` stub to
    `STUB_BIN_FILENAMES`. The Phase 2 test that sets `enabled=1` needs it.
    No existing test was edited.
  - Check on a calvin host (Greg): `wsclean -help` lists `-temp-dir`,
    `-name` and `-no-update-model-required` for the installed version.

## Phase 4: stage-2 publish (processor and controller)

Status: **implemented**.

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

- Implementation notes:
  - Added constants `WSCLEAN_FITS_GLOB`, `WSCLEAN_README_SUFFIX` and
    `IMAGES_DIR_SUFFIX`. `WSCLEAN_OUTPUT_SUFFIXES` was not added.
  - Decision: the index accepts every FITS file that matches
    `WSCLEAN_FITS_GLOB`, not only the five known suffixes. D3 uploads all
    WSClean FITS outputs, so every uploaded file is in the index. The five
    known outputs have their own description. Others (for example
    `-beam.fits`) get "WSClean output image".
  - `find_image_files()` in `solution_files.py` lists what the second
    publish takes: the WSClean FITS files, the WSClean readme and the
    solutions-apply readmes. The processor uses it for the index, and
    `upload_image_files()` uses it for the files, so the two always match.
  - `upload_image_files()` writes `index.json` last and publishes nothing
    if there is no WSClean FITS file (for example after a failed run).
  - Processor: `published_index` holds the first publish's index. It is
    set only if both index generation and `upload_plot_files()` worked.
    `publish_images(fit_id)` does nothing if it is None, because the
    extended index describes the files of both publishes.
    `run_imaging()` now returns True only if WSClean succeeded, and the
    flow calls `publish_images()` only then.
  - Controller: `parse_fit_dir_name()` and `get_s3_subpath()` were added.
    A `<fit_id>_images` dir is left out of the batch (and not counted as
    backlog) while `<fit_id>` exists in the same path. If `<fit_id>` can
    never be uploaded, its images wait too. That is intended (D13).
  - New tests only. Existing tests are unchanged.
  - Not done here: the controller does not check that the S3 index.json is
    older than the one in `<fit_id>_images`. D13 only orders the uploads.

## Phase 5: wall time (controller)

Status: **implemented**.

- D8: replace the hard-coded `"04:00:00"` / `"10:00:00"` in
  `create_sbatch_script()` with config keys `realtime_wall_time_secs` /
  `mwa_asvo_wall_time_secs`. Defaults as constants: realtime 6 h, MWA
  ASVO 12 h. Format the seconds as `HH:MM:SS` for `--time`.
- Update `cfg/calvin_controller*.cfg`.
- Change the comment in `solution_files.py` that says that a job is at
  most 10 hours.
- Tests: `create_sbatch_script()` wall time from config and defaults.

- Implementation notes:
  - The keys are optional, in the `[calvin]` section of the controller
    cfg. If a key is missing, the default constant is used (same pattern as
    `plot_upload_max_fits_per_pass`). No key is required, so deployed cfg
    files still work. The jobs then get 6 h (realtime) and 12 h (MWA ASVO)
    instead of the old 4 h and 10 h. `cfg/calvin_controller*.cfg` list both
    keys with the default values.
  - A wall time of 360 s or less is refused at start-up. Slurm sends
    SIGUSR1 360 s before the wall time (`SLURM_USR1_WARNING_SECS`, now also
    used in the script), so a shorter wall time cannot work.
  - `create_sbatch_script(..., wall_time_secs=None)`: None gives the
    default for the job type. `format_wall_time()` makes the `HH:MM:SS`
    (hours can be more than 24).
  - The wall time is for the whole job. It must cover download (MWA ASVO),
    Birli, hyperdrive, imaging and the publishes. There is no check against
    the `[wsclean] timeout`, which is the only bound on WSClean (D8).
  - The comment in `reap_orphaned_staging_dirs()` is updated. The 24 h
    default still holds, because a staging dir only exists during a publish.
  - New tests only. Existing tests are unchanged.

## Phase 6: WSClean parameters

Status: **implemented**.

- Greg selects the parameters. `-abs-mem` is required (D10). Add each one
  to `WSCleanArgs`, to the `[wsclean]` config, and to
  `to_cmdline_args()`.
- Check that `-channels-out > 1` names (`-MFS-image.fits`,
  `-0000-image.fits`) still match the Phase 4 filters.
- Implementation notes: **the option values, the gridder, the polarization and the image size and
    scale in this list were changed in Phase 9. Read Phase 9 for the current ones.**
  - Greg chose the options and values. Config keys in `[wsclean]` (all
    required when `enabled=1`; they are not read when `enabled=0`):
    `max_mem_gib=830`, `size=3072 3072`, `scale=0.75amin`,
    `briggs_robust=-1.0`, `niter=100000`, `mgain=0.95`, `auto_threshold=1`,
    `auto_mask=5`, `channels_out=4`, and the flags `multiscale`,
    `join_channels`, `log_time`, `verbose` (all 1).
  - Fixed in code: `-gridder idg`, `-idg-mode hybrid`,
    `-link-polarizations i` (constants `WSCLEAN_GRIDDER`,
    `WSCLEAN_IDG_MODE`, `WSCLEAN_LINK_POLARIZATIONS`) and
    `-no-update-model-required`. `-pol` is not passed, so WSClean makes a
    Stokes I image (its default). This follows the WSClean IDG page: IDG
    grids all four instrumental polarizations together, and Stokes I is
    cleaned with `-link-polarizations i`. (Changed after Phase 6 was first
    delivered: it was `-pol xx,yy -join-polarizations`, which gives separate
    XX and YY images.) `-j` is not passed, so WSClean uses all CPU cores.
  - `WSCleanArgs` leaves out any option that is None or False. So the
    Phase 3 tests, which only set `name` and `temp_dir`, are unchanged.
    `wsclean_args_from_config()` fills the options. The processor keeps them
    in `cfg_wsclean_args` and adds `name`, `temp_dir` and `abs_mem_gib` for
    each job with `dataclasses.replace()`.
  - `WSCleanArgs.validate()` runs when the config is read. The processor
    quits if `join_channels` is on and `channels_out` is below 2, or if a
    value is not valid.
  - `-abs-mem` (D10): `[wsclean] max_mem_gib` minus the RAM-backed
    working space (the same value as the Birli reduction, which is 2 x the
    Birli MS estimate when imaging is on). If the working path is on disk
    (500 GB rule), nothing is subtracted. If less than
    `WSCLEAN_MIN_ABS_MEM_GIB` (1) is left, `run_imaging()` skips imaging
    before solutions-apply and the job stays successful.
    `start()` sets `wsclean_abs_mem_gib`. If it is None (only in unit tests),
    `-abs-mem` is not passed.
  - File names: with `-channels-out 4` and Stokes I, WSClean writes names
    such as `<obs>_wsclean-MFS-image.fits` and
    `<obs>_wsclean-0003-dirty.fits`. They match `WSCLEAN_FITS_GLOB` and
    the index descriptions. This is tested.
  - Not covered by a unit test: the code in `start()` that calls
    `calculate_abs_mem_gib()`. The function itself is tested.
  - Check on a calvin host (Greg): `wsclean -help` for the installed version
    lists `-abs-mem`, `-gridder`, `-idg-mode`, `-link-polarizations` (or
    `-link-polarization`), `-temp-dir`, `-auto-mask`, `-log-time` and
    `-verbose`. Check with a short run that no `-pol` plus
    `-link-polarizations i` gives one Stokes I image set. Then run one
    observation with `enabled=1` and measure: WSClean run time (for the
    deferred separate-job decision, D14), peak RAM, and the wall times from
    Phase 5.

---

## Phase 7: preview PNG of the main WSClean image

Status: **implemented**.

- After a successful WSClean run, `run_imaging()` makes a PNG of the main
  image (`calvin/plots/wsclean_preview.py`). The main image is
  `<obs_id>_wsclean-MFS-image.fits`, or `<obs_id>_wsclean-image.fits` if
  WSClean made one output channel. Both are Stokes I (see Phase 6). The PNG
  has the same name with `.png`, in the job output dir.
- The PNG is part of the second publish (`find_image_files()` takes
  `WSCLEAN_PNG_GLOB`). It is moved into `<fit_id>_images` (the FITS files
  are copied). It is in the extended `index.json` with its width, height and
  a description. The first publish moves `*.png` too, but it runs before
  WSClean, so it never takes this file.
- It uses matplotlib (`Figure` and the Agg canvas, no pyplot) and astropy,
  which are already dependencies. The plot has sky axes (from the FITS
  WCS), a colour bar in the FITS `BUNIT`, an asinh stretch, and a colour
  scale from the lowest to the highest value of the image (so the colour bar
  shows the true peak, as DS9 does with min max). The look is set by
  constants at the top of the module.
- A failure to make the PNG is logged only. The images are still published.
  No PNG is made if WSClean failed.

---

## Phase 8: shared imaging step and the local tool

Status: **implemented.**

- `calvin/imaging.py` has `run_imaging_steps()` (find the MS, the `-abs-mem`
  check, solutions-apply, WSClean, the preview PNG) and `ImagingSettings`.
  `MWAXCalvinProcessor.run_imaging()` builds the settings and calls it. The
  processor tests that patched `run_solutions_apply`, `run_wsclean` and
  `make_wsclean_preview` in the processor module now patch them in
  `mwax_mover.calvin.imaging` (20 patch targets; approved by Greg).
- `mwax_dl_conv_cal_image` runs the whole pipeline on one MWA ASVO download,
  with the same imaging code. See `docs/MWAX_DL_CONV_CAL_IMAGE.md`.

---

## Phase 9: revised WSClean defaults, with the image size and scale per observation

Status: **implemented.** The defaults are from a colleague's WSClean script.

- **Gridder and polarization:** `-gridder wgridder` and `-pol i`. The IDG options
  (`-idg-mode`, `-link-polarizations`) are gone. (The script has `-use-wgridder`. That is the
  old name. The WSClean 3.4 and later docs use `-gridder wgridder`, and the `-gridder` option is
  already used here, so `-gridder wgridder` is used.) The output file names do not change
  (Stokes I has no polarization tag), so the index and the preview PNG still work.
- **Config keys in `[wsclean]`** (all required when `enabled=1`): `max_mem_gib=830`,
  `compact_array_baseline_max_length=3500` (metres), `briggs_robust=0`, `niter=1000000`,
  `nmiter=2`, `mgain=0.85`, `multiscale_gain=0.15`, `auto_threshold=5`, `auto_mask=7`,
  `minuv_l=100`, `channels_out=4`, and the flags `multiscale`, `join_channels`, `log_time`,
  `verbose` (all 1). `size` and `scale` are **not** config keys any more. A config that still has them
  works: they are ignored.
- **Image size and pixel scale, for each observation** (`calvin.wsclean.array_image_settings()`):
  - The centre coarse channel is the centre frequency divided by 1.28 MHz, rounded half up. This is the
    metafits `CENTCHAN`, and it is checked against `CENTCHAN` in every test metafits that mwalib can
    open. It is done with whole numbers: in floating point, 163.2 / 1.28 is 127.49999999999999 and
    rounds to the wrong channel, and Python's `round()` rounds half to even.
  - With 184 or more tiles the array is "extended". Otherwise it is "compact" if the longest baseline
    (from all the tile positions, flagged or not, in 3D) is no more than
    `compact_array_baseline_max_length`, and "extended" if it is longer.
  - Compact: `-size 4000 4000`, scale = 1.2 / centre channel. Extended: `-size 8000 8000`,
    scale = 0.6 / centre channel. The scale is in degrees with 7 decimal places (the script cuts the
    string to 8 characters, which gives 7 decimals, but truncates where this rounds).
  - The limits and the sizes are constants in `constants.py` (`WSCLEAN_EXTENDED_MIN_TILES`,
    `WSCLEAN_COMPACT_BASE_SCALE`, ...). Only the baseline limit is config.
- **Processor:** `start()` works the settings out as soon as the metafits is read, and keeps them in
  `wsclean_image_settings`. If that fails, the calibration goes on and imaging is skipped with a warning.
  `run_imaging()` puts the size and scale in the WSClean options. **The tool**
  (`mwax_dl_conv_cal_image`) does the same in its `imaging` step, and shows the result in the summary.
- **Not changed:** `[wsclean] timeout` (4 h) and the Slurm wall times (6 h and 12 h). `-niter 1000000` with
  `-multiscale` on an 8000 x 8000 image may need longer. Measure it first.
- **Check on a calvin host:** that the installed WSClean accepts `-gridder wgridder`, `-minuv-l`,
  `-nmiter` and `-multiscale-gain`, and the run time and memory of an extended-array observation.

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

## Test impact

- Replaced (approved, D2): 3 `_uvfits_num_coarse_chans` tests in
  `tests/calvin/test_hyperdrive.py`.
- All other changes add new tests only.
