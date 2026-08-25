# Changelog

# Unpublished

* Removed stale, unused code and dependencies.

# 1.9.12 25-Aug-2026

* calvin_controller: Fixed race condition bugs which can lead to some files not being uploaded or errors when dirs get removed before we're done. Implemented an atomic operaiton instead.

# 1.9.11 25-Aug-2026

* Removed developer specific paths and dependencies from unit tests.

# 1.9.10 25-Aug-2026

* update_calvin_plots_and_index: upgrade existing v1 to v2 to fix height and width swap of pngs

# 1.9.9 24-Aug-2026

* mwacache_archive: Reworked the ingest code to optimise the calls and speed it up, combined with haproxy.cfg changes to eliminate the wait we had before when dealing with different VSS servers not being in sync with each other.

# 1.9.8 21-Aug-2026

* update_calvin_plots_and_index: Fixed bug where a null fitid would cause the process to fail. When no max_gain specified for a fit, pass max-amp of 100 to Hyperdrive solutions-plot. Otherwise let Hyperdrive figure it out.

# 1.9.7 21-Aug-2026

* update_calvin_plots_and_index: Fixed bug where existing file entries were not updated in index.json.

# 1.9.6 21-Aug-2026

* update_calvin_plots_and_index: Fixed so solutions are copied to the upload dir, never moved. (We always keep them)

# 1.9.5 21-Aug-2026

* calvin_processor: correctly describe the original phase and amp plots from hyperdrive in index.json.

# 1.9.4 21-Aug-2026

* update_calvin_plots_and_index: fixed fit_id db column name being wrong when looking up fit_id.
* calvin_processor: correcly indexes the solution FITS files now for index.json on the cal.mwatelescope.org index.json.

# 1.9.3 20-Aug-2026

* update_calvin_plots_and_index.py: Refactor to recursively find solutions from a root dir so the db connection can be shared when dealing with many solutions to process.
* calvin_processor: Correclty insert `phase_outlier_nstd` into the calibration_fits table (instead of NULL)
* calvin_processor: fixed `ensure_system_byte_order()` silently returning wrong values on genuinely byte-swapped input (e.g. real FITS data) -- it relabelled the dtype without actually swapping the bytes. Fixed via `.astype()`. The existing test didn't catch this since its fixture used `.view()`, which doesn't produce a real byte-swap; rewritten accordingly.
* calvin_processor: removed a duplicate, independently broken copy of `ensure_system_byte_order()` in `plot_debug_phase_fits()` that used `ndarray.newbyteorder()` (removed in numpy 2.0, would crash on real byte-swapped input on the pinned numpy version). Not caught by tests since they mock this function out. Now uses the shared (fixed) implementation; added a regression test.
* calvin_processor: fixed several stale docstrings found during review -- `nstd`/`phase_outlier_nstd` wrongly described as "beyond the population mean" (it's the robust median) in five places; `GainFitInfo`/`PhaseFitInfo` had no field docs, and `pol0`/`pol1` are polynomial coefficients, not polarisations, despite the name; a couple of functions listed a stale flagging-pipeline order; one docstring pointed to a since-renamed CALVIN.md section. No behaviour changes.
* CALVIN.md: clarified Step 4's documented default MAD threshold (10.0) is the shipped config value, not the underlying function's own default (5.0).

# 1.9.2 19-Aug-2026

* calvin_processor: `{obs_id}_*_gain_outliers_tiles.png` -- the "{pct}% Good (n_good/n_total)" + per-channel-reason-breakdown summary introduced for Calvin-fully-flagged tiles now shows on every tile except one flagged structurally (metafits/TILES-HDU/BASELINES-HDU, which still has no per-channel data to break down). A clean tile shows "100% Good" with no second line, in black with no border colour change; a partially-flagged tile shows its actual good fraction and reason breakdown in orange, matching its border; a fully flagged tile (structural or Calvin-caused) still shows it in red. `_fully_flagged_channel_summary_text` renamed to `_channel_summary_text` to reflect this broader use.
* calvin_processor: `{obs_id}_*_gain_outliers_tiles.png` -- a Calvin-fully-flagged tile's title now says "FULLY FLAGGED" too (it previously only said "gx/gy amplitude", without indicating fully-flagged status in the title itself; the structural-flag case already had this). Its top-centre message is also richer now: instead of the single-line reason text, it shows a "{pct}% Good (n_good/n_total)" line (the fraction of channels that were never individually flagged before whatever whole-tile promotion swept the rest into NaN too) followed by a breakdown of every distinct per-channel reason actually present, e.g. "100 NaN, 200 above gain cutoff, 22 outside 10 MAD" (using the actual mad_residual_threshold, not a hardcoded value). Structurally-flagged tiles (metafits/TILES-HDU/BASELINES-HDU) are unaffected -- there's no per-channel data to break down for those, so they keep their existing single-line message. `HyperfitsSolutionGroup` gained a `mad_residual_threshold` attribute (set by `flag_amplitude_outliers`), so this label reflects whatever threshold was actually used without needing it threaded through as a new parameter.

* calvin_processor: reinstated `gain_max_cutoff` (config: `gains_cut_off_max`, default 100) as a real, absolute gain-amplitude sanity check -- previously accepted only for calibration_fits table provenance and never actually applied. Runs early in the flagging pipeline (right after enforce_whole_jones_nan, before amplitude-outlier flagging): any (tile, chanblock) entry whose gx or gy amplitude exceeds the cutoff is flagged (whole Jones NaN'd) with the new `ChannelFlagReason.GAIN_MAX_CUTOFF` bit. Catches a failure mode neither `hyperdrive`'s own per-chanblock convergence flag nor `flag_amplitude_outliers`'s per-tile fit can: a tile's solve diverging to a spurious-but-numerically-stable value (e.g. gain amplitudes of 1e10+) that hyperdrive still marks "converged," and that an amplitude fit adapts to (and hides within) rather than flagging. `HyperfitsSolutionGroup.run_flagging_pipeline()` gained a `gain_max_cutoff: float | None = 100.0` parameter; `cli/cal_utils.py` gained `--gain-max-cutoff`/`--no-gain-max-cutoff`.
* calvin_processor: `{obs_id}_intercepts.png` and `{obs_id}_residual.png` now have a fixed, deterministic facet ordering -- rows alphabetical by receiver flavour, columns XX then YY -- instead of whatever order pandas/seaborn happened to produce.
* calvin_processor: `detect_phase_outliers()` now runs *last* in `run_flagging_pipeline()` (after `flag_amplitude_outliers`/`flag_mostly_bad_tiles`, not third), so its result is the truly final, fully-cleaned state. `write_stats_and_debug_plots()` now reuses that result directly instead of recomputing an equally expensive second phase fit purely for reporting -- phase fitting isn't cheap (~2 minutes for a 256-tile observation in testing), so this was a real, not just theoretical, cost. `write_stats_and_debug_plots()`'s `phase_outlier_nstd` parameter now only affects the BEFORE table's annotation; pass the same value to both calls, or the BEFORE/AFTER sections will silently reflect two different thresholds.
* CALVIN.md: Step 3 (gain-magnitude sanity cutoff) through Step 6 (phase-outlier detection, moved from Step 4) renumbered and reordered to match the corrected execution order; updated the DB parameters list and output-files table accordingly.
* calvin_processor: `{obs_id}_residual.png`'s XX and YY columns now share the same y-axis scale (and therefore the same tick decimal formatting) within each receiver-flavour row, so the two polarisations are directly comparable at a glance. Different flavour rows are still independently scaled, since their typical residual magnitudes can genuinely differ. Previously every facet was independently auto-scaled (`sharey=False`), which could show visually different vertical scales for XX vs YY even for the same flavour.
* calvin_processor: `{obs_id}_*_gain_outliers_tiles.png` now visually distinguishes gain-max-cutoff-flagged channels from ordinary amplitude outliers, and colour now tracks severity rather than which check caught a channel: orange for a partial (some-channels) flag, red reserved for a fully flagged tile/border. Amplitude outliers get a black 'x' marker; gain-cutoff divergences get a black '+' -- the two reasons are told apart by marker shape, not colour, since both now share the same orange shading/border when only some channels are affected. Previously amplitude outliers had no shading or marker distinction at all (just a generic black 'x'), and an earlier version of this same change (before release) used red/orange to distinguish the two reasons directly rather than severity -- changed again after review, since red-for-severity/orange-for-warning is a more intuitive convention than red-for-one-specific-check. Also fixed a pre-existing bug found while making this change: the page legend only ever sampled the first tile's two subplots for its entries, so labels like "flagged" (and now "gain cutoff") could silently be missing from the legend whenever that particular tile happened to be clean, regardless of other flagged tiles on the same page -- it now gathers from every subplot on the page, de-duplicated by label.
* calvin_processor: `{obs_id}_*_gain_outliers_tiles.png` no longer hides a Calvin-fully-flagged tile's real data behind a placeholder panel. A tile fully flagged by Calvin itself (e.g. promoted via Step 5's mostly-bad-tile check) still has real pristine data, so it's now plotted normally (same styling as any other tile) with the flag reason overlaid as red text, top-centre, plus a red border -- rather than being hidden. Tiles flagged structurally before Calvin's own analysis ever ran (metafits/TILES-HDU/BASELINES-HDU -- these genuinely have no data, since apply_tile_flags() NaNs them immediately) still get a text-only placeholder, but that text is now also red and top-centre (previously black and vertically centred), and the panel now gets a red border too.
* calvin_processor: three follow-up fixes to `{obs_id}_*_gain_outliers_tiles.png`: (1) the structurally-flagged placeholder's title now says "gx amplitude"/"gy amplitude" (it previously didn't distinguish the two subplots at all -- the Calvin-fully-flagged case already did, unaffected); (2) the black 'x' marker no longer marks every channel of a promoted tile -- only channels with their own genuine per-channel reason, since flag_mostly_bad_tiles NaNs every channel of a promoted tile regardless of whether that specific channel ever individually triggered anything, and marking innocent swept-in channels the same way as genuinely-caught ones was misleading; (3) removed the forced plain-notation y-axis formatting, so a subplot with very large values (e.g. a gain-cutoff divergence) now switches to scientific notation automatically instead of spelling out the full number, while normal-range subplots are unaffected. Checked empirically (not just by inspection) that the acceptance band already renders correctly with real, valid data whenever a promoted tile has at least one surviving channel for flag_amplitude_outliers to fit against -- it's only genuinely blank when the tile has zero valid channels left by that point, which is an inherent data limitation rather than something to paper over with a fabricated fallback.
* calvin_processor: phase-outlier detection (renamed from `flag_phase_outliers` to `detect_phase_outliers`) no longer flags or modifies a tile's calibration solution -- permanent policy change, not a config toggle. Researchers wanted phase-outlier status visible for review without Calvin silently removing the affected tile's solution from the committed FITS file / database. The result is now report-only: shown in `{obs_id}_stats.txt`'s new `Flavor` and `PhOutlier` columns, and in the phase-fit debug plots. `TileFlagReason.PHASE_OUTLIER` is kept defined but is never set by the automatic pipeline anymore.
* calvin_processor: added a shared `annotate_phase_outliers()` helper (mwax_calvin_utils.py) so `detect_phase_outliers`, the stats table, and the debug plots all use the exact same (pol, flavor)-scoped outlier definition and `phase_outlier_nstd` threshold. Previously the plotting path independently recomputed this with a hardcoded `nstd=3.0`, which could silently disagree with the actual configured threshold.
* calvin_processor: `{obs_id}_stats.txt`'s per-tile table now includes the tile's receiver flavour (`Flavor` column, both BEFORE/AFTER sections).
* calvin_processor: `{obs_id}_residual.png` now shades each receiver-flavour/polarisation facet with a band showing that group's phase-outlier reporting range, mirroring the existing amplitude/gain-outlier acceptance bands. `{obs_id}_intercepts.png` and `{obs_id}_rx_lengths.png` are unchanged.
* calvin_processor: removed dead code from mwax_calvin_utils.py left over from the mwax_calvin_plots.py migration -- `debug_phase_fits`, `plot_rx_lengths`, `plot_phase_fits`, `plot_phase_intercepts`, `plot_phase_residual` were unused duplicates of functions already live in mwax_calvin_plots.py. Also removes the now-unused matplotlib/seaborn imports (and the `mpl.use("Agg")` backend-forcing call) that existed only to support them -- every other module importing mwax_calvin_utils.py no longer pays that import cost for nothing.
* calvin_processor: phase-outlier flagging (`flag_phase_outliers`) now scopes its population-outlier threshold per receiver flavour (rx_type) as well as per polarisation, instead of pooling every flavour together. Different flavours (e.g. RRI/SHAO/NI) have measurably different natural chi2dof/sigma_resid distributions, so pooling let a numerically-dominant flavour's spread set a threshold too strict for a naturally-noisier minority flavour and too lenient for a naturally-tighter one. See CALVIN.md's "Phase-outlier flagging" section for a worked example. `reject_outliers()` gained a `group_cols` parameter (default `("pol",)`, preserving prior behaviour for any other callers) to support this.
* calvin_processor: rename of db column name 'phase_outlier_tile_bad_channel_fraction' to 'tile_bad_channel_fraction' (same for config file).
* calvin_processor: Fix to ensure unintended files don't get indexed as "miscellaneous files".

# 1.9.1 18-Aug-2026

* calvin_processor: fixed wrong db column name 'phase_outlier_tile_bad_channel_fraction'.

# 1.9.0 18-Aug-2026

* calvin_processor: huge refactor to consolidate all flagging, outlier detections, fitting and plotting
* cal_utils: added missing "before" hyperdrive plots (was only ever generating "after" plots).
* calvin_processor: Fixed bug where generate_hyperdrive_plots() always returned success even when hyperdrive failed.
* calvin_processor: Renamed process_gain_fits to process_gain_fits_for_db.
* calvin_processor: Suppressed extraneous warnings/log noise (redundant INFO logs, expected RuntimeWarning, scipy LineSearchWarning).
* calvin_processor: Forced headless Agg matplotlib backend (was silently using an interactive backend).
* cal_utils: Added --profile flag to cal_utils for performance profiling.
* calvin_processor: Fixed major bug where fit_phase_line()'s optimizer was silently failing to refine phase fits on every input (finite-difference gradient issue); fixed by supplying an exact analytic gradient. ~3.8x faster phase fitting.
* calvin_processor: Fixed sigma-clip threshold in fit_phase_line() (was comparing mismatched units by coincidence); replaced with robust median+MAD threshold.
* calvin_processor: Vectorized amplitude-outlier flagging (iterative_poly_clip_batch) instead of per-tile fitting. ~1.5-2.6x faster.
* calvin_processor: Parallelized per-page plot rendering across processes instead of one at a time.
* calvin_processor: Overall pipeline wall time reduced from 549s to 197s (2.8x) on real data.
* calvin_processor: Added phase_outlier_nstd in calvin_processor config file and db.

# 1.8.1 12-Aug-2026

* calvin_processor: Fixed bug where solutions files were not being added to the index.json.

# 1.8.0 12-Aug-2026

* calvin_processor: generate gain outlier fit & residual MAD outlier plots.
* calvin_processor: read gain outlier config params from config file.
* calvin_processor: save gain outlier config params from config file to database calibration_fits table.
* calvin_processor: add digital gains to each tile in the TILE HDU of the hyperdrive solutions file(s) (for the beamformer).
* calvin_processor: upload new gain outlier plots and the actual solutions (and the unmodified original if applicable) to s3.
* calvin_processor: Added more info to solution quality txt file report.
* calvin_processor: don't hardcode the gain plots from hyperdrive to a max of 5.
* calvin_processor: don't delete ASVO download from Acacia (no longer needed- only was needed when I was processing a huge backlog).
* Fixed formatting in many files (ruff).
* Fixed broken tests.
* mwax_subfile_distributor/subfile_processor: removed cli argument --mode which told mwax if it was intended to be runnning as bf or corr.
* cal_utils: New cli tool. Now runs the same code as the real calvin_processor (post hyperdrive). NaN's outliers and generates outlier plots and hyperdrive stats and plots.

# 1.7.30 5-Aug-2026

* calvin_controller: handle "no obs locations" errors and don't fail the calibration_request.
* utils and mwax_asvo_helper: some ruff lint fixes.

# 1.7.28,29 12-Jun-2026

* calvin_processor: If <24 coarse channels still insert db records for 24 channels.
* calvin_processor: Fix bug where successful jobs were being updated with "Cancelled: Received SIGTERM" in the calibration_request table.
* mwacache_archive_processor: make rclone copy -> rclone check delay a config file option.

# 1.7.27 12-Jun-2026

* calvin_controller: fix invalid nice value in sbatch script- negatives not allowed in our current slurm config.

# 1.7.26 12-Jun-2026

* calvin_controller: fix invalid nice value in sbatch script.
* calvin_processor: add task name in health packets for untarring.

# 1.7.25 10-Jun-2026

* calvin_processor: No longer splits and copies aocal files.

# 1.7.24 09-Jun-2026

* mwax_subfile_distributor: No longer pass aocal filename to redis / beamformer.

# 1.7.23 03-Jun-2026

* Fixed version code in backport of importlib_metadata.

# 1.7.22 29-May-2026

* calvin_processor: Check for the acacia donwload file existing. If it does't then it means it has expired (we took more than 7 days from ASVO to calibration to start) OR another recent calibration job for this obs occurred and deleted the tar file (in which case fail the job).

# 1.7.21 29-May-2026

* calvin_controller: added requestids to slurm script filename to prevent duplicates.

# 1.7.20 29-May-2026

* calvin_controller: Removed break in loop that was causing false "job not seen by giant squid warnings" too. Added request id to logging to make it more obvious.

# 1.7.19 29-May-2026

* calvin_controller: Introduce a short 5 sec wait after submitting jobs before running giant-squid-list to eliminate false "job not seen by giant squid warnings".

# 1.7.18 29-May-2026

* calvin_controller: added a lock and updated logic to prevent mutation of a list during iteration issue which was causing controller to lose track of jobs.

# 1.7.17 28-May-2026

* calvin_processor: delete tar file from Acacia Projects after successful calibration.

# 1.7.16 27-May-2026

* calvin_processor: fix bug where giant squid could not find the api key env variable.

# 1.7.15 27-May-2026

* calvin_processor: fixed syntax when using proxy.

# 1.7.14 27-May-2026

* calvin_processor: uses haproxy on 127.0.0.1 to proxy to the mwacache servers which proxy to Pawsey so calvin can use the 100G link to download data from MWA ASVO. Proxy currently hardcoded.
* calvin_processor: added download rate to the log message after successfully downloading a tar file.

# 1.7.13 25-May-2026

* calvin: Removed: no_ref tile from the hyperdrive plots command!
* moved all cli commands to the cli subdir.
* Added new CLI to regenerate the calvin plots with the latest hyperdrive plot settings, pull down the index.json, update index.json with new file size, modified and size, then reupload the plots and index.json to S3.

# 1.7.12 21-May-2026

* Fixed logging bug in rclone move in calvin controller

# 1.7.11 21-May-2026

* calvin_controller: reduced verbosity of logging.
* calvin_controller: Fixed bug where ASVO error job was not being removed from job list.
* calvin_controller: Tweaked nice to have greater range.
* calvin_processor: Fixed insert_calibration_fits row bug

# 1.7.10 20-May-2026

* calvin_processor: fixed parsing of `gain_max_cutoff` in config file to include floats.

# 1.7.9 20-May-2026

* calvin_processor: added new db column `gain_max_cutoff` to calibration_fit table.

# 1.7.8 19-May-2026

* calvin: Added missing "Preparing" ASVO state to list of valid states.
* calvin_processor: Disable gains cut off if value is not set or negative.

# 1.7.7 19-May-2026

* calvin_processor: Now sets gains of entire jones matrix to NaN if gain of any part of the matrix is > cut off specified in config file.
* calvin_processor: Hyperdrive amp plot Y max limit set to 5 (was previously unset, and thus used the max gain which can stil be <=cut_off making plots hard to read).

# 1.7.6 18-May-2026

* calvin_processor: Fixed hyperdrive log not being uploaded.
* calvin_processor: Turn down matplotlib logging verbosity.
* calvin: Implemented a very simple retry mechanism with backoff for giant-squid calls.

# 1.7.5 15-May-2026

* calvin_processor: Fixed swapping of width and height in index.json. Bumped index.json version to 2.
* calvin_controller: Fixed debug log which said no files were moved by rclone even when they were.

# 1.7.4 15-May-2026

* calvin_processor: Fixed index.json containing files it shouldn't have!

# 1.7.3 15-May-2026

* calvin_processor: Fixed logging typo.

# 1.7.2 15-May-2026

* calvin_processor: Now sets gains to NaN if gain is > cut off specified in config file.
* calvin_processor: Now solution fitting runs in parallel.

# 1.7.1 14-May-2026

* calvin: Now passing asvo job id to the processor to ensure giant squid knows how to download the obs.

# 1.7.0 13-May 2026

* calvin_processor: add config file items for hyperdrive: extra_args.
* calvin_processor: set number of sources to 1000 (up from 99) in the config file.
* calvin_processor: move phase_fit_niter config item to the processor section.
* calvin_processor: add full hyperdrive command line to calibration_fits table (calibration_command column).
* calvin_controller: add s3 config file items: s3_endpoints, s3_profile, s3_bucket.
  * Now uploads any solution images to S3 so they can be displayed by the Django (WS) server. Once uploaded they are deleted.
* Added standalone util to generate the index.json given a local file path.

# 1.6.24 11-May 2026

* calvin controller: Fixed bug where any MWA ASVO job that had an error would increment the error count every refresh interval.

# 1.6.23 07-May 2026

* Created standalone vdif combiner util.

# 1.6.22 07-May 2026

* calvin_processor: Fixed bug where tar file was not being deleted.

# 1.6.21 06-May 2026

* Ensure "bulk" calvin requests get lowest priority.

# 1.6.20 05-May 2026

* Created stand alone aocal file splitter.

# 1.6.19 01-May 2026

* calvin_processor: Fixed bug with untaring.

# 1.6.15-18 29-Apr-2026

* calvin_processor: If MWA ASVO download has expired, fail the job, but requeue it in the db.

# 1.6.14 29-Apr-2026

* calvin_processor: Minor fix for giant-squid download and untar to ensure tar file is always cleaned up.

# 1.6.13 23-Apr-2026

* calvin_processor: Get giant-squid to do the downloading (not wget) and use the retryable option (--keep-tar) which also means we need to run tar to untar it and delete the tar file when done.

# 1.6.10-12 15-Apr-2026

* calvin_controller: Fixed bug where asvo helper was removing old jobs from a list while iterating over it causing issues.
* calvin_processor: Fixed Unhandled Exception: cannot reshape array of size 524288 into shape (1,128,24,21,4,2)- code was not handling picket fence correctly
* calvin_controller: Give asvo jobs more wall time as they have to download large files from Pawsey

# 1.6.2-9 14-Apr-2026

* calvin_controller: limit number of ASVO jobs pulled in based on comparing the in progress ASVO download jobs to the config file value (max_in_progress_asvo_jobs).
* calvin_controller: send realtime jobs to the priority partition or if full, the gpu partition. Send ASVO jobs to the gpu partition with a higher nice value to lower priority.
* calvin_controller: added 3 new health attributes: 
  * "slurm_queue" (the number of slurm jobs queued or running)
  * "mwa_asvo_calibration_requests_queued" (the number of MWA ASVO calibration requests which have been held back due to the configured mwax in progress job limit)
  * "mwa_asvo_vis_jobs_in_progress" (the number of MWA ASVO visibility jobs for calvin which are not completed, error or cancelled)
* calvin_processor: fixed space in one of the health packet keys.

# 1.6.1 10-Apr-2026

* calvin: Finally populating the gains_quality fields.

# 1.6.0 09,10-Apr-2026

* Added tests for mwax_calvin_utils and mwax_calvin_solutions.
* Lots of fixes for tests
* Removed coloredlogs from dependencies
* conftest.py: Added pytest_configure hook that sets WARNING level on noisy third-party loggers (PIL, matplotlib, scipy, pandas, urllib3, asyncio, numexpr) so only mwax_mover output appears at DEBUG level during test runs
* mwax_calvin_utils:
  * fit_phase_line: now respects the niter parameter (TODO: needs testing!)
  * get_solns: refant index 0 falsy check. if not ref_tile_idx: → if ref_tile_idx is None: so that refant at DataFrame index 0 is handled correctly and not treated as False.
  * weights property: copy before mutating. results = self.results → results = self.results.copy() so the out-of-range filtering (< 0 and > 1e-4) actually takes effect on the weights calculation rather than being silently discarded.
  * GainFitInfo: replace magic number 24. Added module-level constant MWA_NUM_COARSE_CHANS = 24. GainFitInfo.nan() and GainFitInfo.default() now accept n_coarse: int = MWA_NUM_COARSE_CHANS parameter.
  * write_readme_file: rename misleading parameter names
  * ensure_system_byte_order: fix incorrect numpy call. arr.newbyteorder(system_byte_order) → np.frombuffer(arr.tobytes(), dtype=arr.dtype.newbyteorder("=")) — .newbyteorder() is a dtype method, not an ndarray method; frombuffer correctly reinterprets the bytes as native order.
* mwax_calvin_solutions:
  * Fix picket fence calibration gains- solution files are now sorted by their correct channel number.
  * Clean up of logging of NaNs

# 1.5.13-15 02-Apr-2026

* mwacache: If rclone copy or check fails, just return false and it will requeue and retry
* mwacache: Now that we're using haproxy, we need a small gap in time between rclone copy and rclone check since we may use different vss servers for both calls. So we just put in a small hold between copy and check.
* mwacache: Tweaked some of rclone's settings to boost throughput and added a 3 retry loop for rclone check to take into account vss's syncing with each other.
* voltage buffer dump: Added unit tests to check voltage buffer dumps work.
* unit tests: for watcher and priority_watcher.
* watchers: fix for inotify corner cases.

# 1.5.12 01-Apr-2026

* mwacache: now uses haproxy to try all available VSS endpoints.
  * Removed `ceph_endpoints` from mwacache.cfg config file.

# 1.5.11 27-Mar-2026-31-Mar-2026

* subfile_dist: Subfile distributor doesn't exit when there are leftover files.
* utils: refactor to fix a few corner cases.
* All: module docstrings added.
* All: function docstrings added.
* All: updated logging to include module/func names. Removed hardcoded module/func names from logging.
* mwax_bf_vdif_utils: Change to use mwalib >=2.0.3 to get target_name and start_ra and start_dec for voltage beams for the VDIF header.
* README.md: updated.
* Old broken tests removed.
* mwax_wqw_checksum_and_db: refactored to simplify handler logic. Added tests.
* Added FakeMWAXDBHandler to allow tests to mock SQL SELECTs and INSERTs.
* Added hacky check for archive_file_rclone to know if it is running under pytest and if so, just return true. This should be replaced by a proper Mock pattern in the future.
* Added tests for mwax_calvin_controller.

# 1.5.10 20-Mar-2026

* calvin_processor: Fixed AttributeError: 'MWAXDBHandler' object has no attribute 'execute_dml_row_within_transaction'

# 1.5.9 20-Mar-2026

* calvin_processor: Fixed bug which caused the aocal splitting to fail.
* mwacache_archiver: Fixed naming of outgoing workers so you can tell them apart in logs.

# 1.5.8 20-Mar-2026

* More unit tests
* calvin/subfile_distributor: renamed aocal_export_dir to cal_export_dir as it is now for aocal and FITS solutions
* calvin: Now copying in FITS solution files to cal_export dir
* subfile_dist: Added new key / value for solution files to be passed to redis queue
* mwax_calvin_utils: Fixing linter errors

# 1.5.7 19-Mar-2026

* Subfile_distributor: Fixed bug where pausing archiving at the start of a VCS obs breaks things
* Queueworkers: Added a small sleep in the while loop if paused
* VisStatsProcessor: Fixed archiving==1 vs archiving == True
* VDIF: Fixed retrieval of target_name from VOLTAGEBEAMS HDU in metafits

# 1.5.6 19-Mar-2026

* PriorityQueueWorker: fix inconsistent statuses.
* All: fix clean shutdown of psycopg pool.

# 1.5.5 18-Mar-2026

* BfStitchingProcessor: Fix bug where bf_stitching watcher picks up files other than .fil and .vdif
* SubfileDistributor: cfg_corr_archive_destination_enabled now is bool everywhere
* SubfileDistributor: Added extra logging

# 1.5.4 18-Mar-2026

* watchers: Suppress iNotify logging unless critical
* update tests

# 1.5.3 18-Mar-2026

* subfile_distributor: Removed the calibration_destination_enabled option as it wasn't being used.
* all: Removed passing loggers everywhere anti-pattern.
* mwacache_archiver: replaced individual workers, queues and watchers with a watch_queue_worker class.

# 1.5.2 12-Mar-2026

* calvin: Removed the "--max-amp 2" argument from hyperdrive plots

# 1.5.1 11-Mar-2026

* Unit tests and bug fixes for subfile distributor

# 1.5.0 11-Mar-2026

* Refactor to clean up all the watcher, queue and worker mess

# 1.4.4 20-Feb-2026

* archive_processor: Fix to ensure leftover files in bf/incoming don't crash subfile_distributor.

# 1.4.4 20-Feb-2026

* archive_processor: unstitched files that are kept are moved into dont_archive dir

# 1.4.3 20-Feb-2026

* subfile_processor: Fixed bug where redis message was being rpushed instead of lpushed
* archive_processor: Provide config option to keep unstitched files (beamformer)

# 1.4.2 18-Feb-2026

* archive_processor: More debugging for stitching process.
* archive_processor: will not delete beamformer files after stitching (TODO- change to config option)

# 1.4.1 17-Feb-2026

* Dependencies: now requires mwalib >= v2.0.1
* Updated test metafits files to have MWAX_BEAMFORMER mode

# 1.4.0 17-Feb-2026

* Dependencies: now requires mwalib >= v2.0.0
* archive_processor: using mwalib voltagebeam class we can now populate more of the VDIF hdr
* archive_processor: fixed bug where dont_archive queue was of the wrong type
* archive_processor: clean up pre-stitched vdif and file files

# 1.3.9 17-Feb-2026

* archive_processor: Fixed BF stitching when run for a host whos archiving is disabled.

# 1.3.8 16-Feb-2026

* subfile_processor: changed redis queue name it is now: "bfq_mwax26"- so "bfq_" will be read from config file and "mwax26" will be the hostname.

# 1.3.5-7 13-Feb-2026

* archive_processor: fixed handling of dont_archive_bf
* subfile_processor: fixed bug where redis json is single quoted.
* subfile_processor: fixed bug where all redis messages were sent thrice.


# 1.3.4 12-Feb-2026

* subfile_processor: another refactor- using redis to signal the beamformer now. Better testing.

# 1.3.3 11-Feb-2026

* subfile_processor: reworked the named_pipe handling to be MUCH simpler. Also aocal obsid is not worked out from the CALIBDATA / CALOBSID from the metafits file.

# 1.3.0-2 11-Feb-2026

* subfile_distributor: Now has a --mode param (c/C or b/B) to ensure it handles beamformer obs when b or correlator obs when c and ignores the other. Fixed error message.

# 1.2.17 11-Feb-2026

* subfile_processor: fixed int/str bug preventing subfileprocessor from handling beamformer subfiles

# 1.2.15-16 10-Feb-2026

* archive_processor: Added extra debug and fixed duplicate watcher_incoming_vis bug

# 1.2.11-14 06-Feb-2026

* archive_processor (beamformer): when end of an obs detected, stitch together filterbank and vdif files before archiving
* calvin_processor: copy_file_rsync now correctly displays the throughput of rsyncing files from the mwax boxes
* utils: the function which converts GB to GiB now divides instead of multiplies the conversion factor(!)

# 1.2.11-13 02-Feb-2026

* SubfileDistributor: Added better logging for unexpected termination of subprocessor threads

# 1.2.10 02-Feb-2026

* SubfileProcessor: Fixed named pipe so it only opens on first beamformer observation but then stays open

# 1.2.5-9 30-Jan-2026

* SubfileDistributor: fixed endpoint methods and shutdown code for web server

# 1.2.4 30-Jan-2026

* SubfileDistributor: changed status endpoint to GET

# 1.2.3 30-Jan-2026

* SubfileDistributor: switched from HTTPServer web server to Flask

## 1.2.1 - 2 30-Jan-2026

* Bug fix for subflie_distributor circular reference
* Bug fix for logging issue with archive_processor

## 1.2.0 30-Jan-2026

* MWAX Realtime Beamformer support:
  * SubfileProcessor will detect a BF observation and send the subfile filename plus a space plus the aocal filename to a named pipe (/home/mwa/bf_pipe).
  * SubfileProcessor will send EOF to named pipe on shutdown
    * If error writing to named pipe, then die (for now)
    * Beamformer will rename .sub to .free once finished (not mwax_mover)
    * Beamformer will output files for archiving to /voltdata/bf/outgoing    
  * MWAXArchiveProcessor will watch the /voltdata/bf/outgoing and then:
    * Checksum file
    * Insert row into database
    * Send file to mwacache boxes
  * mwacache_archiver will handle VDIF and Filterbank files for archiving
* Old "Beamformer" (aka the Breakthrough listen pipeline) is removed from mwax_mover codebase (mwax_filterbank_processor.py and beamformer mode of subfile_distributor)
* All logging is now to console/stdout instead of named log files:
  * mwax_subfile_distributor
  * mwacache_archiver
* Calvin: increased the wait time for realtime observations to be finished (and database records inserted)

## 1.1.4 29-Jan-2026

* SubfileDistributor: Added more debug and more shutdown code to prevent hanging on shutdown. Fixes #31.

## 1.1.3 27-Jan-2026

* Calvin: Reverted x_gains_pol0, x_gains_pol1, x_gains_sigma_resid (and same for y) back to populating 0s. x and y gains_quality is now reverted back to 1.0 for now.

## 1.1.2 22-Jan-2026

* Calvin: Reverted x_intercept and y_intercept so they get inserted into calibration_solutions rows as radians (they are back to being radains!).

## 1.1.1 22-Jan-2026

* Calvin: Fixed bug where x_intercept and y_intercept were being inserted into calibration_solutions rows as radians not degrees.

## 1.1.0 20-Jan-2026

* Calvin: Remove step that uses phase_diff.txt as it is no longer useful.
* Calvin: Invert the gains since the phases are negated and calculate pol0, pol1, sigma_res and quality for gains.

## 1.0.90 19-Jan-2026

* Calvin: Fix bug where the deletion of stale aolcal files was not picking up any files for deletion

## 1.0.90 17-Jan-2026

* Calvin: Fixed another bug with 24 band picket fence edge case (missed the path)

## 1.0.89 17-Jan-2026

* Calvin: Fixed another bug with 24 band picket fence edge case

## 1.0.88 17-Jan-2026

* Calvin: Fixed Silly bug when getting the rec chan number from the aocal file

## 1.0.87 17-Jan-2026

* Calvin: Fix for when there are already 1 aocal per coarse channel (24-band picket fence)- now the code renames them properly!

## 1.0.86 16-Jan-2026

* Calvin: Fix for when there are already 1 aocal per coarse channel (24-band picket fence)

## 1.0.85 16-Jan-2026

* calvin: Another attempt to fix the splitting of aocal files.
* calvin: Fix for calvin not producing hyperdrive stats txt file
* calvin: Logic change to calvin_controller to always start a new cal job even if it has seen this obs before 

## 1.0.84 16-Jan-2026

* calvin: Another attempt to fix the splitting of aocal files.
* calvin: If receiving a calibration request for one just process, reprocess it anyway.

## 1.0.84 15-Jan-2026

* calvin: Fixed bug where multiple copies of the requestid were being added to the calvin_controller list.

## 1.0.83 15-Jan-2026

* calvin: Fixed bug where aocal splitting was not working.

## 1.0.82 15-Jan-2026

* calvin: provide mechanism to save calibration aocal file to NFS share/dir and ensure no more than 24 hours of files are stored.
  * If config option [processing]->`aocal_export_path` exists in the config file, Hyperdrive created aocal.bin file is split into 1 file per coarse channel and then copied there. If not exist, ignore.
  * If config option [processing]->`aocal_export_path` exists in the config file, then [processing]->`aocal_max_age_hours` is used to determine how old the oldest aocal files can be before removal.
* calvin: fixed bug where the hyperdrive stats txt file was being written to the source code dir instead of the output dir.

## 1.0.81 13-Nov-2025

* Updated deps. Fixed bug where CORR_MODE_CHANGE would cause subfile to not be renamed to .free


## 1.0.80 24-Sep-2025

* calvin_processor: When trying to call the URL to release calibration files, try indefinitely since the mwax boxes may be in the processing of stopping/starting due to change from oversampling to critically sampling and so might be down for ~10-15 mins.

## 1.0.79 23-Sep-2025

* mwax_subfile_distributor: mwax_stats will only be run on visibilities which end in "_000.fits" (we ignore the 001,002, etc).

## 1.0.78 17-Sep-2025

* Calvin processor: More accurately calculates output size of Birli.

## 1.0.77 10-Sep-2025

* Calvin processor: slurm job walltime is now 4 hours (for slow to download/large ASVO jobs).
* Calvin processor: added slurm directive to send a USR1 signal on 5 mins before walltime hit to error the job properly in the database.

## 1.0.76 15-Aug-2025

* Birli now uses the default "auto" pfb_gains instead of "none" for oversampled observations.

## 1.0.75 01-Aug-2025

* Fixed bug in mwax_subfile_processor where the dd command was not correctly trimming subfiles to the TRANSFER_SIZE from the PSRDADA header.

## 1.0.74 22-Jul-2025

* mwax_calvin_controller: fixed SQL to not try to pick up SUN calibrators for realtime calibration

## 1.0.73 22-Jul-2025

* mwax_archive_processor: fixed logic so that C123 calibrator obs don't wait for calibration.

## 1.0.72 11-Jul-2025

* Calvin/mwax_archive_processor: major refactor to support HPC

## 0.23.7 1-May-2025

* Calvin: fixed passing correct passband option to Birli

## 0.23.6 14-Apr-2025

* Calvin: fix for critically sampled observations (which don't have the OVERSAMP keyword in metafits).

## 0.23.5 14-Apr-2025

* Calvin: If obs is oversampled, ensure Birli doesn't flag edge with and does not correct passband.

## 0.23.4 17-Mar-2025

* Changed config of mwax_stats_executable to mwax_stats_dir - now that 2 binaries live in that dir.

## 0.23.0-0.23.3 14-Mar-2025

* Replaced Python packet_stats code with calling rust binary.

## 0.23.0-0.23.2 11-Mar-2025

* Upgraded to use Python 3.12.9
* Added new watcher, queue and worker to handle 2 stage writing of packet stats. Packet stats will get written locally and then in a seperate thread will be moved to vulcan NFS share.
* Added new config file option `log_level` to set logging level. Values are Python logging constants. E.g. DEBUG, INFO, WARNING, ERROR. If not specified, defaults to DEBUG.
* Changed sleep behaviour in subfile_distributor

## 0.22.0 07-Mar-2025

* Upgraded calvin_downloader to use giant-squid 2.x

## 0.21.10 26-Feb-2025

* Added request_checksum_calculation and response_checksum_validation to resolve upload error in versions of Boto3 gt 1.36

## 0.21.5 - 0.21.9  14-Feb-2025

* Merged in ensuring CALIBSRC="SUN" are not treated as calibrators (and not sent to calvins)
* Fixes to numactl usage
* Fixes to display of seconds to round off to 3dp

## 0.21.4  12-Feb-2025

* Implemented copy_subfile_to_disk_dd function.

## 0.21.3  07-Feb-2025

* Fixed Calvin timezone issue.

## 0.21.2 07-Jan-2025

* More logging, plus optimisation to summarise_packet_stats to remove loop and looped function call.

## 0.21.1 07-Jan-2025

* Added more debug to summarise_packet_stats.

## 0.21.0 07-Jan-2025

* Added logging to debug slow VCS when enabling packet stats.

## 0.20.6 05-Dec-2024

* For mwax_subfile_distributor, if a calibrator has "SUN" as the calibrator source, do not treat it as a calibrator.

## 0.20.5 04-Dec-2024

* If packet_stats_dump_dir config value is blank, then do not write packet stats.

## 0.20.4 04-Dec-2024

* Fixed bug where packet stats filename incorrectly included a space.

## 0.20.0 29-Nov-2024

* Added packet map extraction for M&C to mwax_subfile_processor
  * NOTE: changed correlator config file item "mwax mover"->"packet_stats_dump_dir"
* Added support for location=4 (acacia_mwa)
  * NOTE: new mwacache config file section "acacia_mwa"
  * NOTE: changed mwacache config file section: "acacia" is now "acacia_ingest"

## 0.19.5 20-Nov-2024

* Testing: Added more unit tests for mwax_db module.

## 0.19.4 19-Nov-2024

* Minor bug fix: Fixed SQL error in insert_data_file_row- missing deleted column

## 0.19.3 19-Nov-2024

* Minor bug fix: Fixed SQL error in insert_data_file_row

## 0.19.2 19-Nov-2024

* Minor bug fix: Fixed get_data_file_row method, added some more tests

## 0.19.1 19-Nov-2024

* Minor bug fix: Fixed bug where population of hostname was being done after it was needed

## 0.19.0 19-Nov-2024

* First release after merging `calvin_changes3` branch.
* Start of CHANGELOG
