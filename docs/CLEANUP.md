# mwax_mover post-restructure cleanup plan

Status: **plan only — no code written yet.** All design questions are
answered; this document is intended to be implementable without further
decisions. Where an item still needs a judgement call it says so explicitly.

This is the working plan for the cleanup pass that follows the
`source_code_restructure` lift-and-shift (see `docs/RESTRUCTURE.md`). The
restructure deliberately made no functional changes and deferred every
refactor opportunity it uncovered; this is the list of those opportunities,
plus everything else found in a full review of the restructured tree.

Findings are against commit `4b70118` on branch `source_code_restructure`
(21,818 lines across 60 modules in `src/`). Re-verify line numbers before
editing — they will drift as phases land.

---

## How to work through this document

### Ground rules

Non-negotiable for the implementing session:

1. **Never push, never open a PR, never touch GitHub.** Deliver work as a
   diff or a `git am` mbox patch for review. Applying and committing is
   Greg's job.
2. **Ask before creating, editing or deleting any file**, and before any
   action affecting another server, computer or system.
3. **Run `ruff check`, `ruff format --check` and `ty` on every touched
   file** before calling a change complete. `ruff check src/ tests/`
   currently passes clean under `select = ["E", "F", "B"]` and must still
   pass clean afterwards.
4. **Google-style docstrings on every function** you add or rewrite — with
   the one exemption recorded in 7.2.
5. **One phase per diff**, with the commit split suggested under each phase.
   Do not bundle phases.
6. **Run the full suite before and after every phase** and report the delta.
   Baseline: **377 passed / 5 deselected**. Any change to that number must
   be explained before handover.
7. **Make no behavioural change not explicitly called for here.** Items with
   observable side effects are marked **[BEHAVIOURAL]** and list the
   verification required.

### Phase ordering

- **Phases 1–2 are subtractive.** Fixing bugs and deleting dead code first
  shrinks the surface later phases must rename. Deleting
  `iterative_poly_clip` before the naming pass is 87 fewer lines to review.
- **Phase 3 (naming) before Phase 4 (constants)**: several literals headed
  for `constants.py` sit in code whose identifiers are being renamed anyway.
- **Phase 5 (`cfg_` rollout) must precede Phase 6 (daemon base class).** The
  shared `health_loop()` reads `self.cfg_health_multicast_*`, which only
  works once all four daemons use the convention.
- **Phase 7 last** — highest risk, lowest urgency, and much easier once
  names and constants have settled.

Phases 1–4 are mutually independent and may be reordered. Phases 5, 6, 7 are
strictly ordered.

---

## Phase 0 — Prerequisites

### 0.1 Add missing `__init__.py` files

**Decided:** the omission was not intentional.

Only `src/mwax_mover/__init__.py` exists; every subpackage is currently an
implicit namespace package. Add one to each of:

```
archive/  beamformer/  calibration/  calvin/  calvin/plots/  cli/
core/  db/  filesystem/  fits/  mwa_asvo/  net/  processors/  queues/
```

Read `src/mwax_mover/__init__.py` (102 bytes) first and match its style.
Keep each to a one-line docstring naming the package's role.

**Do not add re-exports.** Nothing imports from a package root today; adding
re-exports would create new import paths and new cycle risk, and
`tests/test_architecture.py` skips `__init__` files, so re-export edges
would be invisible to the layering guard.

`tests/` already has `__init__.py` throughout — no change there.

### 0.2 Add a `[build-system]` table

**Decided:** use `uv_build`.

```toml
[build-system]
requires = ["uv_build>=0.9.0"]
build-backend = "uv_build"
```

Check the installed uv version and pin `requires` to something at or below
it. `[tool.uv] package = true` is already set. Verify with a clean
`uv build` and confirm the resulting wheel contains all 14 subpackages —
that is the check that 0.1 actually worked.

### 0.3 Declare `numpy`

`numpy` is imported in 11 modules under `src/` but is not in
`[project].dependencies`; it arrives transitively via
astropy/pandas/scipy/matplotlib. Add it explicitly.

**No dependency is unused** — all twelve declared runtime dependencies are
genuinely imported. Do not remove any. The two narrowest are `scipy` (only
`optimize.minimize`, in `calibration/fitting.py`) and `seaborn` (5 calls, all
in `calvin/plots/phases.py`).

### 0.4 Record the baseline

Run the full suite; put the exact numbers in the commit message.

---

## Phase 1 — Bug fixes

Each item is independent. **One commit per numbered item** — Greg may want to
cherry-pick.

### 1.1 `tile.name` returns the wrong value

**Files:** `src/mwax_mover/calvin/hyperdrive.py` lines **164** and **209**

In `_phase_fit_one` (L128) and `_gain_fit_one` (L173):

```python
tile = id_matches.iloc[0]
if tile.flag:
    return None
name = tile.name  # <-- BUG
```

`tile` is a `pandas.Series`. `Series.name` is the **index label**, and it
shadows the `name` column. Verified:

```python
df = pd.DataFrame(
    [("Tile104", 104, ...), ("Tile105", 105, ...)], columns=["name", "id", "flag", "rx", "slot", "flavor"]
)
t = df[df.id == 105].iloc[0]
t.name  # -> 1          (the index label)
t["name"]  # -> 'Tile105'  (what was intended)
```

The value is used only in the warning on the following lines
(`f"Skipping phase fit for {tile_id=:4} {pol} ({name}): {exc}"`), so impact
is log-only — but it prints a row index where a tile name is expected,
making fit-skip warnings much harder to act on.

**Fix:** `name = tile["name"]` at both sites.

**Why not a wider change:** every other consumer already uses bracket
access — `calvin/pipeline.py:152,215,270,279` and `cli/cal_utils.py:125,171`
all do `refant["name"]`. These two lines are the only `.name` *attribute*
accesses on a DataFrame row anywhere in `src/`. All other `.name` hits are
`pathlib.Path.name`, `Thread.name` or `IntFlag.name` and are correct — leave
them.

**Test:** add a unit test for `_phase_fit_one` asserting the warning text
contains the tile name. **Construct the fixture so the index label differs
from the tile name** — if they coincide the test passes vacuously and proves
nothing.

### 1.2 Unify the base64 password predicate

**Decided:** `!= "dummy"` is the valid approach, checking the **`db`** value.

The same decision is currently made four ways:

| Location | Current condition |
|---|---|
| `cli/mwax_subfile_distributor.py:424` | `self.cfg_metadatadb_db != "dummy"` |
| `cli/mwacache_archive_processor.py:450` | `self.mro_metadatadb_host != "dummy"` |
| `cli/mwacache_archive_processor.py:479` | `self.remote_metadatadb_db != "dummy"` |
| `cli/mwax_calvin_controller.py:1118` | `not running_under_pytest()` |
| `cli/mwax_calvin_processor.py:1167` | `not running_under_pytest()` |

**Fix:** all use the `db != "dummy"` form. Two already do; three change.

**Also fix the contradictory comment** at
`cli/mwax_subfile_distributor.py:419` — it reads *"Only read the password as
base64 encoded if host is not dummy"* above code checking `db`.

**Ordering constraint:** the predicate reads `db`, so the `db` read must
precede the `pass` read in each `initialise()`. In
`mwacache_archive_processor.py` the mro block currently reads `host` before
`pass`; verify `db` is assigned at that point after editing.

**[BEHAVIOURAL]** For the two calvin daemons this changes when decoding
happens under test. Their test configs use `pass=dummy` with `db=dummy`, so
the new predicate yields `False` and skips decoding — same outcome as before.
Verify by running the calvin tests, and grep every `.cfg` under
`tests/data/` for a case where `db` is not `dummy` but the password is
stored plain.

Prerequisite for the shared DB config helper in 7.6.

### 1.3 Remove the dead `plot_res` parameter

**File:** `src/mwax_mover/calvin/plots/phases.py`

`plot_phase_residual` (L355) takes `plot_res` as a **required positional**,
documents it at L376 as *"Whether to plot residuals"*, and never reads it in
its 202-line body. Confirmed by vulture at 100% confidence.

The only caller, `plot_debug_phase_fits` (L122), already gates on the same
value:

```python
if plot_residual:
    plot_phase_residual(
        freqs,
        soln_xx,
        soln_yy,
        weights,
        prefix,
        title,
        plot_residual,  # <-- passed, then ignored
        residual_vmax,
        flavor_fits,
        nstd=phase_outlier_nstd,
    )
```

**Fix:** remove the parameter, its `Args:` entry, and the argument at the
call site. Keep the `if plot_residual:` gate — that is the working mechanism.

**Grep for every call site first, including `tests/`.** Because the
parameter is positional, a missed site silently shifts every later argument —
`residual_vmax` would receive the old `plot_res` value. Re-run the phases
tests specifically.

### 1.4 Fix incorrect docstrings and comments

1. **`core/env.py:36`** — `running_under_pytest`'s docstring says it *"Checks
   for the presence of the `PYTEST_CURRENT_TEST` environment variable"*. The
   code is
   `return ("PYTEST_CURRENT_TEST" in os.environ) or ("pytest" in sys.modules)`.
   The second condition is undocumented and fires in most real cases.
   Document both.
2. **`constants.py:1`** — the module docstring names the watch modes as
   `WATCH_DIR_FOR_NEW`, `WATCH_DIR_FOR_RENAME`, `WATCH_DIR_FOR_RENAME_OR_NEW`.
   All are actually `MODE_`-prefixed. Fix the docstring, not the constants —
   the `MODE_` prefix is used at 6 call sites.
3. **`fits/metafits.py:98`** — `get_metafits_value_from_hdu` carries
   `# Read key from primary HDU` above `return hdul[hdu_name].header[key]`.
   Copy-paste from the function above; it reads a *named* HDU.
4. **`cli/mwax_subfile_distributor.py:419`** — covered in 1.2.

### 1.5 Fix typos

| File:line | Typo | Correction |
|---|---|---|
| `queues/watcher.py:136` | `yeild` | `yield` |
| `queues/priority_queue_worker.py:32` | `priorty` | `priority` |
| `cli/mwax_calvin_processor.py:446` | `occured` | `occurred` |
| `cli/mwax_calvin_processor.py:578` | `sucessful` | `successful` |
| `cli/mwax_subfile_distributor.py:843` | `Recieved` | `Received` |
| `tests/fits/test_subfile.py:204` | `non-existant` | `non-existent` |
| all 8 `tests/data/**/*.cfg` | `seperated` | `separated` |

The `.cfg` occurrences are in comments (`# projectids should be comma
seperated.`) so cannot affect behaviour — but re-run the suite anyway, since
several tests compare config contents as strings.

### 1.6 Log-message inconsistency in the archiver pytest branch

**File:** `src/mwax_mover/archive/archiver.py:281-290`

Inside `if running_under_pytest():`, `elapsed` and `check_elapsed` are pinned
to `1.0`, but the same log line calls `get_gbps(size_gigabytes, start_time)`,
which computes throughput from the *real* elapsed time. The message reports
"1.000 seconds" beside a Gbps figure derived from a different duration.

**Fix:** compute the logged throughput from the same pinned `elapsed`, so the
message is internally consistent. Note this interacts with 4.8 — if
`get_gbps` is changed to take `elapsed_seconds` first, this becomes
`get_gbps(size_gigabytes, elapsed)` and fixes itself. **Do 4.8 first and
this item collapses into it.**

Leave the `# TODO: Ugly solution here for testing` at L279 and the branch
itself alone — see "Deliberately out of scope".

---

## Phase 2 — Dead code removal

**Commit split:** 2.1–2.2 (calibration), 2.3 (textwrap), 2.4–2.5 (pragmas).

### 2.1 Remove `iterative_poly_clip`

**Decided:** remove it.

**File:** `src/mwax_mover/calibration/outliers.py:17-101` (87 lines)

Zero callers in `src/` or `tests/`. Only `iterative_poly_clip_batch` is used
(`calvin/hyperdrive.py:1274,1277`).

**Fix:** delete the function, then update every reference or the module
becomes self-contradictory:

- `outliers.py:9` — module docstring: *"iterative_poly_clip(_batch) fits a
  robust, sigma-clipped polynomial..."*
- `outliers.py:35` — references `mwax_calvin_quality._iterative_poly_clip`, a
  module that no longer exists
- `outliers.py:111` — *"Vectorized, batched equivalent of
  iterative_poly_clip"*
- `outliers.py:114` — *"Equivalent to calling iterative_poly_clip(x, Y[t],
  ...) for each tile t"*
- `outliers.py:147` — *"iterative_poly_clip's per-tile return but with an
  added leading..."*
- `outliers.py:253` — comment on `iterative_poly_clip's 'valid = new_valid'`
- `outliers.py:287` — *"iterative-clip approach already used by
  iterative_poly_clip"*
- `tests/calvin/test_hyperdrive.py:747` — comment mention

References at `calvin/hyperdrive.py:1219,1248` are to the *batch* function
and are fine — leave them.

**Important:** these docstrings currently define the batch function's
contract *by reference to* the scalar one. Once the scalar version is gone,
`iterative_poly_clip_batch` needs its behaviour described **on its own
terms**: the per-iteration MAD threshold, the `degree + 2` minimum-valid
guard, the fixed-point exit when `new_valid == valid`, and the `mad == 0`
early break. Do not just delete the cross-references and leave it vague —
this is 164 lines of index arithmetic and the docstring is the only readable
statement of what it does.

### 2.2 Remove `fit_iono`

**Decided:** remove it.

**File:** `src/mwax_mover/calibration/fitting.py`

- L171 — the `fit_iono: bool = False` parameter of `fit_phase_line`
- L188 — its `Args:` entry
- L298-301 — the commented-out `# if fit_iono:` block

Also remove the adjacent commented-out parameters at L172-174
(`# chanblocks_per_coarse: int,`, `# bin_size: int = 10,`,
`# typical_thickness: float = 3.9,`) — same never-implemented ionospheric
placeholder.

Inspect the commented block at L320-322 as well; if it is also ionospheric
residue, remove it. If it is not obviously related, leave it and say so in
the diff.

**Check for callers.** No production caller passes it
(`calvin/hyperdrive.py:166` calls
`fit_phase_line(chanblocks_hz, solns, weights, niter=phase_fit_niter)`), but
grep `tests/`. Removal is safe for keyword callers; a positional caller
passing 5+ arguments would break.

### 2.3 Replace the hand-rolled `textwrap`

**File:** `src/mwax_mover/calibration/fitting.py:550`

A 20-line reimplementation of `textwrap.fill()` that:

- **shadows the stdlib module name.** `calvin/plots/phases.py:36` does
  `from mwax_mover.calibration.fitting import ... textwrap ...`, so a reader
  at `phases.py:533` sees `textwrap(...)` and will assume stdlib.
- is pure text formatting inside a numeric fitting module.
- does not break a word longer than `width`; stdlib does.

**Fix:** delete it. In `calvin/plots/phases.py`, drop `textwrap` from the
L36 import, add `import textwrap`, and change L533 to
`textwrap.fill(f"[{len(best_coeffs)}] {eqn}", width=40)`. Update the
`fitting.py` module docstring at L6 (*"poly_str()/textwrap() format..."*).

**[BEHAVIOURAL]** `textwrap.fill` is not byte-identical: the hand-rolled
version has an off-by-one in its `current_length` accounting (adds
`len(word) + 1` for the space but compares `current_length + len(word) <=
width`), so wrap points may shift by a character. This affects only an
equation annotation inside a debug plot. Check whether
`tests/calvin/plots/test_phases.py` asserts on that string; if so, update the
expected value and **have Greg eyeball a rendered plot** to confirm the new
wrapping looks right.

### 2.4 Remove vestigial `pylint` pragmas

11 occurrences of `# pylint: disable=broad-except`:

```
processors/subfile_incoming.py     (3)    cli/mwax_subfile_distributor.py  (3)
filesystem/files.py                (1)    cli/mwax_calvin_controller.py    (1)
cli/mwacache_archive_processor.py  (1)    cli/mwax_calvin_processor.py     (1)
db/calibration.py                  (1)
```

The project lints with ruff; pylint is not in any dependency group. These
suppress nothing.

**Fix:** delete all 11. Do **not** change the `except Exception:` clauses
they annotate — narrowing exception handling is behavioural and out of scope.

### 2.5 Unify type-checker pragma dialect

The project uses `ty`. Three pragmas use mypy syntax: 1× `# type: ignore`,
1× `# type: ignore[arg-type]`, 1× `# type: ignore[misc]`, alongside
2× `# ty: ignore[unresolved-import]`.

**Fix:** run `ty` on the affected files, determine what each mypy-style
pragma was suppressing, and either convert to `# ty: ignore[rule]` or delete
if `ty` does not flag the line. Report which turned out unnecessary. Also
check the 2× `# noqa:` still suppress an enabled rule.

---

## Phase 3 — Naming

**Every item is a pure rename with no logic change.** One commit each. After
each, grep the whole tree — `src/`, `tests/`, `docs/`, `pyproject.toml`.

> **Decided, and it applies throughout this phase:** **config key names stay
> exactly as they are.** Only Python attribute and identifier names change.
> Nothing in this phase should require editing any `.cfg` file. If a rename
> appears to force a `.cfg` change, stop — you have renamed a key by mistake.
> (The only `.cfg` edits in this whole plan are in 4.3 and 5.2.)

### 3.1 ASVO prefix: `mwax_asvo` / `mwaasvo` → `mwa_asvo`

**Decided:** good idea.

Three spellings coexist. `mwa_asvo` (95 uses) is correct — ASVO is an MWA
service, not an MWAX one. `mwax_asvo` (14 uses) is wrong. `mwaasvo` (4 uses)
is a third variant. `cli/mwax_calvin_controller.py` uses two spellings
*within one class*: `self.mwax_asvo_helper` beside
`self.mwa_asvo_vis_jobs_in_progress`.

**Renames (attributes only):**

| Old | New | Sites |
|---|---|---|
| `self.mwax_asvo_helper` | `self.mwa_asvo_helper` | `mwax_calvin_controller.py` L190, 227, 228, 568, 571, 574, 578, 732, 733, 848, 975, 982, 1043, 1216 |
| `self.mwaasvo_download_obs_timeout` | `self.mwa_asvo_download_obs_timeout` | `mwax_calvin_processor.py` |

**The config key `mwaasvo_download_obs_timeout` in `[downloading]` stays as
it is.** After Phase 5 the attribute becomes
`self.cfg_download_mwa_asvo_obs_timeout` while still reading the key
`mwaasvo_download_obs_timeout` — a deliberate mismatch. Add a short comment
at the read site noting the key name is retained for config compatibility,
so a future reader does not "fix" it.

**Leave alone:** class names `MWAASVOHelper`, `MWAASVOJob`,
`MWAASVOJobState`, and the exceptions in `mwa_asvo/giant_squid.py`.
`MWAASVO` is the correct CamelCase for "MWA ASVO" and is used consistently
(24 uses).

### 3.2 High-priority project list parameters

The same pair of config values travels under five names:

| Name | Uses | Where |
|---|---|---|
| `list_of_vcs_high_priority_projects` | 14 | `filesystem/naming.py`, `queues/` |
| `list_of_correlator_high_priority_projects` | 13 | same |
| `list_of_corr_hi_priority_projects` | 13 | `processors/` |
| `list_of_vcs_hi_priority_projects` | 13 | `processors/` |
| `corr_hi_priority_projects` / `vcs_hi_priority_projects` | 7 each | `queues/watch_queue_worker.py` |
| `cfg_corr_high_priority_correlator_projectids` / `cfg_corr_high_priority_vcs_projectids` | 5 each | `cli/mwax_subfile_distributor.py` |

**Standardise** function parameters and non-`cfg_` attributes on:

```
high_priority_correlator_projects
high_priority_vcs_projects
```

Dropping `list_of_` (the `list[str]` annotation says it) and expanding
`corr`/`hi`. Keep the `cfg_` forms in the CLI classes mirroring the config
key exactly, per the Phase 5 convention:
`cfg_corr_high_priority_correlator_projectids` and
`cfg_corr_high_priority_vcs_projectids`.

Roughly 60 sites across `filesystem/`, `queues/`, `processors/`, `cli/`. Do
it as one commit and lean on `ty` to catch misses.

### 3.3 `watch_path_exts` → `watch_paths_exts`

**File:** `src/mwax_mover/queues/watch_queue_worker.py`

`MWAXWatchQueueWorker` uses `watch_paths_exts` (L13, L97); its sibling
`MWAXPriorityWatchQueueWorker` uses `watch_path_exts` (L285, L288).
`processors/pawsey_outgoing.py:30` uses a third spelling,
`watch_paths_and_exts`.

**Fix:** standardise on `watch_paths_exts` (plural paths — it is a list of
tuples). Update the priority class, its docstring at L285-286, the
`processors/` subclasses, and `pawsey_outgoing.py`.

### 3.4 `queues/` attribute `p` prefix

**File:** `src/mwax_mover/queues/watch_queue_worker.py`

`MWAXPriorityWatchQueueWorker` names members `self.pwatchers`,
`self.pwatcher_threads`, `self.pqueue`, `self.pqueue_worker`,
`self.pqueue_worker_thread`, against `self.watchers`, `self.queue`,
`self.queue_worker` in the sibling. The `p` carries no information the class
name does not already carry.

**Fix:** drop the `p` prefix throughout that class. Confined to one class in
one file, plus tests that reach into these attributes — grep
`tests/queues/` and `tests/cli/`.

The two hierarchies are **not** being unified (see "Deliberately out of
scope"); this is name alignment only.

### 3.5 Misleading function names

| Current | File:line | Problem | New |
|---|---|---|---|
| `get_metafits_values` | `fits/metafits.py:108` | Reads as plural of `get_metafits_value`; actually returns `(is_calibrator, project_id, calib_source)` | `get_calibrator_info` |
| `run_command_ext` | `core/command.py:78` | "ext" conveys nothing; it is the synchronous runner | `run_command` |
| `run_command_popen` | `core/command.py:181` | Named after its implementation | `start_command` |
| `plot_dpi` | `calvin/plots/layout.py:28` | Does not plot; resolves a DPI | `resolve_plot_dpi` |
| `plot_figsize` | `calvin/plots/layout.py:41` | Does not plot; scales a figsize | `scale_plot_figsize` |
| `determine_bucket` | `filesystem/naming.py:309` | Returns a bucket *name*; named unlike the two functions it delegates to | `get_bucket_name_for_location` |

`run_command_ext` has 20+ call sites — its own commit.

Leave the
`determine_bucket` → `get_bucket_name_from_filename` → `get_bucket_name_from_obs_id`
chain intact; the two-line middle layer is called from more than one level.
Mention it in the diff as a possible future collapse.

### 3.6 `update_calsolution_request_*` → `update_calibration_request_*`

**File:** `src/mwax_mover/db/calibration.py`

Four functions use a `calsolution` prefix; two use `calibration_request`.
All six update `public.calibration_request`. "calsolution" is not the table
name.

| Current | New |
|---|---|
| `update_calsolution_request_submit_mwa_asvo_job_status` (L369) | `update_calibration_request_mwa_asvo_job_status` |
| `update_calsolution_request_download_complete_status` (L472) | `update_calibration_request_download_complete_status` |
| `update_calsolution_request_calibration_started_status` (L590) | `update_calibration_request_calibration_started_status` |
| `update_calsolution_request_calibration_complete_status` (L640) | `update_calibration_request_calibration_complete_status` |

These four are also exactly the functions with old-style docstrings — **do
3.6 and 7.1 in one commit.**

### 3.7 `calvin/plots/hyperdrive_plots.py` → `calvin/plots/hyperdrive.py`

The name stutters as `calvin.plots.hyperdrive_plots`; its siblings are
`gains`, `phases`, `index`, `layout`, `stats_table`.

**Decided:** rename the module *and* shorten the functions.

| Current | New |
|---|---|
| `generate_hyperdrive_plots` | `generate_plots` |
| `generate_hyperdrive_plots_for_files` | `generate_plots_for_files` |

`git mv` the module, update importers (grep `hyperdrive_plots`, at minimum
`calvin/pipeline.py`), and move
`tests/calvin/plots/test_hyperdrive_plots.py` → `test_hyperdrive.py`.

**Watch out:** `tests/calvin/test_hyperdrive.py` already exists. The test
tree is a package with `__init__.py` throughout precisely so two test
modules in different directories can share a basename — see the
`[tool.pytest.ini_options]` comment in `pyproject.toml`. The two will
coexist; verify collection after the move.

Because the functions are now `generate_plots`, importers reading
`hyperdrive.generate_plots` need the module imported as a name (not
`from ... import generate_plots`, which loses the context). Check how
`pipeline.py` imports it and keep the call site self-documenting.

Also update `tests/test_architecture.py` `LAYERS` if the module path is
matched by an exact-name entry — it is matched by the `calvin` prefix, so no
change should be needed, but confirm the architecture tests pass.

### 3.8 Greek-letter identifiers in `calibration/fitting.py`

38 uses of `ν` (U+03BD GREEK SMALL LETTER NU) as **identifiers**: `ν`
(parameter of `_phase_fit_hess_inv` at L136 and inside `fit_phase_line`),
`sum_ν` (L160), `sum_ν2` (L161), `dν` (L232).

`ν` and Latin `v` are near-indistinguishable in most editor fonts. A
contributor who types `v` gets a `NameError` if lucky and a silent wrong
answer if `v` is in scope. They are unkeyable without copy-paste and break
plain-text grep.

**Rename identifiers only:**

| Current | New |
|---|---|
| `ν` (parameter) | `freqs_hz` (matches `fit_phase_line`'s own parameter) |
| `sum_ν` | `sum_freqs` |
| `sum_ν2` | `sum_freqs_sq` |
| `dν` | `d_freq` |

Update `_phase_fit_hess_inv`'s `Args:` entry at L154, which documents `ν:`.

**Leave all non-ASCII in docstrings and comments alone.** The superscripts,
`π`, `θ`, `Σ`, `±`, `·`, `×` and em dashes read well and suit the maths —
including the `residual_i(m, c) = wrap(θ_i - m·ν_i - c)` formula at L139,
which keeps its Greek. This item is specifically about identifiers.

`calvin/plots/phases.py`, `calibration/outliers.py`, `calibration/models.py`,
`calvin/solution_files.py`, `fits/subfile.py`, `mwa_asvo/giant_squid.py` and
the two calvin CLI modules also contain non-ASCII — confirm each is prose and
leave prose alone.

---

## Phase 4 — Constants and configuration

**Commit split:** 4.1 alone; then 4.2–4.3; then 4.4–4.6; then 4.7–4.9.

### 4.1 INI section names → `constants.py`

The largest mechanical win: **115 hard-coded section-name literals.**

| Literal | Occurrences |
|---|---|
| `"mwax mover"` | 40 |
| `"mro metadata database"` | 20 |
| `"correlator"` | 12 |
| `"beamformer"` | 8 |
| `"plots upload"` | 6 |
| `"hyperdrive"` | 6 |
| `"giant squid"` | 6 |
| `"birli"` | 6 |
| `"remote metadata database"` | 5 |
| `"calvin"` | 5 |
| `"archiving"` | 1 |

Plus, from the `.cfg` files: `"mwa metadata database"`, `"downloading"`,
`"processing"`, `"acacia_ingest"`, `"acacia_mwa"`, `"banksia"`, and per-host
`"mwacache99"`-style sections.

**Fix:** add a `SECTION_*` block to `src/mwax_mover/constants.py`:

```python
SECTION_MWAX_MOVER = "mwax mover"
SECTION_CORRELATOR = "correlator"
SECTION_BEAMFORMER = "beamformer"
SECTION_CALVIN = "calvin"
SECTION_BIRLI = "birli"
SECTION_HYPERDRIVE = "hyperdrive"
SECTION_GIANT_SQUID = "giant squid"
SECTION_PLOTS_UPLOAD = "plots upload"
SECTION_DOWNLOADING = "downloading"
SECTION_PROCESSING = "processing"
SECTION_ARCHIVING = "archiving"
```

**Skip the three database sections here** — they are consolidated in 5.2.

**Do not change any section string.** Values must stay byte-identical or
every deployed `.cfg` breaks. This item introduces *names* for existing
values, nothing more. Verify by confirming the suite passes with **no `.cfg`
edits** — if a `.cfg` needs editing, something changed that should not have.

Update the `constants.py` module docstring, which currently describes only
the two replacement tokens and the three watch modes (and see 1.4 item 2).

### 4.2 `get_priority` magic integers → `IntEnum`

**File:** `src/mwax_mover/filesystem/naming.py:384-453`

`get_priority` hard-codes nine priority integers (1, 2, 3, 5, 10, 20, 30,
90, 100) in its body **and tabulates the same nine in its own docstring**
(L389-406). Two sources of truth that can drift.

**Fix:** define the enum in `filesystem/naming.py`, beside the existing
`MWADataFileType` and `ArchiveLocation`:

```python
class ArchivePriority(IntEnum):
    """Archive queue priority. Lower dequeues first."""

    METAFITS_OR_PPD = 1
    CALIBRATOR_CORRELATOR = 2
    HIGH_PRIORITY_CORRELATOR = 3
    HIGH_PRIORITY_VCS_BEAMFORMED = 5
    NORMAL_VCS_BEAMFORMED = 10
    HIGH_PRIORITY_VCS_VOLTAGE = 20
    NORMAL_CORRELATOR = 30
    NORMAL_VCS_VOLTAGE = 90
    DEFAULT = 100
```

Replace the docstring table with a pointer to the enum so there is one
definition.

**Also — decided:** `queues/priority_queue_worker.py:140` currently uses a
bare `filename_priority = 99` as the fallback when `current_item[0] is None`.
**Change it to `ArchivePriority.DEFAULT` (100).**

**[BEHAVIOURAL]** That is a real change: 99 → 100. A fallback item now sorts
*behind* rather than *ahead of* an item explicitly assigned the default
priority. Only reachable when a queue tuple has `None` as its priority, which
no current producer does. Note it in the diff; add a test covering the
`None`-priority path if one does not exist.

`IntEnum` members compare and sort as ints and `json.dumps` renders them as
their integer value, so the `PriorityQueue` ordering and the health-multicast
payload are otherwise unchanged. **Verify with a test asserting the
serialised health JSON is byte-identical before and after.**

Note `naming.py` will need `from enum import IntEnum` — it currently imports
`Enum`.

### 4.3 `should_project_be_archived` → config

**File:** `src/mwax_mover/filesystem/naming.py:456-471`

```python
return project_id.upper() != "C123"
```

The docstring already says: *"If this list grows or changes frequently it
should be moved into a configuration file."*

**Decided:** move it to config, default to `C123`, **and** add the key to
each `.cfg` that needs it with `C123` as the value. Belt and braces — no
silent behaviour change either way.

**Config key:** `do_not_archive_projectids` in the **`[correlator]`**
section, comma-separated, read with `read_config_list`. `[correlator]` is
correct because — verified — every caller belongs to
`mwax_subfile_distributor`:

- `processors/checksum_and_db.py:222` (`ChecksumAndDBProcessor`)
- `processors/vis_stats.py:119` (`VisStatsProcessor`)
- `cli/mwax_subfile_distributor.py:751`

`mwacache_archive_processor` does **not** call it (its
`PawseyOutgoingProcessor` has no reference), so its `.cfg` needs no key.

**Signature:** `should_project_be_archived(project_id: str, do_not_archive_projectids: list[str]) -> bool`,
comparing case-insensitively as today (`project_id.upper()` against
upper-cased list entries).

**Default:** when the key is absent, default to `["C123"]` — preserving
today's behaviour for any config that has not been updated.

**Threading:** the value must reach two `processors/` classes. Add a
constructor parameter to `ChecksumAndDBProcessor` and `VisStatsProcessor`,
passed from `MWAXSubfileDistributor.initialise()` as
`self.cfg_corr_do_not_archive_projectids`. That is one layer, so no long
parameter chain.

**`.cfg` files to update** — the 7 subfile-distributor configs, adding
`do_not_archive_projectids=C123` to `[correlator]`:

```
tests/data/config/config.cfg
tests/data/correlator_subfile/correlator_subfile.cfg
tests/data/vcs_subfile/vcs_subfile.cfg
tests/data/beamformer_fil/beamformer_fil.cfg
tests/data/beamformer_subfile/beamformer_subfile.cfg
tests/data/beamformer_vdif/beamformer_vdif.cfg
tests/data/buffer_dump/buffer_dump.cfg
```

Greg will add the key to the production configs.

**Tests to update:** `tests/filesystem/test_naming.py:342-346` calls the
function with one argument; and `tests/processors/test_checksum_and_db.py:389`
depends on the `C123` behaviour.

### 4.4 MWA webservice URLs → constants

Hard-coded four times across two modules:

- `filesystem/naming.py:518-519` (`get_data_files_for_obsid_from_webservice`)
- `filesystem/naming.py:559-560` (`get_data_files_with_hostname_for_obsid_from_webservice`)
- `fits/metafits.py:39-40` (`download_metafits_file`)

All build the same two-host failover list: `http://mro.mwa128t.org/...` and
`http://ws.mwatelescope.org/...`.

**Fix:** put the two host base URLs in `constants.py` —

```python
MWA_WEBSERVICE_HOSTS = ("http://mro.mwa128t.org", "http://ws.mwatelescope.org")
```

— and build the per-endpoint paths at each call site. Constants rather than
config: they are identical in every deployment, and `download_metafits_file`
is reachable from paths where no config object is in hand.

### 4.5 Hard-coded proxy → config

**File:** `src/mwax_mover/cli/mwax_calvin_processor.py:771-772`

```python
env_args = (
    {
        "HTTPS_PROXY": "http://localhost:3128",
        "NO_PROXY": "asvo.mwatelescope.org",
    },
)
```

The comment at L761-763 explains the routing (haproxy → mwacache → squid →
Pawsey, for the 100G link).

**Fix:** two **new** keys in `[downloading]`: `https_proxy` and `no_proxy`.
(New keys are fine — the "leave key names alone" rule in Phase 3 is about
*renaming existing* keys.) Read with `read_optional_config` and fall back to
the current values as defaults, so no deployed `.cfg` breaks and the key can
be added at leisure. Attributes: `self.cfg_download_https_proxy`,
`self.cfg_download_no_proxy`.

Move the explanatory comment into the `.cfg` files as a comment too — that is
where a future operator will look. Add both keys to the two calvin-processor
test configs.

### 4.6 `gigabyte_to_gibibyte` precision

**Decided:** nothing depends on the tolerance; make it exact.

**File:** `src/mwax_mover/core/units.py:39`

```python
return gigabytes / 1.07374  # truncation of 1.073741824
```

**Fix:**

```python
return gigabytes * 10**9 / 2**30
```

Feeds `birli_max_mem_gib` via `cli/mwax_calvin_processor.py:327,337`.
Relative error ~4e-7, so the `int()` result is unchanged for any realistic
memory figure — but check `tests/core/` for an assertion on the exact value.

### 4.7 Repeated numeric literals

| Literal | Count | Replacement |
|---|---|---|
| `3600` | 7 | `SECONDS_PER_HOUR` in `constants.py` |
| `60` (as seconds-per-minute) | 12 | `SECONDS_PER_MINUTE` |
| `200` (HTTP status) | 9 | `http.HTTPStatus.OK` |
| `1000.0` (bytes→GB) | 25 | `core.units.bytes_to_gigabytes` — see 4.8 |

**Inspect every site.** Not all `60`s are seconds-per-minute (some are
timeout defaults, some plot dimensions) and not all `200`s are HTTP statuses
— only the Flask return tuples at
`cli/mwax_subfile_distributor.py:823,828,833,838,896` and
`cli/mwax_calvin_processor.py:566`. Replace only genuine matches; leave the
rest and say so.

### 4.8 `get_gbps` takes elapsed seconds

**Decided:** change the signature.

**File:** `src/mwax_mover/core/units.py:69`

```python
def get_gbps(size_gigabytes: float, start_time: float) -> float:
    elapsed_seconds = time.time() - start_time
    return gigabytes_to_gigabits(size_gigabytes) / elapsed_seconds if elapsed_seconds > 0 else 0.0
```

Computing elapsed internally silently constrains every caller to
`time.time()`, and `cli/mwax_calvin_processor.py:755` uses
`time.monotonic()`.

**New signature:**

```python
def get_gbps(size_gigabytes: float, elapsed_seconds: float) -> float:
    """..."""
    return gigabytes_to_gigabits(size_gigabytes) / elapsed_seconds if elapsed_seconds > 0 else 0.0
```

This makes it clock-agnostic and lets `import time` be dropped from
`core/units.py` if nothing else there needs it (check `is_int` and the
conversions — they do not).

**Update the call sites:**

- `archive/archiver.py:287` and `:394` — both already have an `elapsed`
  in scope from `time.time()`; pass it. **`:287` is inside the pytest branch
  where `elapsed` is pinned to `1.0`, which is precisely the fix for 1.6** —
  passing `elapsed` makes the log line self-consistent automatically.
- `cli/mwax_calvin_processor.py:783-786` — replace the inline
  `(file_size_bytes * 8) / (elapsed_seconds * 1_000_000_000)` with
  `get_gbps(bytes_to_gigabytes(file_size_bytes), elapsed_seconds)`, and the
  inline `file_size_bytes / 1_000_000_000` with
  `bytes_to_gigabytes(file_size_bytes)`.
- `archive/archiver.py:74,148` — the bare `1000.0` clusters; use
  `bytes_to_gigabytes`.

**Check `tests/core/test_units.py`** — any existing `get_gbps` test passes a
`start_time` and needs rewriting. This is the one signature change in Phase 4;
`ty` will find every caller.

### 4.9 Single `EXIT_FAILURE` constant

**Decided:** none of the current exit codes carry meaning. Introduce one
non-zero `EXIT_FAILURE` and use it for every aborting `sys.exit()`.

Current inventory in `src/`:

| Code | Count |
|---|---|
| `sys.exit(1)` | 58 |
| `sys.exit(-1)` | 12 |
| `sys.exit(0)` | 6 |
| `sys.exit()` (bare) | 3 |
| `sys.exit(2)` | 2 |
| `sys.exit(3)` | 1 |
| `sys.exit(-3)` | 1 |
| `sys.exit(-10)` | 1 |

Plus `request_fatal_shutdown(4, ...)` at
`cli/mwacache_archive_processor.py:174` and
`cli/mwax_subfile_distributor.py:1115`.

**Fix:** add to `constants.py`:

```python
# Process exit code for any abnormal termination. The specific value carries
# no meaning to any caller; only zero vs non-zero is significant.
EXIT_FAILURE = 1
```

Replace **every non-zero literal** — `1`, `-1`, `2`, `3`, `-3`, `-10`, and
the `4` in both `request_fatal_shutdown` calls — with `EXIT_FAILURE`. Also
`stop(exit_code=-1)` in `cli/mwax_calvin_processor.py`'s `signal_handler`.

**Leave unchanged:**

- `sys.exit(0)` (6 sites) and bare `sys.exit()` (3 sites) — success paths.
- **`sys.exit(processor.fatal_exit_code)`** at
  `cli/mwacache_archive_processor.py:561` and the equivalent in
  `mwax_subfile_distributor`. This is the *variable*, which is `0` on clean
  shutdown and `EXIT_FAILURE` when a worker requested a fatal shutdown.
  Replacing it with the constant would make clean shutdowns exit non-zero.
- `self.fatal_exit_code: int = 0` initialisation and the `if
  self.fatal_exit_code:` truthiness checks — these rely on `EXIT_FAILURE`
  being non-zero, which it is.

**[BEHAVIOURAL]** `-1` becomes `255` at the shell today; after this it
becomes `1`. Greg has confirmed nothing depends on the values, so this is the
intended normalisation — but state it in the diff. Grep for any systemd unit,
Slurm script or Ansible task in adjacent repos that tests an exit code, and
report what you find rather than assuming.

---

## Phase 5 — `cfg_` prefix rollout and database consolidation

Done together because both rewrite the config-reading blocks of the same four
`initialise()` methods; separately means editing the same ~500 lines twice.

**Must complete before Phase 6.**

### 5.1 Roll out the `cfg_` prefix

**Decided:** use the `cfg_` prefix everywhere. Abbreviation table confirmed.
`[mwax mover]` keys take **no** section abbreviation.

`cli/mwax_subfile_distributor.py` already uses `self.cfg_<section>_<key>` for
all 216 config-derived attributes; the other three use plain names.

**Convention:** `self.cfg_<section_abbrev>_<config_key>`, and for
`[mwax mover]` simply `self.cfg_<config_key>`.

| Section | Abbrev |
|---|---|
| `mwax mover` | *(none)* |
| `correlator` | `corr` |
| `beamformer` | `bf` |
| `calvin` | `calvin` |
| `birli` | `birli` |
| `hyperdrive` | `hyperdrive` |
| `giant squid` | `gs` |
| `plots upload` | `plots` |
| `downloading` | `download` |
| `processing` | `proc` |
| the consolidated MWA database (5.2) | `db` |

**Full mapping.** Verify against the tree before applying; several
attributes are also pre-declared with type annotations in `__init__`.

**`cli/mwacache_archive_processor.py`** — 10 non-DB attributes:

```
[mwax mover]
  self.metafits_path                        -> self.cfg_metafits_path
  self.archive_command_timeout_sec          -> self.cfg_archive_command_timeout_sec
  self.concurrent_archive_workers           -> self.cfg_concurrent_archive_workers
  self.rclone_check_wait_secs               -> self.cfg_rclone_check_wait_secs
  self.health_multicast_interface_name      -> self.cfg_health_multicast_interface_name
  self.health_multicast_ip                  -> self.cfg_health_multicast_ip
  self.health_multicast_port                -> self.cfg_health_multicast_port
  self.health_multicast_hops                -> self.cfg_health_multicast_hops
  self.high_priority_correlator_projectids  -> self.cfg_high_priority_correlator_projectids
  self.high_priority_vcs_projectids         -> self.cfg_high_priority_vcs_projectids
```

**Decided — one exception to the convention.**
`self.health_multicast_interface_ip` **keeps its plain name and does not get
a `cfg_` prefix**, in all four daemons. It is *derived* at runtime from
`health_multicast_interface_name` (via `net.multicast.get_ip_address`), not
read from the config file, and the `cfg_` prefix means "this value came
straight from config".

So the four health attributes split:

```
self.cfg_health_multicast_interface_name   <- config key
self.cfg_health_multicast_ip               <- config key
self.cfg_health_multicast_port             <- config key
self.cfg_health_multicast_hops             <- config key
self.health_multicast_interface_ip         <- DERIVED, no cfg_ prefix
```

Phase 6's `health_loop` reads all five, four prefixed and one not. Add a
one-line comment where `health_multicast_interface_ip` is assigned, noting
it is derived and therefore deliberately unprefixed, so a future reader does
not "fix" the inconsistency.

This is the general rule, not a special case: **only values read directly
from the config file take the `cfg_` prefix.** Apply the same test to any
other derived attribute you meet during the rollout.

**`cli/mwax_calvin_controller.py`** — 15 non-DB attributes:

```
[mwax mover]
  self.log_path                             -> self.cfg_log_path
  self.health_multicast_*                   -> self.cfg_health_multicast_*   (4 keys)
[calvin]
  self.script_path                          -> self.cfg_calvin_script_path
  self.check_interval_seconds               -> self.cfg_calvin_check_interval_seconds
[giant squid]
  self.giant_squid_binary_path              -> self.cfg_gs_binary_path
  self.giant_squid_list_timeout_seconds     -> self.cfg_gs_list_timeout_seconds
  self.giant_squid_submitvis_timeout_seconds-> self.cfg_gs_submitvis_timeout_seconds
  self.mwa_asvo_longest_wait_time_seconds   -> self.cfg_gs_mwa_asvo_longest_wait_time_seconds
  self.mwa_asvo_outage_check_seconds        -> self.cfg_gs_mwa_asvo_outage_check_seconds
[plots upload]
  self.s3_profile                           -> self.cfg_plots_s3_profile
  self.s3_bucket                            -> self.cfg_plots_s3_bucket
  self.plot_upload_paths                    -> self.cfg_plots_upload_paths
  self.plot_upload_interval_secs            -> self.cfg_plots_upload_interval_secs
  self.plot_upload_max_fits_per_pass        -> self.cfg_plots_upload_max_fits_per_pass
```

**`cli/mwax_calvin_processor.py`** — 26 non-DB attributes:

```
[mwax mover]
  self.log_path, self.health_multicast_*    -> self.cfg_*  (5 keys)
[birli]
  self.birli_binary_path                    -> self.cfg_birli_binary_path
  self.birli_timeout                        -> self.cfg_birli_timeout
  self.birli_max_mem_gib                    -> self.cfg_birli_max_mem_gib
  self.birli_freq_res_khz                   -> self.cfg_birli_freq_res_khz
  self.birli_int_time_res_sec               -> self.cfg_birli_int_time_res_sec
  self.birli_edge_width_khz                 -> self.cfg_birli_edge_width_khz
[hyperdrive]
  self.hyperdrive_binary_path               -> self.cfg_hyperdrive_binary_path
  self.hyperdrive_timeout                   -> self.cfg_hyperdrive_timeout
  self.num_sources                          -> self.cfg_hyperdrive_num_sources
  self.source_list_filename                 -> self.cfg_hyperdrive_source_list_filename
  self.source_list_type                     -> self.cfg_hyperdrive_source_list_type
[giant squid]
  self.giant_squid_binary_path              -> self.cfg_gs_binary_path
[downloading]
  self.download_retries                     -> self.cfg_download_retries
  self.download_retry_wait                  -> self.cfg_download_retry_wait
  self.mwaasvo_download_obs_timeout         -> self.cfg_download_mwa_asvo_obs_timeout   (key unchanged; see 3.1)
  self.realtime_download_file_timeout       -> self.cfg_download_realtime_file_timeout
  (+ cfg_download_https_proxy, cfg_download_no_proxy from 4.5)
[processing]
  self.job_input_path                       -> self.cfg_proc_job_input_path
  self.job_output_path                      -> self.cfg_proc_job_output_path
  self.temp_working_path                    -> self.cfg_proc_temp_working_path
  self.cal_export_path                      -> self.cfg_proc_cal_export_path
  self.cal_export_max_age_hours             -> self.cfg_proc_cal_export_max_age_hours
  self.keep_completed_visibility_files      -> self.cfg_proc_keep_completed_visibility_files
  self.phase_fit_niter                      -> self.cfg_proc_phase_fit_niter
  self.phase_outlier_nstd                   -> self.cfg_proc_phase_outlier_nstd
  self.plot_front_end_url                   -> self.cfg_proc_plot_front_end_url
  self.plot_upload_path                     -> self.cfg_proc_plot_upload_path
```

**`cli/mwax_subfile_distributor.py`** — already conformant apart from its DB
attributes (5.2). Its existing `cfg_metadatadb_*` become `cfg_db_*`.

**Method:** per daemon, rename in `initialise()`, in `__init__()` where the
attribute is pre-declared, and at every read site in the class. `ty` catches
missed reads. Also grep `tests/` (several set or assert these attributes
directly) and `processors/` — note the processor **constructor parameter**
names stay as they are; only `self.cfg_*` attribute names change.

**No config key is renamed in this item.**

### 5.2 Consolidate the database sections

**Decided:** they are all the same database; use one section and one handler.
Use the new name directly — no transitional fallback. Greg will update the
production `.cfg` files.

Three section names exist today:

| Section | Read by |
|---|---|
| `mwa metadata database` | `mwax_subfile_distributor` |
| `mro metadata database` | `mwacache_archive_processor`, `mwax_calvin_controller`, `mwax_calvin_processor` |
| `remote metadata database` | `mwacache_archive_processor` |

**Target:** one section, **`[mwa database]`**, with keys `host`, `db`,
`user`, `pass`, `port`. Add `SECTION_MWA_DATABASE = "mwa database"` to
`constants.py`.

Attributes: `self.cfg_db_host`, `self.cfg_db_name` (reading the key `db` —
`cfg_db_db` reads badly), `self.cfg_db_user`, `self.cfg_db_pass`,
`self.cfg_db_port`.

**Merge the two handlers in `mwacache_archive_processor`.**

**Decided:** only one database is needed. The current split is one read-only
connection and one read/write connection to the same database, and can
collapse to a single handler.

Today:

- `self.remote_db_handler` — the **read** path;
  `processors/pawsey_outgoing.py:106` calls
  `get_data_file_row(self.remote_db_handler_object, item, val.obs_id)`
- `self.mro_db_handler` — the **write** path;
  `processors/pawsey_outgoing.py:167` calls
  `data_files.update_data_file_row_as_archived(self.mro_db_handler_object, ...)`

Both are opened (`mwacache_archive_processor.py:119,123`), closed
(`:227-231`) and passed separately into `PawseyOutgoingProcessor.__init__`
(`processors/pawsey_outgoing.py:37-38`, wired at `:506-507`).

**Changes required:**

1. `cli/mwacache_archive_processor.py`
   - L96-100: replace `self.mro_db_handler` / `self.remote_db_handler`
     declarations with a single `self.db_handler: MWAXDBHandler`
   - L119,123: one `start_database_pool()` call
   - L227-231: one `close()`
   - L297-310: `initialise(..., override_mro_db_handler,
     override_remote_db_handler)` becomes `initialise(..., override_db_handler)`,
     matching the other three daemons. Update the docstring.
   - L440-500 (approx): one block of five config reads, one `MWAXDBHandler`
     construction
   - L506-507: pass one handler to `PawseyOutgoingProcessor`
2. `processors/pawsey_outgoing.py`
   - L37-38: replace the two parameters with one `db_handler_object`
   - L52-53: one `Args:` entry
   - L72-73: one attribute
   - L106 and L167: both use `self.db_handler_object`
3. `tests/cli/test_mwacache_archive_processor.py` — passes both overrides;
   update to one. Check `tests/tests_fakedb.py` for anything assuming two.

**`.cfg` migration.** Replace the section(s) in all 8 affected test configs
with a single `[mwa database]`:

```
tests/data/mwacache_archive_processor/mwacache_archive_processor.cfg   (both sections -> one)
tests/data/mwax_calvin_controller/mwax_calvin_controller.cfg
tests/data/mwax_calvin_processor/mwax_calvin_processor.cfg
tests/data/mwax_calvin_processor/mwax_calvin_processor_no_gains_cutoff.cfg
tests/data/config/config.cfg
tests/data/correlator_subfile/correlator_subfile.cfg
tests/data/vcs_subfile/vcs_subfile.cfg
tests/data/buffer_dump/buffer_dump.cfg
```

Check the three `beamformer_*.cfg` files too — grep each for a database
section before assuming it has none.

**Highest operational risk in this plan.** Call it out prominently in the
diff and list every `.cfg` key change explicitly, so Greg has a checklist for
the production configs. A missed production config fails at daemon startup
with `NoSectionError`.

---

## Phase 6 — Shared daemon base class

**Requires Phase 5 complete.**

**Decided:** the base class lives at **`src/mwax_mover/processors/daemon.py`**.
`cli/` is reserved for actual CLI executable scripts, and `core/` cannot hold
it (see 6.3). **No change to `tests/test_architecture.py` is required** —
verified empirically; read 6.3 before writing any code.

### 6.1 Analysis

| Class | File | Has `workers` | `request_fatal_shutdown` | `sleep` | Health method |
|---|---|---|---|---|---|
| `MWACacheArchiveProcessor` | `cli/mwacache_archive_processor.py` | yes | yes | no | `health_handler` |
| `MWAXCalvinController` | `cli/mwax_calvin_controller.py` | no | no | yes | `health_loop` |
| `MWAXCalvinProcessor` | `cli/mwax_calvin_processor.py` | no | no | yes | `health_loop` |
| `MWAXSubfileDistributor` | `cli/mwax_subfile_distributor.py` | yes | yes | no | `health_handler` |

**`request_fatal_shutdown` — fully unifiable.** The two implementations
(`mwacache:188`, `subfile_distributor:1129`) are logically identical
(first-caller-wins; sets `fatal_exit_code`, `fatal_reason`, `running`); only
docstring wording differs. Move to the base class verbatim, keeping the
better of the two docstrings.

The controller and processor do not have it. They do have background threads,
so arguably should — but **do not add it in this phase.** Adding a shutdown
path to two daemons that lack one is a behavioural addition unrelated to
deduplication. Note it as a follow-up.

**`signal_handler` — unifiable for three of four.** `mwacache:285` and
`controller:897` are byte-identical. `subfile_distributor:1063` differs only
in the log message (includes `len(self.workers)`). `processor:1069` is a
different algorithm: it handles `SIGUSR1` (Slurm walltime), records job
failure via `fail_job_processing` / `fail_job_downloading` depending on
`self.data_downloaded`, and calls `self.stop(exit_code=EXIT_FAILURE)`.

Put the simple version in the base; `MWAXCalvinProcessor` **overrides** it.
Do not try to accommodate the processor's version with hooks.

For `subfile_distributor`'s worker-count message: give the base a
`shutdown_log_detail() -> str` hook returning `""` by default, or accept the
tiny loss of detail. **Prefer the hook** — the worker count is genuinely
useful at shutdown.

**`health_handler` / `health_loop` — unifiable, two hooks.** All four bodies
are the same loop: `while self.running` → build status → JSON-encode →
`send_multicast(...)` → sleep 1s. Differences:

1. Attribute names — resolved by Phase 5.
2. The controller refreshes
   `self.mwa_asvo_vis_jobs_in_progress = self.mwa_asvo_helper.get_in_progress_asvo_job_count()`
   before building status → hook `before_health_send()`, default no-op.
3. `time.sleep(1)` (mwacache, subfile_distributor) vs `self.sleep(1)`
   (controller, processor) → use `self.sleep(1)` uniformly.
4. `logger.warning(f"...{catch_all_exception}")` (three) vs
   `logger.exception(...)` (processor) → standardise on `logger.exception`;
   it preserves the traceback and clears the `TRY400` findings.

**Name it `health_loop`** — it is a loop, not a callback. Also fix the stale
`"health_handler:"` prefix inside the log message, which survives in all four
copies and no longer matches the method name.

**`sleep` — unifiable, one hook.** `controller:1246` and `processor:1616` are
identical apart from the controller's slurm-queue refresh every
`SLURM_REFRESH_INTERVAL` seconds. Put the interval-chopping loop in the base
with a `during_sleep_interval()` hook (default no-op) called once per
interval; the controller overrides it to refresh `self.slurm_queue_size`.

Hoist `SECS_PER_INTERVAL = 5` to a module-level constant — it is currently a
`SCREAMING_CASE` local in both copies, which reads oddly.

**[BEHAVIOURAL]** This gives mwacache and subfile_distributor an
interruptible sleep they lack today. Immaterial for the 1-second health
sleep. **Do not convert their other `time.sleep` calls in this phase** — list
them for Greg instead.

**`get_status` — partially unifiable.** Five keys are common to all four:

```
unix_timestamp   time.time()
process          type(self).__name__
version          version.get_mwax_mover_version_string()
host             self.hostname
running          self.running
```

Divergences:

- `cmdline` (`" ".join(sys.argv[1:])`) — mwacache and subfile_distributor only
- `workers` list — mwacache and subfile_distributor only; the other two
  return `{"main": ...}` with no `workers` key
- extras: controller adds 9 counters (`slurm_queue`,
  `mwa_asvo_calibration_requests_queued`, `mwa_asvo_vis_jobs_in_progress`,
  `realtime_slurm_jobs_submitted`, `mwa_asvo_slurm_jobs_submitted`,
  `giant_squid_errors`, `mwa_asvo_errors`, `database_errors`,
  `slurm_errors`); processor adds 5 (`slurm_job_id`, `obs_id`, `job_type`,
  `task`, `requests`); subfile_distributor adds 2 (`mode`, `archiving`)

**Design:** base `get_status()` builds the five common keys **plus
`cmdline`**, merges `self.get_extra_status()`, and adds a `workers` key only
when `self.get_worker_status()` returns non-`None`.

**[BEHAVIOURAL]** **Decided:** consumers of the health JSON exist but are
easy to update, and normalising is fine. So `cmdline` is added to the
controller and processor payloads. Document the new payload shape in the diff
so the consumers can be updated. Keep `workers` conditional — the controller
and processor have no `self.workers`, and emitting an empty list would be
misleading rather than merely new.

**`initialise_from_command_line` — unifiable for three of four.**
`mwacache:521`, `controller:1222` and `subfile_distributor:176` are identical
apart from the `parser.description` string: build an `ArgumentParser`, add
`-c/--cfg` (required), parse, call `self.initialise(config_filename)`.

`processor:1493` is 122 lines: seven arguments, `is_int` validation on three,
`print()`-based diagnostics, `sys.exit(EXIT_FAILURE)` on bad input.

**Design:** the base provides a concrete `initialise_from_command_line()`
reading a class-level `DESCRIPTION: str`. `MWAXCalvinProcessor` **overrides**
it entirely. Do not parameterise the processor's version into the base.

**`stop` — not unifiable.** mwacache stops workers and closes handlers;
controller sets a stop event and `ready_to_exit`; processor takes
`exit_code`, logs success/failure and calls `sys.exit()`;
subfile_distributor stops Flask first. Leave abstract.

The processor's `stop(self, exit_code: int = 0)` is signature-incompatible
with the other three. Declare the abstract method as `stop(self) -> None` and
let the processor widen it with a defaulted parameter — that is
Liskov-compatible (callers passing no argument still work) and `ty` accepts
it. If `ty` objects, report the exact error rather than loosening to
`*args, **kwargs`.

**`start` — not unifiable.** 79 / 69 / 346 / 55 lines, all different. Leave
abstract.

### 6.2 Suggested implementation

`src/mwax_mover/processors/daemon.py`:

```python
class MWAXDaemon(ABC):
    """Shared lifecycle for the four long-running mwax_mover daemons."""

    DESCRIPTION: str = ""  # subclass sets; used by initialise_from_command_line

    # --- concrete, shared ---
    def request_fatal_shutdown(self, exit_code: int, reason: str) -> None: ...
    def health_loop(self) -> None: ...
    def sleep(self, seconds: float) -> None: ...
    def signal_handler(self, signum, frame) -> None: ...
    def get_status(self) -> dict: ...
    def initialise_from_command_line(self) -> None: ...

    # --- hooks with defaults ---
    def before_health_send(self) -> None:
        """Called once per health iteration before the status is built."""
        return None

    def during_sleep_interval(self) -> None:
        """Called once per sleep interval by sleep()."""
        return None

    def get_worker_status(self) -> list[dict] | None:
        """Per-worker status for the "workers" key, or None to omit it."""
        return None

    def shutdown_log_detail(self) -> str:
        """Extra detail for the signal-handler shutdown message."""
        return ""

    # --- abstract ---
    @abstractmethod
    def get_extra_status(self) -> dict: ...
    @abstractmethod
    def initialise(self, config_filename: str, *args, **kwargs) -> None: ...
    @abstractmethod
    def start(self) -> None: ...
    @abstractmethod
    def stop(self) -> None: ...
```

Hoist to the base the attributes every daemon has and the base methods read:
`self.running`, `self.hostname`, `self.fatal_exit_code`, `self.fatal_reason`,
and the `cfg_health_multicast_*` values.

### 6.3 Layering — why `processors/` and not `core/`

**Read this before writing the module.** The conclusion is that no change to
`tests/test_architecture.py` is needed, but the reasoning matters if the
placement is ever revisited.

`tests/test_architecture.py` enforces a layering ratchet: a module may import
from a lower layer or its own layer, and the known violations are listed
explicitly so a new one fails the build. `KNOWN_UPWARD_IMPORTS` is currently
**empty**.

`MWAXDaemon` needs:

| Import | Module | Layer |
|---|---|---|
| `send_multicast` | `net.multicast` | 2 |
| `MWAXDBHandler` | `db.handler` | 2 |
| `read_config` etc. | `core.config` | 1 |
| version string | `version` | 0 |

**`processors` is layer 4**, so `processors.daemon` picks up the existing
`"processors"` prefix entry in `LAYERS` and resolves to L4. Every import
above is at L0–L2, i.e. strictly downward. Nothing at L4 or below imports it
— only `cli` (L5) does, which is exactly what L5 is for.

**Verified empirically** against this commit with a stub
`processors/daemon.py` carrying all four imports above: all five architecture
tests pass, with no `LAYERS` edit and no `KNOWN_UPWARD_IMPORTS` entry.

It also fits semantically — the four classes it will serve are
`MWACacheArchiveProcessor`, `MWAXCalvinProcessor`, `MWAXCalvinController` and
`MWAXSubfileDistributor`, three of which are already named "Processor".

**Why not `core/daemon.py`:** `core` is **layer 1** ("thin wrappers over the
standard library and the OS") and currently has **zero** internal imports —
it is perfectly layer-1-clean. Placing the daemon there would make it import
`net` (L2) and `db` (L2), which is upward. Imports are collected from the
whole AST, so a function-local import does not dodge the check. Verified: the
same stub at `core/daemon.py` **fails**
`test_no_unexpected_upward_imports`. It could be rescued by adding
`"core.daemon"` to the L3 tuple (the test's longest-prefix matching supports
pulling a single module out of its package's default layer, and its docstring
says so explicitly) — but that is a workaround for a placement that
`processors/` satisfies cleanly, and it would be the first crack in an
otherwise pristine `core`.

**Note on `processors/` gaining a second concept.** The package currently
holds only queue-worker processors — the eight `MWAXWatchQueueWorker`
subclasses with their uniform `handler(self, item: str) -> bool` contract.
`MWAXDaemon` is a different kind of thing. Give the module a clear docstring
distinguishing the two, and consider a line in `processors/__init__.py`
(added in 0.1) noting that the package holds both the per-file queue
processors and the daemon lifecycle base class. If that ever starts to grate,
a dedicated L4 `daemon/` package is a clean future move requiring only a new
`LAYERS` entry — but do not do it pre-emptively.

Run `tests/test_architecture.py` immediately after creating the module and
before migrating any daemon. If it fails, stop and report — **do not add a
`KNOWN_UPWARD_IMPORTS` entry to make it pass.**

### 6.4 Commit split

1. Add `processors/daemon.py` with shared methods and hooks. **No daemon
   inherits yet.** Add unit tests for the base class in isolation, and run
   `tests/test_architecture.py` to confirm the layering (no `LAYERS` edit
   should be needed — see 6.3).
2. Migrate `MWACacheArchiveProcessor` (simplest). Run its tests.
3. Migrate `MWAXSubfileDistributor`. Run its six test modules.
4. Migrate `MWAXCalvinController` (exercises `before_health_send` and
   `during_sleep_interval`).
5. Migrate `MWAXCalvinProcessor` (exercises the `signal_handler` and
   `initialise_from_command_line` overrides).
6. Delete the dead per-daemon copies; rename `health_handler` →
   `health_loop` at the two thread-creation sites.

After each migration, **capture the health JSON payload and diff it against
the pre-migration payload.** The only expected difference anywhere is the
added `cmdline` key on the controller and processor. Anything else is a bug.

---

## Phase 7 — Docstrings, conventions and long functions

Lowest urgency, highest volume. Do not start until Phases 1–6 have landed.

### 7.1 Docstring convention stragglers

**File:** `src/mwax_mover/db/calibration.py`

Four functions use a `Parameters:` section with inline `(type)` annotations
and a non-standard indent instead of Google-style `Args:`. Three have
undocumented parameters, and all four claim `Returns: Nothing. Raises
exceptions on error` while annotated `-> None`:

| Function | Line | Undocumented params |
|---|---|---|
| `update_calsolution_request_submit_mwa_asvo_job_status` | 369 | `mwa_asvo_job_id`, `mwa_asvo_job_submitted_datetime` |
| `update_calsolution_request_download_complete_status` | 472 | `slurm_job_id` |
| `update_calsolution_request_calibration_started_status` | 590 | `slurm_job_id` |
| `update_calsolution_request_calibration_complete_status` | 640 | — |

The first also types `request_ids (int)` when it is `list[int]`.

**Fix:** convert all four to Google style, document the missing parameters,
replace `Returns:` with `Raises:`. **Same commit as 3.6** — these are the
same four functions.

The rest of the tree is in good shape: an automated Args-vs-signature audit
across ~250 functions found only these plus the `ν` parameter from 3.8.

### 7.2 Missing `Returns:` sections and return annotations

**Decided:** **no Google-style `Returns:` block required on trivial one-line
properties.** Record that exemption in a comment or in `CONTRIBUTING`-style
notes so it is a documented convention rather than an inconsistency.

That exempts the 11 properties in `calibration/models.py` (`mwalib_context`
L89, `tiles` L99, `inputs` L122, `tiles_df` L148, `inputs_df` L153,
`chan_info` L158, `time_info` L178, `calibrator` L186, `obsid` L194, plus
`GainFitInfo.default` L283 and `.nan` L298) and the short accessors in
`calvin/hyperdrive.py` (`chanblocks_hz` L309, `tile_flags` L327,
`get_average_times` L333, `all_chanblocks_hz_concat` L873, `calibrator` L912,
`results` L917).

**Still to fix — non-trivial functions returning undocumented values:**

- `calvin/plots/index.py:23` — `get_file_description`
- `db/calibration.py:125` — `insert_calibration_solutions_row` (`-> bool`)
- `db/data_files.py:77` — `insert_data_file_row` (`-> bool`)
- `db/data_files.py:153` — `update_data_file_row_as_archived` (`-> bool`)
- `net/webservice.py:53` — `call_webservice_inner`
- `cli/mwax_subfile_distributor.py:820,825,830,835,840,860` — the six
  `endpoint_*` methods return `(bytes, int)` tuples with **no return
  annotation at all**. Add annotations and `Returns:` — these are a public
  HTTP interface and the least obvious of the lot.

**Also fix:** `calvin/solution_files.py:145` `export_calibration_solutions`
has a `Returns:` section but is `-> None` and returns nothing — delete the
section.

**Also:** `calvin/hyperdrive.py:912` `calibrator` has no return annotation
(exempt from `Returns:` under the decision above, but the annotation is still
worth adding).

Separately, `ruff --select ANN` reports 141 `ANN001` and 118 `ANN201` across
`src/`, and `PTH` reports 200+ (`os.path` → `pathlib`). Each is a campaign of
its own, out of scope here.

### 7.3 Move restructure archaeology to `RESTRUCTURE.md`

Several module docstrings now narrate the migration instead of describing the
module. The worst is `core/command.py:1-18` — 17 lines about a
`calvin.pipeline` import cycle, naming files that no longer exist. Also:

- `calvin/pipeline.py:6,13`
- `calvin/solution_files.py:16`
- `calvin/hyperdrive.py:18-22`
- `calvin/plots/gains.py:3`
- `calvin/plots/hyperdrive_plots.py:32` (the module renamed in 3.7)
- `calvin/hyperdrive.py:884-895` — `refant` (*"Previously this computed its
  own metafits-OR-TILES-HDU check locally..."*)
- `calvin/hyperdrive.py:809-812` — `combined_tile_flags`
- `calvin/hyperdrive.py:917-921` — `results` (*"Previously .results was
  touched twice per file here..."*)
- `queues/queue_worker.py:22-45` — `calculate_backoff_seconds`'s `NOTE:`
  about the old linear-vs-exponential behaviour
- `filesystem/scan.py:1-7` — apologises for its sibling living in
  `queues/priority_queue_data.py`
- `calibration/outliers.py:35` — references the deleted
  `mwax_calvin_quality` (**removed by 2.1**)
- `tests/test_architecture.py:85-90` and `:73-78`
- `tests/calvin/test_hyperdrive.py:3-13`

A reader in a year will grep for `mwax_calvin_utils.py` and find nothing.

**Fix:** for each, **keep the sentence explaining why the code is the way it
is** — that reasoning is genuinely valuable (why `write_readme_file` lives in
`core.command`, why `refant` uses the combined flags) — and move the
**historical narrative** (which file it used to live in, which phase moved
it) into `docs/RESTRUCTURE.md`.

One commit per file, and **be conservative: if in doubt, keep the text.**
Losing a hard-won explanation is worse than carrying some archaeology.

### 7.4 Docstring style for the priority-queue siblings

`queues/priority_watcher.py:1-3` and `queues/priority_queue_worker.py:1-6`
kept terse pre-restructure module docstrings while `watcher.py` and
`queue_worker.py` received detailed ones. The two halves of each pair now
read as if written by different people.

**Fix:** bring the two priority modules up to the siblings' level of detail.
`priority_queue_worker.py:32` also has the `priorty` typo (1.5) and the
`filename_priority = 99` change (4.2).

Smaller related items:

- `queues/queue_worker.py:66-69` — a floating comment block describing
  `requeue_on_error` sits in the class body before `__init__`; it belongs in
  the `__init__` docstring, where the parameter is already documented. While
  there, **add a note recording that `PriorityQueueWorker` deliberately lacks
  this parameter** (see "Deliberately out of scope") so the next reviewer does
  not re-raise it.
- `core/command.py:178-180` — the `# This will return a popen process
  object...` comment duplicates the docstring below it; delete the comment.
- **Rename `MWAXWatchQueueWorker.scan_completed()` → `all_scans_completed()`.**
  **Decided.** `Watcher.scan_completed` is a bool attribute while the method
  is a method returning bool — same name, different kind, adjacent modules.
  Sites: `queues/watch_queue_worker.py:254` and `:448` (the two definitions),
  and callers. Note `:182`, `:261`, `:395`, `:455` reference
  `watcher.scan_completed` — the **attribute** on the watcher objects — and
  must **not** be renamed. Read each line carefully before editing; this is
  the kind of rename where a blind find-and-replace breaks things silently.
  Also grep `tests/queues/`.
- Log-prefix punctuation differs between the sibling base classes:
  `f"{self.name} Waiting..."` vs `f"{self.name}: Waiting..."`. Pick the
  colon form and apply it consistently.

### 7.5 Long function decomposition

| Lines | Function |
|---|---|
| 521 | `initialise` — `cli/mwax_subfile_distributor.py:202` |
| 415 | `_render_combined_gains_figure` — `calvin/plots/gains.py:579` |
| 396 | `initialise` — `cli/mwax_calvin_processor.py:1096` |
| 379 | `process_solutions` — `calvin/pipeline.py:48` |
| 346 | `start` — `cli/mwax_calvin_processor.py:184` |
| 315 | `handler` — `processors/subfile_incoming.py:132` |
| 308 | `main` — `cli/update_calvin_plots_and_index.py:149` |
| 265 | `fit_phase_line` — `calibration/fitting.py:166` |

**Start with `_render_combined_gains_figure`** — the most tractable. Its
shape is ~140 lines of setup, one `for i, tile in enumerate(tile_range)` loop
of ~235 lines, then ~40 lines of cleanup. The loop body is already
self-delimited by the author's own section comments:

```
# -- shade flagged channels with a translucent orange band --
# -- gx subplot: data, fit line, shaded acceptance band --
# -- gy subplot: data, fit line, shaded acceptance band --
```

Extracting `_draw_tile_panel(ax_gx, ax_gy, tile, bundle, ...)` takes it under
200 lines with no logic change. The gx and gy blocks are near duplicates —
check whether one parameterised helper serves both, but **do not force it**
if the polarisation-specific details differ.

**Verification is essential.** These functions render PNGs. Before
refactoring, capture the plotting tests' output as reference images (note
`plots/layout.py` already reduces DPI under pytest, so they are small), then
assert byte-identical output afterwards. Any difference means the refactor
changed behaviour.

The four `initialise` methods shrink substantially once Phase 5 and 7.6 land
— **re-measure after those before deciding whether they still need
decomposition.**

`fit_phase_line` and `process_solutions` are dense numerical and
orchestration code. **Do not touch them without discussing with Greg first**
— they are the least safe things in this document to refactor.

### 7.6 Shared config-reading helpers

Of 99 distinct `(section, key)` reads across the four daemons, 13 are read by
two or more with the reading code written out each time:

- `[mwax mover].health_multicast_{ip,port,hops,interface_name}` and
  `log_level` — **all four**, identical
- the five MWA-database keys — **all four** after 5.2
- `[giant squid].giant_squid_binary_path` — controller and processor
- `[mwax mover].archive_command_timeout_sec` — mwacache and subfile_distributor
- `[mwax mover].log_path` — controller and processor

**Fix:**

1. **Health config** — fold into Phase 6, not here. The `MWAXDaemon` base
   class is the only consumer of those five values, so reading them belongs
   in a base-class method (`_read_health_config(config)`) on
   `processors/daemon.py` rather than a free function in `core/config.py`.
   It sets the four `cfg_health_multicast_*` attributes plus the derived
   `health_multicast_interface_ip` (see 5.1) — which is why the derivation
   and the reads want to live together.

2. **Database config** — add a classmethod to `db/handler.py`:

   ```python
   @classmethod
   def from_config(cls, config: ConfigParser) -> "MWAXDBHandler": ...
   ```

   reading `[mwa database]` and applying the `db != "dummy"` b64 predicate
   from 1.2.

   **Put it in `db/handler.py`, not `core/config.py`.** A `core/config.py`
   helper returning an `MWAXDBHandler` would be `core` (L1) importing `db`
   (L2) — upward, and it would fail `tests/test_architecture.py`. A
   classmethod on the handler is correctly layered (`db` importing
   `core.config` is downward) and is the more natural API regardless.

   Note this constraint is independent of where the daemon base class lives:
   it is about `core`, not about `processors`. All four `cli` daemons (L5)
   and `processors/daemon.py` (L4) can call
   `MWAXDBHandler.from_config(...)` freely.

   Depends on 1.2 (one predicate) and 5.2 (one section).

### 7.7 Retry-strategy consistency

Two mechanisms for one concern:

- `tenacity` decorators — `filesystem/files.py:24`, `db/handler.py:15-20`
- hand-rolled loops — `mwa_asvo/giant_squid.py:88-100`, `net/webservice.py`

**Optional; raise with Greg before starting.** Converging on `tenacity` is
not a mechanical swap: `giant_squid.py`'s loop has a pytest-specific delay
collapse (L95) and jitter (`random.uniform(0, 1)`) needing
`wait_exponential_jitter` equivalents, and the pytest branch would have to
survive the conversion. Lower priority than everything above.

### 7.8 Clock consistency

44 uses of `time.time()` vs 7 of `time.monotonic()`. Elapsed durations should
use `monotonic`; wall-clock timestamps should use `time()`.

4.8 removes the worst instance of the coupling (`get_gbps` forcing
`time.time()` on its callers). The rest is a large, low-risk, tedious audit:
classify each of the 44 sites as duration or timestamp and convert the
duration ones.

**Do this last, or not at all.** The `get_gbps` fix in 4.8 is the part that
matters.

---

## Deliberately out of scope

Recorded so a future reader knows these were considered and rejected, not
missed.

### The `queues/` parallel hierarchies — NOT to be unified

**Decided:** *"No I don't want the queue/workers unified for now."*

Three pairs of near-identical classes remain, by decision:

| Plain | Priority | Overlap |
|---|---|---|
| `watcher.Watcher` | `priority_watcher.PriorityWatcher` | ~90% |
| `queue_worker.QueueWorker` | `priority_queue_worker.PriorityQueueWorker` | ~85% |
| `watch_queue_worker.MWAXWatchQueueWorker` | `watch_queue_worker.MWAXPriorityWatchQueueWorker` | ~90% |

Unifying them would remove roughly 300 lines but touches the hot path for
every processor. **Do not attempt it.** Items 3.3, 3.4 and 7.4 align their
*names and docstrings* only.

**`PriorityQueueWorker` lacks `requeue_on_error` by design.** Greg:
*"Absent because it wasn't needed for that use case."* Not a defect — do not
add it. Item 7.4 adds a comment recording this.

### Real mocks for `running_under_pytest()`

**Decided:** deferred until after this cleanup.

Nine call sites across seven modules change production behaviour under test:

- `archive/archiver.py:281` — skips the rclone upload entirely (carries its
  own `# TODO: ... should replace this with a Mock pattern`)
- `fits/subfile.py:208` — fakes the psrdada ringbuffer load
- `mwa_asvo/giant_squid.py:95` — collapses retry delays (well documented and
  justified)
- `calvin/plots/layout.py:38,52` — reduces plot DPI and figure size (well
  documented and justified)
- `cli/mwax_calvin_controller.py:1118`, `cli/mwax_calvin_processor.py:1167` —
  skips the b64 password decode (**removed by 1.2**)
- `cli/mwacache_archive_processor.py:53`,
  `cli/mwax_subfile_distributor.py:81,1186`

The `layout.py` and `giant_squid.py` uses are sound and should stay. The
`archiver.py` and `subfile.py` ones mean the tests never execute the real
code path — worth a dedicated piece of work afterwards, with its own plan.

Note 1.2 reduces the count from nine to seven, so the eventual mocking work
gets slightly smaller.

### Adding `request_fatal_shutdown` to the calvin daemons

Considered in 6.1 and rejected for this pass: giving the controller and
processor a fatal-shutdown path they currently lack is a behavioural
addition, not deduplication. Worth revisiting separately.

---

## Settled details

These were the last open choices; all are now decided. Recorded here as well
as inline, so there is one place to check.

1. **`health_multicast_interface_ip` takes no `cfg_` prefix** (see 5.1). It
   is derived at runtime from `health_multicast_interface_name`, not read
   from the config file. General rule: **only values read directly from
   config get the `cfg_` prefix.** Apply that test to any other derived
   attribute met during the rollout.
2. **`cfg_db_name`**, reading the config key `db` (see 5.2). The one place
   where the attribute name deliberately does not mirror its key —
   `cfg_db_db` reads badly. Add a short comment at the read site.
3. **Keep the `shutdown_log_detail()` hook** (see 6.1), so
   `MWAXSubfileDistributor`'s shutdown message keeps its worker count.
4. **`fitting.py:320-322`** (see 2.2) — **leave it in place and flag it in
   the diff** unless it is obviously ionospheric-fitting residue like the
   L298-301 block. Do not remove it on a guess; describe what it appears to
   be and let Greg decide.
5. **`hyperdrive.generate_plots(...)`** (see 3.7) — import the module as a
   name (`from mwax_mover.calvin.plots import hyperdrive`) and call through
   it, rather than importing the bare symbol. `generate_plots` alone is
   ambiguous at the call site; `hyperdrive.generate_plots` is not.

**No exit-code audit is required.** This repository contains no Ansible
roles, systemd units or Slurm scripts, and Greg has confirmed the current
exit codes carry no meaning. Implement 4.9 as written — one `EXIT_FAILURE`
constant for every aborting `sys.exit()` — without hunting for external
consumers.