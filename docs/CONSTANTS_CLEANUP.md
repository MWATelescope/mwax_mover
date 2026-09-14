# mwax_mover constants & magic-value cleanup plan

Status: **plan only — no code written yet.** All open decisions (Phase 0)
are now settled; every item is ready to implement as written.

This follows `docs/CLEANUP.md`'s Phase 4 (constants and configuration), which
this plan re-opens: that phase centralised INI section names, priority
integers, webservice URLs, the proxy config, the GiB conversion, and a
handful of repeated numeric literals. This document is a second pass,
specifically for constants/magic values that phase either didn't reach or
that were introduced afterwards (Phases 5-7).

Findings are against commit `3c4d369` on branch `source_code_restructure`
("CHANGELOG update plus some minor tweaks"). Re-verify line numbers before
editing — they will drift as phases land.

---

## Ground rules

Same as `docs/CLEANUP.md`:

1. **Never push, never open a PR, never touch GitHub.** Deliver work as a
   diff or a `git am` mbox patch for review. Applying and committing is
   Greg's job.
2. **Ask before creating, editing or deleting any file**, and before any
   action affecting another server, computer or system.
3. **Run `ruff check`, `ruff format --check` and `ty` on every touched
   file** before calling a change complete.
4. **Google-style docstrings on every function** added or rewritten.
5. **One phase per diff**, with the commit split suggested under each phase.
6. **Do not run the test suite as part of this pass.** Greg has already
   established the baseline and runs the suite himself after applying each
   diff.
7. **Make no behavioural change not explicitly called for here.** Every item
   in this plan is a pure rename/relocation of an existing literal — same
   value, new name. If any item turns out to require a *different* value
   anywhere (e.g. two "duplicate" literals that aren't actually equal on
   closer reading), stop and report rather than picking one.

## Phase ordering

- **Phase 1 first** — it's the highest-confidence, lowest-risk set: exact
  duplicate literals, verified equal, moved into `constants.py` with no
  logic change.
- **Phase 2 (the calibration/plotting column-name schema) is the largest
  single item** and touches six files at once. Do it in its own phase, after
  Phase 1, so a mistake there doesn't get tangled up with the smaller wins.
- **Phase 3 is lower-confidence / lower-value** — softer duplicates,
  internal-consistency nits, and one dead-code removal. Do this last, or
  skip it if time is short; nothing here is urgent.

---

## Phase 0 — Prerequisites

### 0.1 Baseline

**Skipped for this pass.** Greg has already run the full suite on the
current `main`/`source_code_restructure` tip; no baseline-recording step is
needed before starting Phase 1.

### 0.2 Decided: the column-name schema lives in `calibration/df_columns.py`

Phase 2 centralises 11 DataFrame-column/dict-key strings (`pol`, `tile_id`,
`flavor`, `outlier`, `soln_idx`, `sigma_resid`, `chi2dof`, `XX`, `YY`, `gx`,
`gy`) shared across `calibration/outliers.py`, `calvin/hyperdrive.py`,
`calvin/pipeline.py`, `calvin/plots/phases.py`, `calvin/plots/stats_table.py`,
and `calvin/plots/gains.py`, into a new module,
`src/mwax_mover/calibration/df_columns.py`. Keeps `constants.py` (currently
INI/timing/webservice-flavoured) from becoming a dumping ground;
`calibration` and `calvin` are both L4, so either can import it with no
layering issue.

### 0.3 Decided: the subfile_distributor log format was an oversight

`mwax_subfile_distributor.py:67` uses
`"%(asctime)s, %(levelname)s, %(message)s"` — the other three daemons all use
`"%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"`, which
additionally logs the module and function name. **Confirmed oversight** —
Phase 1.2 unifies all four daemons (plus `cal_utils.py`) on the fuller
format via a single shared `LOG_FORMAT` constant.

**[BEHAVIOURAL]** subfile_distributor's log lines gain a `module.function`
component they don't have today. More information, not less — low risk —
but flag it explicitly in the diff since it changes every line the daemon
logs.

---

## Phase 1 — Exact duplicate literals → `constants.py`

**Every item here is a verified exact duplicate** — same value, same
meaning, multiple independent sites. Commit split: 1.1 (statistics), 1.2
(logging), 1.3 (file extensions), 1.4 (solution-file naming
+ index.json), 1.5 (small identifiers: health_thread/dummy/port), 1.6
(EXPOSURE + MWAX_BEAMFORMER), 1.7 (config key names), 1.8 (HTTP status
codes).

### 1.1 MAD-to-std-dev scale factor

**Decided:** add to `constants.py`.

```python
# Scale factor converting median absolute deviation (MAD) to an equivalent
# standard deviation for a normal distribution (1 / Phi^-1(3/4)).
MAD_TO_STD_SCALE_FACTOR = 1.4826
```

Replace the literal `1.4826` at:

- `calibration/fitting.py:401` — inline, in `clip_scale = max(1.4826 *
  resid_mad, _MIN_CLIP_THRESHOLD_RAD)`
- `calibration/outliers.py:278` — currently `mad_to_std = 1.4826`
- `calvin/plots/phases.py:434` — currently `mad_to_std = 1.4826`

Keep the local `mad_to_std = MAD_TO_STD_SCALE_FACTOR` assignment where one
already exists (outliers.py, phases.py) rather than inlining the constant
name at every use — less diff noise, same effect.

### 1.2 Shared log format

**Decided (see 0.3).** Add:

```python
# Shared logging.Formatter format string for the four CLI daemons.
LOG_FORMAT = "%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"
```

Replace the identical literal at `cli/cal_utils.py:69`,
`cli/mwax_calvin_controller.py:62`, `cli/mwacache_archive_processor.py:33`,
`cli/mwax_calvin_processor.py:73`, **and** replace
`cli/mwax_subfile_distributor.py:67`'s shorter
`"%(asctime)s, %(levelname)s, %(message)s"` with the same `LOG_FORMAT`
constant, bringing it in line with its siblings.

**[BEHAVIOURAL]** — see 0.3. subfile_distributor's logs gain the
`%(name)s.%(funcName)s` component. Note it prominently in the diff even
though it's confirmed intentional; it's still a visible change to every log
line that daemon writes.

### 1.3 File extensions

**Decided:** add to `constants.py`, grouped with the existing `MODE_*`/`SECTION_*` block conventions.

```python
EXT_FITS = ".fits"
EXT_SUB = ".sub"
EXT_VDIF = ".vdif"
EXT_FIL = ".fil"
EXT_HDR = ".hdr"
```

Replace at:

| Extension | Sites |
|---|---|
| `.fits` | `processors/checksum_and_db.py:75`, `vis_cal_outgoing.py:39`, `vis_stats.py:58`, `outgoing.py:53`, `filesystem/naming.py:181`, `calvin/hyperdrive.py:516` (part of a `.replace()` pair — see 1.4), `calvin/plots/index.py:181,185` |
| `.sub` | `processors/subfile_incoming.py:101`, `checksum_and_db.py:76`, `outgoing.py:54`, `filesystem/naming.py:179`, `cli/mwax_subfile_distributor.py:543` |
| `.vdif` | `processors/bf_stitching.py:94,141`, `beamformer/vdif.py:212,103`, `filesystem/naming.py:197` |
| `.fil` | `processors/bf_stitching.py:94,179`, `filesystem/naming.py:201` |
| `.hdr` | `beamformer/vdif.py:212,103`, `filesystem/naming.py:197` |

**Watch out:** `filesystem/naming.py`'s `validate_filename` compares against
`file_ext_part.lower()` — confirm each constant is used only in contexts
that are already lower-cased, or the comparison silently breaks for
mixed-case input that worked before by accident.

### 1.4 Solution-file naming convention

**Decided:** add to `constants.py`.

```python
SOLUTIONS_FITS_SUFFIX = "solutions.fits"
SOLUTIONS_ORIGINAL_FITS_SUFFIX = "solutions.original.fits"
SOLUTIONS_FITS_GLOB = "*_solutions.fits"
SOLUTIONS_ORIGINAL_FITS_GLOB = "*_solutions.original.fits"
```

Replace at:

- `cli/mwax_calvin_processor.py:400` — `glob.glob(..., "*_solutions.fits")`
- `cli/update_calvin_plots_and_index.py:358,412` — both globs
- `calvin/solution_files.py:317,318,327` — both the list-of-globs and the
  membership check
- `calvin/birli.py:71` — `.endswith("solutions.fits")`
- `calvin/plots/index.py:185` — both `.endswith()` checks
- `calvin/hyperdrive.py:516` — `self.filename.replace(".fits",
  ".original.fits")`. This one doesn't match cleanly onto the new constants
  (it's a `.replace()` on the bare `.fits` extension, not the `solutions.*`
  suffix) — leave as `EXT_FITS`/`".original" + EXT_FITS` unless a cleaner
  form is obvious once the surrounding code is in view.

Also add:

```python
INDEX_JSON_FILENAME = "index.json"
```

Replace at `cli/generate_index_json.py:68,71`,
`cli/mwax_calvin_processor.py:447`,
`cli/update_calvin_plots_and_index.py:62,425`,
`calvin/plots/index.py:119`.

### 1.5 Small identifiers

**Decided:** add all three.

```python
HEALTH_THREAD_NAME = "health_thread"
DUMMY_CONFIG_VALUE = "dummy"
DEFAULT_POSTGRES_PORT = 5432
```

- `HEALTH_THREAD_NAME` — replace at the `threading.Thread(name="health_thread", ...)`
  call in all four daemons' `start()` methods.
- `DUMMY_CONFIG_VALUE` — replace the `"dummy"` literal at
  `cli/mwacache_archive_processor.py:106`, `cli/mwax_subfile_distributor.py:1043`,
  `db/handler.py:49,81`. **Do not touch the predicate logic** (`!= "dummy"` vs
  `== "dummy"` at each site) — this item only names the literal, per Ground
  Rule 7.
- `DEFAULT_POSTGRES_PORT` — replace the `5432` default at
  `cli/mwacache_archive_processor.py:79` and `cli/mwax_subfile_distributor.py:162`
  (`self.cfg_db_port: int = 5432`, the pre-config-read default).

### 1.6 `EXPOSURE` and `MWAX_BEAMFORMER`

**Decided:**

```python
METAFITS_KEY_EXPOSURE = "EXPOSURE"
```

`processors/subfile_incoming.py:40` and `processors/bf_stitching.py:21` each
currently define their own `METAFITS_EXPOSURE = "EXPOSURE"`. Delete both
local definitions, import `METAFITS_KEY_EXPOSURE` from `constants.py`
instead, and update the three use sites in `bf_stitching.py` (`:117,123,126`)
and any in `subfile_incoming.py`.

**Separately — decided:** `beamformer/vdif.py:34` hardcodes
`self.MODE: str = "MWAX_BEAMFORMER"`. `fits/subfile.py:59` already has this
as `CorrelatorMode.MWAX_BEAMFORMER`, a real enum member. `beamformer` is
layer 3, `fits` is layer 2, so `beamformer` importing from `fits` is a
downward import — no layering issue. Change `vdif.py:34` to
`self.MODE: str = CorrelatorMode.MWAX_BEAMFORMER.value` and import
`CorrelatorMode` from `fits.subfile`. Run `tests/test_architecture.py`
immediately after to confirm.

### 1.7 Repeated config-key-name literals

**Decided.** These four config keys are each read identically in a pair of
sibling daemons, with the key name typed out separately in each:

| Key | Sites |
|---|---|
| `log_level` | all four daemons' `initialise()`, via `read_optional_config(config, SECTION_MWAX_MOVER, "log_level")` |
| `giant_squid_binary_path` | `mwax_calvin_controller.py:1119`, `mwax_calvin_processor.py:1181` |
| `high_priority_correlator_projectids` | `mwacache_archive_processor.py:279`, `mwax_subfile_distributor.py:339` |
| `high_priority_vcs_projectids` | `mwacache_archive_processor.py:284`, `mwax_subfile_distributor.py:344` |
| `archive_command_timeout_sec` | `mwacache_archive_processor.py:261`, `mwax_subfile_distributor.py:233` |

Add matching `CONFIG_KEY_*` string constants to `constants.py` and use them
at each read site. This does **not** replace the reads with a shared helper
method (that's a Phase 7.6-style refactor, out of scope here) — it only
removes the literal-string duplication. Note in the diff that a shared
`_read_common_daemon_config()` helper is a natural follow-up, for Greg to
decide on separately.

### 1.8 HTTP status codes

**Decided.** `cli/mwax_subfile_distributor.py` already uses
`http.HTTPStatus.OK` for every `200` response (Phase 4.7 of the previous
cleanup) but still has raw `400`/`500` literals at
`:841,902,907,911,914`. Replace with `http.HTTPStatus.BAD_REQUEST` and
`http.HTTPStatus.INTERNAL_SERVER_ERROR` respectively — `http` is already
imported for the `.OK` usage. Also `net/webservice.py:89`'s range check
(`response.status_code >= 400 and response.status_code <= 599`) — replace
the two bounds with `http.HTTPStatus.BAD_REQUEST` and `599` (there's no
single named constant for "highest possible HTTP status"; leave `599`
literal with a short comment).

---

## Phase 2 — Calibration/plotting column-name schema

The strings `pol`, `tile_id`, `flavor`, `outlier`, `soln_idx`, `sigma_resid`,
`chi2dof`, `XX`, `YY`, `gx`, `gy` are used as DataFrame column names and dict
keys, identically, across:

- `calibration/outliers.py`
- `calvin/hyperdrive.py`
- `calvin/pipeline.py`
- `calvin/plots/phases.py`
- `calvin/plots/stats_table.py`
- `calvin/plots/gains.py` (`gx`/`gy` only)

**Design:** new module `src/mwax_mover/calibration/df_columns.py`, one
constant per column:

```python
"""Shared DataFrame column names and dict keys for the calibration pipeline.

These are read and written across calibration/, calvin/, and calvin/plots/ —
centralised here so a typo doesn't silently create a new column or fail with
a KeyError far from the mistake.
"""

COL_POL = "pol"
COL_TILE_ID = "tile_id"
COL_FLAVOR = "flavor"
COL_OUTLIER = "outlier"
COL_SOLN_IDX = "soln_idx"
COL_SIGMA_RESID = "sigma_resid"
COL_CHI2DOF = "chi2dof"
COL_XX = "XX"
COL_YY = "YY"
COL_GX = "gx"
COL_GY = "gy"
```

**Commit split — one file at a time, in this order** (narrowest first, so a
mistake surfaces early on a smaller blast radius):

1. `calibration/outliers.py` — defines the columns it produces; migrate
   first since the others consume its output.
2. `calvin/hyperdrive.py`
3. `calvin/pipeline.py`
4. `calvin/plots/stats_table.py`
5. `calvin/plots/phases.py`
6. `calvin/plots/gains.py` (`gx`/`gy` only — it doesn't touch the other nine)

**Grep for every string after each file**, including f-strings and
`.rename()`/`.groupby()` calls — a column name used as a `groupby` key or
inside a format string is just as real a dependency as a dict subscript and
easy to miss with a mechanical find-and-replace.

**Verification is essential — this changes what six files import and
reference, with no logic change intended.** Run each file's test module
immediately after its commit, not batched at the end. If any test asserts
on a raw string that should now reference the constant, update the
assertion rather than leaving the string floating.

**Leave `obs_id` alone.** It matches the same literal in this codebase but
is a different, much more widely-used concept (the MWA observation ID,
referenced everywhere from `filesystem/naming.py` to `mwa_asvo/jobs.py`) —
promoting it to a column constant here would conflate two different things
that happen to share a name.

---

## Phase 3 — Lower-confidence items

Optional; do these last, or skip. Nothing here is urgent, and a couple are
genuinely small enough that a dedicated diff may not be worth the review
overhead — flagged per item.

### 3.1 `core/units.py` internal consistency

**File:** `src/mwax_mover/core/units.py`

`gigabyte_to_gibibyte` (L38) computes `gigabytes * 10**9 / 2**30`;
`bytes_to_gigabytes` (L64) computes `num_bytes / (1000.0 * 1000.0 *
1000.0)` — same unit (10^9 bytes = 1 GB) written two different ways in the
same file.

**Fix:** add a local module constant:

```python
BYTES_PER_GIGABYTE = 10**9
```

and use it in both functions:
`gigabytes * BYTES_PER_GIGABYTE / 2**30` and
`num_bytes / BYTES_PER_GIGABYTE`.

Local to `core/units.py` — no need to put this in `constants.py`, it isn't
used anywhere else. **Check `tests/core/test_units.py`** for any test
asserting on the exact float division path (`1000.0 * 1000.0 * 1000.0` vs
`10**9` can differ in floating-point rounding at the many-significant-digit
level — verify the existing tests' tolerance covers this before assuming
it's a no-op).

### 3.2 Beamformer chunk-read sizes

**Files:** `beamformer/filterbank.py:19`, `beamformer/vdif.py:228`

`filterbank.py` has `CHUNK_SIZE = 8 * 1024 * 1024` (named, 8 MiB);
`vdif.py:228` has an unnamed `input_file.read(1024 * 1024)` (1 MiB). Same
concept — a streaming read buffer size — different magnitude, in sibling
modules of the same package.

**Fix (optional, cosmetic):** add a local constant to `vdif.py`:

```python
_READ_CHUNK_SIZE = 1024 * 1024  # 1 MiB
```

and use it at L228. Does **not** propose unifying the two sizes — 8 MiB vs 1
MiB may be a deliberate choice for the different file formats; this item is
purely about naming the literal that already exists, not changing it.

### 3.3 Dead `COMMAND_DADA_DISKDB` constant

**File:** `processors/subfile_incoming.py:41`

`COMMAND_DADA_DISKDB = "dada_diskdb"` is defined but never referenced
anywhere in `src/` or `tests/` — confirmed by grep. Meanwhile
`fits/subfile.py:202` hardcodes the same command name directly:
`cmd = f"dada_diskdb -k {ringbuffer_key} -f {full_filename}"`.

**Decided:** delete the unused constant from `subfile_incoming.py`, and add
a replacement to `constants.py`:

```python
COMMAND_DADA_DISKDB = "dada_diskdb"
```

used at `fits/subfile.py:202`. This is a rename **plus** a dead-code
removal in one commit — call that out in the diff.

### 3.4 `update_calvin_plots_and_index.py` should use `MWAXDBHandler.from_config()`

**File:** `cli/update_calvin_plots_and_index.py:235-240`

This script reads the five `[mwa database]` keys (`host`, `db`, `user`,
`pass`, `port`) manually, duplicating exactly what
`db/handler.py`'s `MWAXDBHandler.from_config()` classmethod already does
(added in Phase 7.6 of the previous cleanup). Not a new constant — an
existing shared helper this script didn't adopt.

**Fix:** replace the five manual reads with
`MWAXDBHandler.from_config(config)`, matching how the other CLI entry
points already do it. Check what the resulting handler is used for
downstream in this script (it currently builds `db_host`/`db_user`/etc. as
separate locals) and adjust call sites accordingly.

**Lowest priority in this plan** — `update_calvin_plots_and_index.py` is a
one-off utility script, not a long-running daemon, so the duplication here
carries less ongoing risk than the Phase 1 items. Fine to defer indefinitely.

---

## Deliberately out of scope

Recorded so a future reader knows these were considered and rejected, not
missed.

- **Matplotlib cosmetic literals** in `calvin/plots/*.py` (`fontsize=`,
  `zorder=`, `linewidth=`, `s=`, colour names). Hundreds of one-off
  presentation values. Centralising these would add cross-module coupling
  for a value nobody will ever need to change in more than one place at
  once — this is what "local, no action" means for plotting code.
- **`encoding="utf-8"` casing inconsistency** (`'utf-8'` vs `'UTF-8'` across
  16 files). Cosmetic only; Python's `codecs` module treats both
  identically. Not worth a diff.
- **HTTP `GET`/`POST` as Flask route-method literals.** Idiomatic Flask
  usage; not a magic-value risk.
- **argparse boilerplate** (`-c`/`--cfg`, `store_true`, etc.) — inherent to
  each CLI entry point's argument definitions; the duplication that exists
  (`processors/daemon.py` vs `mwax_calvin_processor.py`'s override) is a
  known, accepted consequence of the Phase 6 decision to let the processor
  override `initialise_from_command_line` entirely.
- **`MWA_NUM_COARSE_CHANS = 24`** (`calibration/models.py:18`) — a
  genuinely telescope-wide physical constant, but currently referenced only
  within its own file. Nothing to consolidate yet; worth remembering it
  exists if a literal `24` meaning "coarse channels" turns up elsewhere in
  future code, rather than re-deriving it.
- **Retry counts and timeouts that coincidentally share a value** (several
  unrelated `3`s, `10`s, `30`s, `60`s across `net/`, `db/`, `archive/`,
  `queues/`). Each occurrence was checked individually; where two sites use
  the same number for a genuinely different reason, unifying them would
  create a false coupling — a change to one's retry count would silently
  change the other's. Only the verified same-meaning duplicates are in
  Phase 1.

---

## Settled details

1. **Phase 1 items are pure literal-naming — no logic change anywhere.**
   Where an item touches a predicate or comparison (1.5's `dummy` sentinel,
   1.3's `.lower()` comparisons), only the literal is renamed; the
   surrounding logic is untouched.
2. **`constants.py` grouping** — new Phase 1 constants should be added
   under a short comment header per category (extensions, file-naming,
   identifiers, config keys), matching the existing file's style, and the
   module docstring at the top of `constants.py` updated to mention the new
   categories, the same way it already lists `SECTION_*`/`MWA_WEBSERVICE_HOSTS`/etc.
3. **Phase 2's module is `calibration/df_columns.py`** (see 0.2) — decided,
   not a recommendation.