# Source tree restructure

Working document for the migration from a flat module list to layered
subpackages. It records the target structure, what has already landed, the
decisions taken (and why, including the ones that were reversed), and the
remaining phases.

Kept in the repo rather than in an issue so the reasoning travels with the code
and survives a gap between working sessions.

## Why

`src/mwax_mover/` is flat: 37 modules in one directory, plus `cli/`. Two of them
have become grab-bags. `utils.py` (2486 lines, 62 top-level definitions) contains
config parsing, filename validation, subfile header injection, multicast, a
webservice client, rclone/S3 wrappers, GPS time conversion and unit helpers.
`mwax_calvin_utils.py` (2568 lines, 46 definitions) mixes FITS readers, domain
models, calibration maths, external-tool runners, Slurm submission, solution file
naming and plot indexing.

The goal is smaller, self-contained modules grouped by concern, with a
dependency direction that is enforced rather than hoped for.

## Target structure

```
L0  constants.py (was mwax_mover.py), version.py
L1  core/        config, command, units, gpstime, env
L2  fits/        metafits, hdu, subfile
    filesystem/  scan, files, naming
    net/         multicast, webservice, redis, s3
    db/          handler, calibration, data_files, ...
L3  queues/      watcher, priority_watcher, queue_worker,
                 priority_queue_worker, priority_queue_data, watch_queue_worker
    archive/     archiver
    beamformer/  vdif, filterbank
L4  processors/  subfile_incoming, checksum_and_db, outgoing, pawsey_outgoing,
                 vis_cal_outgoing, vis_stats, packet_stats, bf_stitching
    calibration/ models, fitting, outliers, solutions
    calvin/      pipeline, solution_files, hyperdrive, birli, slurm, asvo,
                 plots/{gains,phase_fits,stats_table,hyperdrive_plots,layout,index}
L5  cli/         unchanged
```

Rule: a module may import from a **lower** layer or its **own** layer, never a
higher one. Enforced by `tests/test000_architecture.py`.

### The target layering already almost holds

Measured across 37 modules and 102 internal import edges: exactly **one** upward
import and **one** cycle. The restructure is largely moving files into the shape
the dependencies already have, which is far lower risk than it first appeared.

- Upward: `utils` -> `mwax_priority_queue_data`, caused solely by
  `scan_for_existing_files_and_add_to_priority_queue()` needing
  `MWAXPriorityQueueData`. That function is queue-population logic, not a generic
  utility; moving it into `queues/` removes the violation.
- Cycle: `mwax_calvin_utils` <-> `mwax_hyperdrive_solutions`, already worked
  around with a function-local import in `get_convergence_summary()`. Splitting
  the shared primitives down into `fits/` and `calibration/fitting.py` and moving
  `get_convergence_summary` above the solutions reader dissolves it.

Both are listed in the architecture test's ratchet lists and must be deleted from
those lists as they are fixed.

### Migration surface

| | count |
|---|---|
| internal import statements | 120 |
| prose references to module filenames in docstrings/comments | 115 |
| fan-in: `utils` | 20 |
| fan-in: `mwax_mover` (constants) | 12 |
| fan-in: `mwax_watch_queue_worker` | 10 |
| fan-in: `mwax_calvin_utils` | 9 |
| fan-in: `mwax_db` | 8 |

The 115 prose references are the easily-forgotten cost: this codebase's
docstrings cross-reference module filenames heavily (16 point at
`mwax_calvin_utils.py` alone), and stale "see xxx.py" pointers rot silently.

## Phase 0 (complete)

Guard rails and prerequisites, plus the one L0 rename.

| commit | what |
|---|---|
| `258e959` | `tests/test000_architecture.py` -- import-layering test |
| `1b28c86` | `tests/__init__.py` + `pythonpath` / ty `extra-paths` |
| `25e0577` | `ruff check` made a blocking gate (CI + pre-commit) |
| `617393e` | fixture paths made CWD-independent |
| `fea718b` | `mwax_mover.py` renamed to `constants.py` |
| `c26c537` | the 15 dependent files (12 expected from the doc's own fan-in count, plus 3 more caught by `ty check`) |

### Architecture test

Three rules: every module must be assigned a layer; no upward imports; no
cycles. Layers are keyed by **dotted prefix**, so one entry covers a whole future
package (`calibration.fitting` matches `calibration`) and the flat module-name
entries get deleted as files move.

Known violations are a **ratchet**: the test asserts the real set matches the
listed set *exactly*. A new violation fails; fixing a listed one also fails until
the entry is deleted. The lists can only shrink.

Imports are collected from the whole AST, so a function-local import still
counts -- a deferred import is still a dependency.

Negative-tested against eight failure modes (unassigned module, upward import in
both `import` styles, new cycle, non-cli importing cli in both styles, both
stale-entry cases). All detected.

**Gotcha found while doing this:** `from mwax_mover import utils` has
`node.module == "mwax_mover"`, so a naive AST walk attributes the edge to the
*constants* module and silently drops the real one. That form is used in 18
files, and an earlier analysis built on it produced wrong fan-in numbers. The
test handles all three forms (`from pkg.mod import sym`, `from pkg import mod`,
`import pkg.mod`); don't "simplify" it.

### Test tree as a package

`tests/__init__.py` plus `pythonpath = ["tests"]` in `pyproject.toml`.

Reason: the tests are to be reorganised to mirror the package layout, and without
a package pytest's prepend import mode identifies a test module by basename
alone, so `tests/calibration/test_plots.py` and `tests/calvin/test_plots.py`
collide with "import file mismatch". As a package the collision goes away.

But making it a package puts the repository root on `sys.path` instead of
`tests/`, so the shared helpers (`tests_common`, `tests_fakedb`) stop resolving
as top-level imports -- hence `pythonpath`.

**This needed a second, non-obvious setting.** `ty` does not read pytest's
`pythonpath`, and went from clean to 24 `unresolved-import` errors. Fixed with
`[tool.ty.environment] extra-paths = ["tests"]`. The two settings must stay in
step. The alternative (move helpers to `tests/support/` and rewrite the 24
imports to `from tests.support... import ...`) needs neither setting; it was
considered and not taken, but remains a reasonable future simplification.

### Lint as a gate

`ruff check` is now blocking in CI with `--output-format=github` (annotations
land on the diff), and `- id: ruff-check` runs in pre-commit. Deliberately no
`--fix`: the findings cleared before this needed real decisions
(`zip(strict=)` is a judgement about whether a length mismatch is a bug), and
several of ruff's fixes for them are classed unsafe.

`ty check` was widened from `src/` to `src/ tests/` -- a `src/`-only check cannot
see a break in the test-helper path wiring, which is exactly the regression
above.

This gate immediately paid for itself in the next step: after a mechanical
rewrite it listed every missing import as F821 by file and name.

### CWD-independent fixture paths

`tests_common` gained `data_path(*parts)`, `obs_data_dir(obs_id)` and
`obs_metafits_path(obs_id)`, anchored on `Path(__file__).resolve().parent`. All
95 hardcoded `"tests/data/..."` literals across 15 files were converted, and
`render_test_config`'s own relative `Path("tests") / "data" / ...` was fixed.

They return `str`, not `Path`, because nearly every call site passes the result
straight into production code that takes str paths.

Before: `pytest` run from outside the repo root died at collection. After: it
passes. This was the hard prerequisite for moving any test file, since fixture
paths must not depend on where the test module lives.

Verified by diffing the *resolved* path sets between the committed and working
versions (old literals from `git show`, new calls evaluated via AST): zero paths
referenced after but not before. Twelve new paths do not exist on disk; all
twelve were already non-existent (mock subfiles the tests create, and filename
prefixes).

## Phase 1 (complete)

Leaf packages: `queues/`, `archive/`, `beamformer/`, `processors/`. Done as a
single commit rather than one per package, since all four are pure moves with
no cross-dependencies between them.

| old | new |
|---|---|
| `mwax_watcher.py` | `queues/watcher.py` |
| `mwax_priority_watcher.py` | `queues/priority_watcher.py` |
| `mwax_queue_worker.py` | `queues/queue_worker.py` |
| `mwax_priority_queue_worker.py` | `queues/priority_queue_worker.py` |
| `mwax_priority_queue_data.py` | `queues/priority_queue_data.py` |
| `mwax_watch_queue_worker.py` | `queues/watch_queue_worker.py` |
| `mwa_archiver.py` | `archive/archiver.py` |
| `mwax_bf_vdif_utils.py` | `beamformer/vdif.py` |
| `mwax_bf_filterbank_utils.py` | `beamformer/filterbank.py` |
| `mwax_wqw_subfile_incoming_processor.py` | `processors/subfile_incoming.py` |
| `mwax_wqw_checksum_and_db.py` | `processors/checksum_and_db.py` |
| `mwax_wqw_outgoing.py` | `processors/outgoing.py` |
| `mwax_wqw_pawsey_outgoing.py` | `processors/pawsey_outgoing.py` |
| `mwax_wqw_vis_cal_outgoing.py` | `processors/vis_cal_outgoing.py` |
| `mwax_wqw_vis_stats.py` | `processors/vis_stats.py` |
| `mwax_wqw_packet_stats_processor.py` | `processors/packet_stats.py` |
| `mwax_wqw_bf_stitching_processor.py` | `processors/bf_stitching.py` |

No `__init__.py` in any of the four: `cli/` was already an implicit namespace
package, so the new leaf packages follow the same convention rather than
introducing a second one.

### The upward import is gone

`scan_for_existing_files_and_add_to_priority_queue()` moved from `utils.py`
into `queues/priority_queue_data.py`, alongside the `MWAXPriorityQueueData` it
constructs. It now calls back into `utils.scan_directory()` and
`utils.get_priority()` (a downward import, L3 -> L2, which is fine) instead of
the reverse. `KNOWN_UPWARD_IMPORTS` in the architecture test is now empty --
the ratchet's first entry is fully paid off.

Its one caller (`queues/priority_watcher.py`) imports it directly rather than
via a `utils.`-qualified call, matching how it already imported
`MWAXPriorityQueueData` from the same module. The one test that exercised it
directly moved from `test005_utils.py` to `test012_priority_queue_data.py` for
the same reason -- it was testing code that no longer lives in `utils.py`.

### Import-site fixing, again

Same two gotchas as the `constants.py` rename bit again, at larger scale:

- **Multi-name `from mwax_mover import a, b, c` lists.** `bf_stitching.py`
  imported both beamformer utility modules this way
  (`mwax_bf_filterbank_utils, mwax_bf_vdif_utils, utils`), and
  `pawsey_outgoing.py` imported `mwa_archiver` alongside `mwax_db` and `utils`
  the same way. A plain grep for `from mwax_mover.<old> import` misses these;
  had to grep for the bare name anywhere in a `from mwax_mover import ...`
  line and fix the list plus every in-body `<old_name>.` attribute access by
  hand.
- **`mock.patch("mwax_mover.<old_module>.<attr>")` string targets.** These
  don't show up in an import-statement grep at all --
  `test015_wqw_checksum_and_db.py` alone had 66 of them. Fixed with a
  prefix-only substitution (`mwax_mover.mwax_wqw_checksum_and_db.` ->
  `mwax_mover.processors.checksum_and_db.`) rather than touching the attribute
  names after the prefix.

Verified clean with a whole-repo grep for all three import forms (`from
pkg.mod import sym`, `from pkg import mod`, `import pkg.mod`) plus a bare
word-boundary grep for each of the 17 old names, before running the gates.

## Phase 2 (complete)

Split `mwax_db.py` (1219 lines, 17 top-level symbols) by table/domain into
`db/`, as one commit:

| old | new |
|---|---|
| `MWAXDBHandler` | `db/handler.py` |
| `DataFileRow`, `get_data_file_row`, `insert_data_file_row`, `update_data_file_row_as_archived` | `db/data_files.py` |
| the other 12 functions (`calibration_request`/`calibration_fits`/`calibration_solutions`) | `db/calibration.py` |

`db/calibration.py` is a distinct thing from the `calibration/` package Phase 3
will create -- this one is the database layer for those tables (L2); Phase 3's
`calibration/` is calibration *domain logic* (fitting, outliers) at L4. Same
word, different layer, no actual import collision since the dotted paths
differ (`mwax_mover.db.calibration` vs `mwax_mover.calibration`).

`git mv mwax_db.py db/calibration.py` first (keeps history for the largest
chunk), then wrote `handler.py` and `data_files.py` as new files, then deleted
the non-calibration content back out of `calibration.py`.

### Verifying a split, not just a move

A rename's correctness check is "grep for the old name." A split needs more:
content could be dropped, duplicated, or subtly edited while still leaving
every name findable. Did two checks per the working agreement's AST-comparison
rule, both before touching any caller:

- **Symbol set.** `ast.parse` each of the three new files, collect top-level
  `FunctionDef`/`ClassDef` names, union them, and diff against the same
  collected from the original file at its last commit (`git show
  <rev>:src/mwax_mover/mwax_db.py`). 17 in, 17 out, nothing dropped or added.
- **Body equality.** Not enough on its own -- a function present by name could
  still have been edited while moving it. `ast.get_source_segment` each
  top-level node and compare the literal source text, old vs new, per symbol.
  All 17 identical.

### Import-site fixing found one new gotcha

Same three forms as before, plus one the leaf-package phase didn't hit: a
bare-module import colliding with an unrelated same-named variable.
`mwacache_archive_processor.py` and `mwax_subfile_distributor.py` both define
a module-level `handler = logging.StreamHandler()` for the root logger right
next to where `from mwax_mover.db import handler` would have gone for the
`mwax_db.MWAXDBHandler(...)` call sites. Importing the submodule under that
name would have silently shadowed the logging handler instead of failing
loudly -- `ty` doesn't catch a same-named rebind like this the way it catches
a missing import. Used the direct-name import (`from mwax_mover.db.handler
import MWAXDBHandler`) instead, which both files already had partially in
place, and dropped the bare-module attribute access entirely rather than
picking an alias.

Also fixed two stale prose pointers while in the affected files anyway (not a
Phase 5 sweep, just the ones sitting in files this commit already touched):
`tests_fakedb.py`'s docstring said `mwax_mover.mwax_db`, and
`mwax_calvin_controller.py` had a `` `mwax_db.insert_calibration_fits_row` ``
cross-reference in a docstring.

## Remaining phases

Ordering principle: leaves first, to prove the tooling before it touches the
high-fan-in god-modules.

**Phase 3 -- split `utils.py` and `mwax_calvin_utils.py`** into `core/`,
`fits/`, `filesystem/`, `net/`, `calibration/`, `calvin/`. Highest value and
highest churn (fan-in 20 and 9), done once the pattern is established.

**Phase 4 -- split `mwax_calvin_plots.py`** (2383 lines) into
`calvin/plots/`. Note `fit_phase_line` is 265 lines on its own, so ~200-400 line
files are a guide, not a rule.

**Phase 5 -- docs.** The 115 prose module references, `README.md`, `CALVIN.md`,
`.pre-commit-config.yaml`.

**Also outstanding (test-side, can happen any time):** move test modules into a
mirror of the package structure (`tests/calibration/test_fitting.py`, etc.).
Fixture data stays in one shared `tests/data/` -- it is 414 MB across 29 obsid
directories, several shared between test modules, so splitting it per package
would duplicate or scatter it.

### Deliberately not doing

- **Tests inside `src/`.** Considered and rejected: 414 MB of shared fixture
  data, and tests under `src/` ship in the wheel unless excluded. Mirroring the
  structure under `tests/` gets the locality benefit without either problem.
- **Nesting `processors/` under `queues/`.** Initially suggested, then reversed:
  `mwax_wqw_subfile_incoming_processor` imports `mwax_calvin_utils`, so
  processors genuinely depend on the calibration domain rather than being pure
  framework. Nesting them would put a subpackage at a higher layer than its
  parent -- a layer inversion in the directory tree. `processors/` is top-level.
- **Renaming `testNNN_*.py` to `test_<module>.py`** as part of this work. The
  numeric codes are load-bearing: `tests/data/test001...test021` directories are
  keyed by them and `setup_test_directories("test016")` looks them up by string.
  Renaming means renaming those data directories too -- a separate step. (It
  would also allow dropping the `python_files` override in `pyproject.toml`.)

## Working agreements

- Always clone fresh from GitHub at the start of a session; never push.
- One concern per diff, reviewed and applied by Greg.
- Keep pure moves in separate commits from content edits, so a reviewer can
  trust that a move commit changed nothing.
- `git mv` for every move so history follows.
- For a split, verify the new package's exported symbol set equals the old
  module's via an AST comparison -- that catches a dropped function no current
  caller happens to use.
- After each step: `ruff check .`, `ruff format --check .`,
  `ty check src/ tests/`, and the test suite.
- Test suite timing: ~13 minutes, dominated by `test020_calvin_solutions.py`
  (11 tests, real fixtures, phase fitting).
- Diffs have been clipped in transit once (a truncated final line reads as
  `corrupt patch at line NNN`). Ship a base64 copy alongside, and a sha256.
