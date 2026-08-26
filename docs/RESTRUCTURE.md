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

Guard rails and prerequisites. No source files moved.

| commit | what |
|---|---|
| `258e959` | `tests/test000_architecture.py` -- import-layering test |
| `1b28c86` | `tests/__init__.py` + `pythonpath` / ty `extra-paths` |
| `25e0577` | `ruff check` made a blocking gate (CI + pre-commit) |
| `617393e` | fixture paths made CWD-independent |

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

## Remaining phases

Ordering principle: leaves first, to prove the tooling before it touches the
high-fan-in god-modules.

**Phase 1 -- leaf packages, one per commit.** `beamformer/`, `queues/`,
`processors/`, `archive/`. Pure `git mv` plus import rewrites, low fan-in.
Includes moving `scan_for_existing_files_and_add_to_priority_queue()` out of
`utils` into `queues/`, which clears the one known upward import.

**Phase 2 -- split `mwax_db.py`** (1219 lines) by table/domain into `db/`.
Mechanical; 8 dependents.

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
