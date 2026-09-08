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

## Phase 3 (complete)

Splitting `utils.py` and `mwax_calvin_utils.py` into `core/`, `fits/`,
`filesystem/`, `net/`, `calibration/`, `calvin/` is by far the largest phase --
roughly 105 symbols across the two source files moving into ~19 new files,
against a combined fan-in over 25. Being done as three commits by target
group rather than one, per Greg's call on chunking:

1. `core/` (this commit)
2. `fits/` + `filesystem/` + `net/` (the rest of `utils.py`)
3. `calibration/` + `calvin/` (`mwax_calvin_utils.py`, including resolving
   the `KNOWN_CYCLES` entry with `mwax_hyperdrive_solutions`)

### Commit 1: core/ (complete)

| old | new |
|---|---|
| `mwax_command.py` (whole file) | `core/command.py` |
| `read_config`, `read_optional_config`, `read_config_list`, `read_config_bool` | `core/config.py` |
| `is_int`, `gigabyte_to_gibibyte`, `gigabytes_to_gigabits`, `bytes_to_gigabytes`, `get_gbps` | `core/units.py` |
| `get_gpstime_of_datetime`, `get_gpstime_of_now` | `core/gpstime.py` |
| `get_hostname`, `running_under_pytest` | `core/env.py` |
| `send_multicast`, `get_ip_address` | `net/multicast.py` (see below) |

`get_ip_address` reads like host/environment introspection by name alone, but
every call site resolves an interface name to an IP immediately before
passing it to `send_multicast()` -- it's multicast-interface resolution, not
generic env querying, so it went to `net/multicast.py` instead of `core/env.py`.

### Extraction, done twice

First pass extracted each function with `ast.get_source_segment`. That drops
anything outside the node's own span: a trailing same-line comment
(`return round(utc_datetime.gps)  # type: ignore[arg-type]` in
`get_gpstime_of_datetime`) and two standalone comment lines directly above a
`def` (`# For a given datetime...`, `# Return the GPS seconds...` above the
two gpstime functions) were silently dropped. Found by inspecting the
diff before touching callers, not by a later test failure.

Redone by taking full source *lines* (`node.lineno` to `node.end_lineno`,
whole lines rather than column-sliced) plus a check for a comment-only line
immediately preceding the node. Re-verified every moved symbol's exact
original text -- comments included -- is present verbatim in its new file
before deleting anything from `utils.py`.

### Two new bare-module-import collisions

Same category of bug as `mwacache_archive_processor.py`'s logging `handler`
in Phase 2, found the same way (checking call sites before choosing an import
style, not after):

- `utils.copy_subfile_to_disk_dd()` has a local variable named `command` (the
  shell command string being built). A bare `from mwax_mover.core import
  command` would be shadowed by it, turning `command.run_command_ext(command,
  ...)` into a string method lookup. Used the direct import everywhere in
  `utils.py` instead -- checked first that no local variable anywhere in the
  file was named `run_command_ext`.
- Every CLI file's config-reading code assigns `config = ConfigParser()` and
  passes it as the first argument to `utils.read_config(config, ...)`,
  dozens of times per file. A bare `from mwax_mover.core import config` would
  be shadowed by that local on essentially every call site. Used direct-name
  imports (`read_config`, `read_config_bool`, etc.) for the whole config
  family everywhere, rather than mixing styles file-by-file.

### Reconnaissance missed call sites; the sweep caught them

Initial per-function caller search (grepping each moved name against files
already known to import `mwax_command`/`utils`) missed `send_multicast` in
four CLI files, `get_hostname`/`running_under_pytest` in two more, and one
`gigabyte_to_gibibyte` call -- all real, all would have been `NameError` at
runtime. Caught by the same whole-repo word-boundary sweep used at the end of
Phase 1 and Phase 2, run *before* the gates rather than assumed unnecessary
because "the caller list was already built." Re-ran the sweep a second time
after fixing the first batch, and a third covering direct-name imports and
mock.patch strings, until all three came back empty.

Verified: ruff check, ruff format --check, ty check src/ tests/ all clean.
tests/test000_architecture.py: all 5 pass. Full test suite: 456 passed, 5
deselected, 0 failed -- identical to the Phase 2 baseline.

### Commit 2: fits/ + filesystem/ + net/ (complete)

The rest of `utils.py` -- 55 top-level symbols (45 defs/classes, 7 `PSRDADA_*`
constants, `metafits_file_lock`) -- split by domain rather than mechanism:

| old | new |
|---|---|
| `download_metafits_file`, `get_metafits_value`, `get_metafits_value_from_hdu`, `get_metafits_values` | `fits/metafits.py` |
| `CorrelatorMode`, the 7 `PSRDADA_*` constants, `inject_subfile_header`, `inject_beamformer_headers`, `read_subfile_value(s)`, `read_subfile_trigger_value`, `write_mock_subfile(_from_header)`, `process_mwax_stats`, `load_psrdada_ringbuffer`, `run_mwax_packet_stats`, `copy_subfile_to_disk_dd` | `fits/subfile.py` |
| `ValidationData`, `MWADataFileType`, `ArchiveLocation`, `metafits_file_lock`, `validate_filename`, `determine_bucket`, `get_bucket_name_from_filename/obs_id`, `should_project_be_archived`, `extract_channels_from_filename`, `get_priority`, `get_data_files_for_obsid_from_webservice`, `get_data_files_with_hostname_for_obsid_from_webservice` | `filesystem/naming.py` |
| `scan_directory`, `scan_for_existing_files_and_add_to_queue` | `filesystem/scan.py` |
| `remove_file`, `delete_files_older_than`, `extract_tar`, `get_png_dimensions`, `do_checksum_md5` | `filesystem/files.py` |
| `call_webservice` | `net/webservice.py` |
| `push_message_to_redis` | `net/redis.py` |
| `rclone_move`, `parse_rclone_stats`, `rclone_delete_file`, `check_remote_file_exists` | `net/s3.py` |
| `GiantSquidException`, `GiantSquidMWAASVOOutageException`, `GiantSquidJobAlreadyExistsException`, `run_giant_squid`, `extract_filename_from_mwa_asvo_signed_url` | `net/asvo.py` (new -- not in the original target structure; ASVO/giant-squid job submission didn't fit `fits`/`filesystem`/`net`'s existing three-file split for `net`, so it got its own file rather than being wedged into `webservice.py`) |

`utils.py` itself is now empty and has been deleted outright (not left as a
stub) -- every one of its 55 symbols has a home, so there was nothing to keep
it open for. `fits/hdu.py`, named in the target structure, is not created in
this commit: nothing in `utils.py` maps to it (the metafits HDU functions are
metafits-specific, not generic FITS-HDU utilities), so it's deferred until
something actually needs it.

Domain over mechanism, applied consistently: `process_mwax_stats`,
`load_psrdada_ringbuffer`, `run_mwax_packet_stats` and `copy_subfile_to_disk_dd`
are all thin `run_command_ext` wrappers around an external binary, which would
suggest `filesystem/files.py` by mechanism. But all four act specifically on
subfiles (per Greg's call), so they went to `fits/subfile.py` instead --
grouped with the PSRDADA header functions by what they operate on, not how
they're implemented. `do_checksum_md5` is the one external-binary wrapper that
stayed in `filesystem/files.py`, since it works on any file, not just subfiles.

### A cycle the target structure's own boundaries created

`get_data_files_for_obsid_from_webservice` and
`get_data_files_with_hostname_for_obsid_from_webservice` look like they belong
in `net/webservice.py` -- they're webservice queries. But they filter results
by `MWADataFileType`, and `download_metafits_file` (now in `fits/metafits.py`)
calls `net.webservice.call_webservice`, and `validate_filename` (now in
`filesystem/naming.py`) calls `download_metafits_file`. Putting the two
data-file-listing functions in `net/webservice.py` would have closed a
three-module cycle: `fits.metafits -> net.webservice -> filesystem.naming ->
fits.metafits`.

Same category of issue as Phase 1's upward import and Phase 3's
`mwax_calvin_utils`/`mwax_hyperdrive_solutions` cycle: the fix is to put the
function where its *dependency* points down, not where its *name* suggests.
Moved both functions into `filesystem/naming.py` instead -- they depend on
`MWADataFileType`, which already lives there, so the edge becomes
`filesystem.naming -> net.webservice` (a leaf dependency, no cycle).
`net/webservice.py` now contains only `call_webservice` itself. Caught before
writing any caller-fixing code, by tracing the three files' planned imports on
paper rather than after the architecture test failed.

### A decorator dropped by line-based extraction

The AST-extraction method (whole source lines, `node.lineno` to
`node.end_lineno`, used since Phase 3 commit 1 to avoid the trailing-comment
bug found there) has its own blind spot: `ast.FunctionDef.lineno` points at
the `def` line, not at any decorator above it, since decorators are a
separate `decorator_list` with their own line numbers. `remove_file`'s
`@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))` was silently
dropped by the first extraction pass -- and the AST-based split-verification
(symbol-set diff, body-equality diff) didn't catch it either, because both
the "old" and "new" sides of that comparison were built with the same
lineno-only extraction, so the dropped decorator wasn't a *mismatch*, it was
an omission both sides agreed on.

Caught by `ruff check` (`F401 tenacity.retry imported but unused` -- the
import was there, the only thing using it wasn't), not by the split
verification. Fixed the extraction method to take
`min(decorator.lineno for decorator in node.decorator_list, node.lineno)` as
the effective start line, re-ran extraction and verification from the
original file's last commit, confirmed the fix, and re-verified the fix
didn't affect any other symbol (only one decorated top-level definition
existed in the whole of `utils.py`).

Also caught by ruff: a missing `from enum import Enum` in `fits/subfile.py`
(`CorrelatorMode` needs it; the import list was hand-written per file rather
than mechanically carried over, and this one was missed).

### Import-site fixing, third time

Same three import forms, plus the same two gotchas (multi-name
`from mwax_mover import a, b, c` lists; `mock.patch("mwax_mover.<old>.<attr>")`
string targets) as Phase 1 and Phase 2, across 19 files this time
(`mwax_asvo_helper.py`, 5 processors, 6 CLI entry points, `db/data_files.py`,
3 queue files, and `mwax_calvin_utils.py`, plus 6 test files). One test-side
wrinkle not seen before: three `mock.patch` targets
(`mwax_mover.queues.priority_watcher.utils.get_priority`,
`mwax_mover.queues.watcher.utils.scan_for_existing_files_and_add_to_queue`,
`mwax_mover.cli.mwax_calvin_controller.utils.rclone_move`) patch a name
*inside* the importing module's own namespace, which only works because that
module previously did `from mwax_mover import utils` and called
`utils.the_function(...)`. Switching to a direct-name import
(`from ... import the_function`) moves the patch target too --
`mwax_mover.queues.watcher.utils.scan_for_existing_files_and_add_to_queue`
becomes `mwax_mover.queues.watcher.scan_for_existing_files_and_add_to_queue`
-- easy to miss since the string still imports and still patches *something*,
just not the call site the test thinks it's patching, so a stale target here
fails as a silent no-op mock rather than an import error.

Also fixed stale prose pointers in files this commit already touched (not a
Phase 5 sweep): `update_calvin_plots_and_index.py` and `mwax_calvin_utils.py`
each had a `:func:`-style cross-reference to `mwax_mover.utils.get_png_dimensions`,
and two test docstrings (`test016_calvin_controller.py`, `test019_priority_watcher.py`)
described mocking `utils.rclone_move`/`utils.get_priority` in prose.

`tests/test005_utils.py` (the original test file for `utils.py`) was not
split into per-new-module test files for this commit -- its docstring now
explains why: moving test modules to mirror the package structure is called
out in this doc as a separate, can-happen-any-time task, not something to
fold into a source-restructure commit. Its imports and `utils.` call sites
were updated in place; the file still tests the same functions, now imported
from their new homes.

Verified: ruff check, ruff format --check, ty check src/ tests/ all clean.
tests/test000_architecture.py: all 5 pass (including the new `net.asvo` and
`filesystem.naming` layer assignments, and no new upward imports or cycles).
Full test suite: 456 passed, 5 deselected, 0 failed -- identical to the
Phase 2 baseline, on a 16-minute run dominated by test020 as expected.

### Commit 3: calibration/ + calvin/ (complete)

`mwax_calvin_utils.py` -- 49 top-level symbols (46 defs/classes, 3 module
constants) -- split into calibration/ (leaf, no dependency on
mwax_hyperdrive_solutions) and calvin/ (depends on it):

| old | new |
|---|---|
| `MWA_NUM_COARSE_CHANS`, `Tile`, `Input`, `ChanInfo`, `TimeInfo`, `Metafits`, `PhaseFitInfo`, `GainFitInfo` | `calibration/models.py` |
| `read_solutions_hdu_complex`, `read_results_hdu`, `read_tiles_hdu`, `read_baseline_tile_flags` | `calibration/solutions.py` (not `fits/` -- see below) |
| `pad_gains_to_full_coarse`, `pad_gain_fit_info`, `ensure_system_byte_order`, `parse_csv_header`, `wrap_angle`, `_MIN_CLIP_THRESHOLD_RAD`, `_phase_fit_hess_inv`, `fit_phase_line`, `fit_gain`, `poly_str`, `textwrap` | `calibration/fitting.py` |
| `iterative_poly_clip(_batch)`, `reject_outliers`, `annotate_phase_outliers`, `pivot_phase_fits` | `calibration/outliers.py` |
| `CalvinJobType`, `write_readme_file` | `calvin/pipeline.py` (new seed file -- see below) |
| `create_sbatch_script`, `submit_sbatch`, `count_slurm_asvo_jobs` | `calvin/slurm.py` |
| `run_birli`, `estimate_birli_output_bytes` | `calvin/birli.py` |
| `run_hyperdrive`, `write_hyperdrive_stats` | `calvin/hyperdrive.py` (new seed file -- see below) |
| `get_solution_fits_filename`, `parse_solution_channels`, `get_sorted_solution_files`, `get_file_description`, `generate_plot_index_file`, `populate_index_json_entry`, `export_calibration_solutions`, `STAGING_DIR_PREFIX`, `get_staging_path`, `reap_orphaned_staging_dirs`, `upload_plot_files`, `get_convergence_summary` | `calvin/solution_files.py` |

`mwax_calvin_utils.py` is now empty and deleted outright, same as `utils.py`
in commit 2.

**Scope, decided up front:** the doc's own Phase 3 plan says "calibration/ +
calvin/ from `mwax_calvin_utils.py`" -- it does not say `mwax_hyperdrive_solutions.py`,
`mwax_calvin_solutions.py`, or `mwax_asvo_helper.py` move too, only that the
*cycle* with `mwax_hyperdrive_solutions` gets resolved. Confirmed with Greg
before writing anything: those three files (and `mwax_calvin_plots.py`) stay
where they are for now; only their imports change. This left two clusters of
symbols (`run_hyperdrive`/`write_hyperdrive_stats`, and `CalvinJobType`/
`write_readme_file`) without an existing target-structure file to land in,
since their natural homes (`calvin/hyperdrive.py`, a merge of run-hyperdrive
logic with `mwax_hyperdrive_solutions.py`'s solution-reading logic; and
`calvin/pipeline.py`, eventually `mwax_calvin_solutions.py` itself) don't
exist yet as full files. Created both as small seed files instead, containing
only what came from `mwax_calvin_utils.py` -- to be filled in when the bigger
files actually move, per Greg's go-ahead.

**One correction to the doc's own plan:** it says the shared primitives
dissolving the cycle split "down into `fits/` and `calibration/fitting.py`."
Having read them, `read_solutions_hdu_complex`/`read_results_hdu`/
`read_tiles_hdu`/`read_baseline_tile_flags` are specifically about
hyperdrive's SOLUTIONS/RESULTS/TILES/BASELINES HDU formats -- not generic
FITS the way `fits/metafits.py` is. All four went to `calibration/solutions.py`
instead, confirmed with Greg.

### The cycle, and how it actually broke

`mwax_calvin_utils.get_convergence_summary()` did a function-local import of
`HyperfitsSolution` from `mwax_hyperdrive_solutions.py` specifically to dodge
a module-level circular dependency, since `mwax_hyperdrive_solutions.py`
itself imported `ChanInfo`/`GainFitInfo`/`Metafits`/`PhaseFitInfo`/
`annotate_phase_outliers`/`ensure_system_byte_order`/`fit_gain`/
`fit_phase_line`/`iterative_poly_clip_batch`/the 4 HDU readers from
`mwax_calvin_utils.py` at module level.

Moving those shared primitives into `calibration/` -- which has no reason to
ever import `mwax_hyperdrive_solutions.py` -- let `mwax_hyperdrive_solutions.py`
import them from there instead, a one-directional edge. Moving
`get_convergence_summary` into `calvin/solution_files.py` -- which already
needs to import `HyperfitsSolution` for exactly this function -- let that
import move to module level too, since nothing in `calibration/` or
`mwax_hyperdrive_solutions.py` needs anything from `calvin/`. The
function-local import and its explanatory comment were removed as part of
this fix (the one deliberate content edit in this commit, beyond pure
lift-and-shift, called for explicitly by the doc's own plan). `KNOWN_CYCLES`
in `tests/test000_architecture.py` is now empty; the entry is deleted per the
ratchet rule rather than left in place.

Verified by running `tests/test000_architecture.py` after the fix: all 5
checks pass with `KNOWN_CYCLES` empty, meaning `test_no_unexpected_cycles`
found no cycle anywhere in the import graph -- not just this one instance,
which is the actual guarantee the ratchet gives.

### A duplicate function, found and left alone

`mwax_calvin_plots.py` already has its own `write_hyperdrive_stats` --
byte-for-byte identical to the one moved out of `mwax_calvin_utils.py` into
`calvin/hyperdrive.py`. Two independent copies existed before this commit;
they still do after it, in different files now. Per the standing instruction
to track refactor/reduce opportunities without acting on them mid-restructure,
this is noted here rather than deduplicated. `cli/cal_utils.py` imports its
copy from `mwax_calvin_plots.py`, unaffected by this commit.

### Import-site fixing, fourth time

Same three import forms as every previous phase, across 14 files this time
(`mwax_hyperdrive_solutions.py`, `mwax_calvin_plots.py`, `mwax_calvin_solutions.py`,
`net/s3.py`, `processors/subfile_incoming.py`, 6 CLI entry points, plus 5 test
files). Three bare-module call sites in `cli/mwax_calvin_processor.py`
(`mwax_calvin_utils.generate_plot_index_file/run_birli/run_hyperdrive`) were
switched to direct-name calls, consistent with every prior phase's convention.
`tests/test014_calvin_utils.py` (the original test file for
`mwax_calvin_utils.py`) had its imports rebuilt across six new modules, its
docstring updated, and ~13 lines of already-commented-out dead code
(`split_aocal_file_into_coarse_channels`, `get_aocal_filename` -- functions
that don't exist anywhere in the current codebase) deliberately left alone,
being unrelated to this restructure.

Four missing imports were caught by ruff check rather than by the split
verification, since they're new *cross-file* wiring the verification
doesn't check (it only confirms each moved symbol's body is unchanged, not
that its new file can resolve every name that body uses): `NDArray` in
`calibration/models.py` (needed by `ChanInfo`'s type hint); `Metafits` and
`numpy` in `calvin/birli.py` (both were same-file neighbours in the old
`mwax_calvin_utils.py`, invisible as separate dependencies until split);
`datetime` and a `logger` in `calvin/pipeline.py`, for the same reason.

Verified: ruff check, ruff format --check, ty check src/ tests/ all clean.
tests/test000_architecture.py: all 5 pass, `KNOWN_CYCLES` empty. Full test
suite: 456 passed, 5 deselected, 0 failed -- identical to the Phase 2
baseline, on a 20-minute run (the slowest yet, still dominated by test020).

## Phase 4: mwax_calvin_plots.py + mwax_hyperdrive_solutions.py + mwax_calvin_solutions.py into calvin/ (complete)

Originally scoped as just splitting `mwax_calvin_plots.py` (2377 lines, 41
symbols) into `calvin/plots/{layout,phase_fits,hyperdrive_plots,gains,
stats_table,index}.py`, per the target structure. Widened before writing
any code, per Greg's call: `mwax_hyperdrive_solutions.py` (8 symbols) and
`mwax_calvin_solutions.py` (`process_solutions`, the whole file) also fold
into `calvin/` in this same commit, rather than waiting for a phase that
was never separately scheduled.

| old | new |
|---|---|
| `plot_dpi`, `plot_figsize`, plus their 2 `_PYTEST_PLOT_*` constants | `calvin/plots/layout.py` |
| `plot_debug_phase_fits`, `plot_rx_lengths`, `plot_phase_fits`, `plot_phase_intercepts`, `plot_phase_residual` | `calvin/plots/phase_fits.py` |
| `generate_hyperdrive_plots(_for_files)` | `calvin/plots/hyperdrive_plots.py` |
| `plot_combined_gains`, `plot_outlier_gains`, ~19 private helpers/constants (stitching, paging, memory-budgeted rendering) | `calvin/plots/gains.py` |
| `build_tile_stats_rows`, `write_tile_stats_table` | `calvin/plots/stats_table.py` |
| `generate_plot_index_file`, `populate_index_json_entry`, `get_file_description` (relocated a second time -- see below) | `calvin/plots/index.py` |
| all of `mwax_hyperdrive_solutions.py` (`TileFlagReason`, `ChannelFlagReason`, 4 private helpers, `HyperfitsSolution`, `HyperfitsSolutionGroup`), plus `get_convergence_summary` (relocated a second time) | `calvin/hyperdrive.py` (merged with the existing `run_hyperdrive`/`write_hyperdrive_stats` seed) |
| `process_solutions` (all of `mwax_calvin_solutions.py`) | `calvin/pipeline.py` (merged with the existing `CalvinJobType` seed) |

`mwax_calvin_plots.py`, `mwax_hyperdrive_solutions.py`, and
`mwax_calvin_solutions.py` are all deleted outright.

### Three decisions made before writing any code

Confirmed with Greg first, since each changes the shape of the commit
rather than just where a symbol lands:

- **Deduplicate `write_hyperdrive_stats`.** `mwax_calvin_plots.py` had its
  own copy, byte-for-byte identical to the one already in `calvin/hyperdrive.py`
  from commit 3 -- noted then, not acted on. Checking actual callers before
  choosing which copy to keep (rather than assuming the newer one) found
  both real call sites (`mwax_calvin_solutions.py`, `cli/cal_utils.py`)
  used the `mwax_calvin_plots.py` copy; the commit-3 copy in
  `calvin/hyperdrive.py` had zero callers. Kept the `calvin/hyperdrive.py`
  copy (its home was always going to be right, once
  `mwax_hyperdrive_solutions.py` merged in beside it) and repointed both
  callers to it.
- **`generate_plot_index_file`/`populate_index_json_entry`/`get_file_description`
  move a second time**, from `calvin/solution_files.py` (commit 3's best
  guess, before `calvin/plots/` existed) to the new `calvin/plots/index.py`
  -- the "index" slot the target structure's `plots/{...,index}` entry was
  naming all along.
- **Split `write_stats_and_debug_plots` by concern, not by file.** Rather
  than picking one of `calvin.plots.stats_table`/`calvin.plots.phase_fits`
  to own the whole function and importing the other's half in, it became
  two functions: `write_before_after_stats()` (stats_table.py) and
  `write_debug_phase_fit_plots()` (phase_fits.py), the latter taking the
  former's return value (the final annotated phase-fit DataFrame) as a
  parameter instead of recomputing it. This is the one place in this
  commit that is not a pure lift-and-shift -- the function's actual logic
  is unchanged, but its signature and call sites are new. Updated 2
  production call sites (`calvin/pipeline.py`'s `process_solutions`,
  `cli/cal_utils.py`'s `run_pipeline`) and the dedicated regression test
  (`test022_hyperdrive_solutions.py::test_write_before_after_stats_reuses_final_phase_fit_without_recomputing`,
  renamed from `test_write_stats_and_debug_plots_...`) to call both
  functions in sequence, preserving the original ordering (stats first,
  then plots) and the original behaviour the regression test guards:
  `process_phase_fits` must not be called again for the "after" state.

### Two new cycles, both created by this merge itself

Neither existed before this commit; both came from putting two things in
the same file that each needed something the other file provided:

- **`calvin.pipeline` <-> `calvin.hyperdrive`.** Merging `process_solutions`
  into `calvin/pipeline.py` meant it now needed `HyperfitsSolution`/
  `HyperfitsSolutionGroup`/`write_hyperdrive_stats` from `calvin/hyperdrive.py`.
  But `calvin/hyperdrive.py`'s `run_hyperdrive` needed `write_readme_file`
  from `calvin/pipeline.py`. Fixed by moving `write_readme_file` into
  `core/command.py` -- it has no calvin-specific logic (just logs a
  command's outcome to a file) and `core.command` already owns
  `run_command_ext`/`run_command_popen`, the thing it's logging the
  outcome of, so it has no reason to ever import from `calvin` and the
  cycle can't recur. Updated both callers (`calvin/birli.py`,
  `calvin/hyperdrive.py`) to import it from there instead.
- **`calvin.hyperdrive` <-> `calvin.solution_files`.** Merging
  `HyperfitsSolution` into `calvin/hyperdrive.py` meant
  `calvin/solution_files.py`'s `get_convergence_summary` (which
  instantiates `HyperfitsSolution`) now imported from `calvin.hyperdrive`,
  while `calvin/hyperdrive.py`'s `write_hyperdrive_stats` (its only other
  caller) imported `get_convergence_summary` back from `calvin.solution_files`.
  Fixed by moving `get_convergence_summary` into `calvin/hyperdrive.py`
  too, right next to `HyperfitsSolution` and its only caller -- same fix
  as commit 3's original cycle, one file up. `calvin/solution_files.py`
  no longer imports anything calvin-hyperdrive-related at all.

Both found by tracing the planned imports on paper before writing any
caller-fixing code, same as commit 2's `net.webservice` cycle -- not by
the architecture test failing first.

### A backend-selection guard, now needed twice

`mwax_calvin_plots.py` had a single `mpl.use("Agg")` call, positioned
between `import matplotlib as mpl` and `import matplotlib.pyplot as plt`
specifically because backend selection locks in at pyplot's first import
anywhere in the process, and this pipeline never displays a figure
interactively. Splitting the file put `from matplotlib import pyplot as plt`
into two independent modules (`calvin/plots/phase_fits.py`,
`calvin/plots/gains.py`) that may now be imported in either order, so the
guard was duplicated into both, in the same position relative to the
pyplot import, rather than centralised in `calvin/plots/layout.py` --
centralising it would only have helped if every consumer's own import
block happened to reach the `layout` import before its own `pyplot`
import, which the normal (alphabetical, local-imports-last) import-block
convention does not guarantee. Calling `mpl.use("Agg")` twice with the
same backend, before either module's own first figure, is safe.
`calvin/plots/hyperdrive_plots.py` needed no such guard -- it never
imports pyplot at all.

### A stats-table function's missing neighbours

`build_tile_stats_rows` (now in `calvin/plots/stats_table.py`) calls
`_format_flavor`, `_tile_flag_reason_text`, and `_channel_reason_counts_text`
-- all three defined in `calvin/plots/gains.py`, since the original
grouping only tracked which *file* a function moved to, not which other
functions in the same old file it still called across the new split.
Caught by `ty check` (`unresolved-import`... actually `unresolved-reference`
inside the function body), not by the split verification, for the same
reason as commit 3's four missing imports: verification confirms a body's
*text* is unchanged, not that the file around it can resolve every name
that text uses. Fixed with a same-layer import
(`calvin.plots.stats_table -> calvin.plots.gains`, one direction only, no
cycle).

### Import-site fixing, fifth time

Same three import forms as every previous phase. `cli/generate_index_json.py`
and `tests/test021_gen_index_json.py` both still pointed at
`calvin.solution_files.generate_plot_index_file` -- commit 3's location --
after it moved a second time to `calvin.plots.index` in this commit; a
grep sweep for the *old module names* (`mwax_calvin_plots`, etc.) doesn't
catch a symbol that moved between two *new* locations, so this pair was
caught by `ty check`, not the sweep. A useful reminder for any future
phase that relocates something a second time: the sweep needs to check
the symbol's immediately-prior location too, not just its original one.

~35 `mock.patch` string targets across `test020_calvin_solutions.py`,
`test022_hyperdrive_solutions.py`, and `test023_calvin_plots.py` were
updated to their new module paths, each verified by comparing the
occurrence count of the old and new target strings before and after the
substitution (not just spot-checking a few), the same discipline as
commit 2's `checksum_and_db` mock-patch fixes at similar scale.

Verified: ruff check, ruff format --check, ty check src/ tests/ all clean.
tests/test000_architecture.py: all 5 pass -- `test_no_unexpected_cycles`
in particular, confirming both new cycles found during this commit are
actually gone, not just hidden by the ratchet. Full test suite: 456
passed, 5 deselected, 0 failed -- identical to every prior baseline, on a
20-minute run.

## mwax_asvo_helper.py -> calvin/asvo.py (complete)

The last file named in the target structure's `calvin/` list that hadn't
moved. Small and clean by comparison with Phase 4: 5 symbols
(`MWAASVOJobState`, `MWAASVOJob`, `MWAASVOHelper`,
`get_job_id_from_giant_squid_stdout`, `get_job_info_from_giant_squid_json`),
already importing only from `net/asvo.py` (a lower layer), no decorators,
no module-level constants beyond `logger`. One-for-one lift-and-shift,
verified byte-identical (5/5 symbols) the same way as every prior split.

Distinct from `net/asvo.py` (Phase 3 commit 2): that one is the
low-level `run_giant_squid` CLI wrapper and its exceptions; this one is
the job-tracking layer built on top of it (submitting, polling, retrying
through an outage).

### A same-named instance attribute, correctly left alone

`cli/mwax_calvin_controller.py` has both a bare module import
(`from mwax_mover import mwax_asvo_helper`) and an instance attribute
`self.mwax_asvo_helper` (an `MWAASVOHelper` instance) -- the exact shape
of the bare-module-import collision flagged as a gotcha in earlier
phases (`command`, `handler`, `config`). Not a real collision here,
though: `self.mwax_asvo_helper` and the bare name `mwax_asvo_helper` are
different namespaces, so switching the bare module import to direct-name
imports (`MWAASVOHelper`, `MWAASVOJobState`, `GiantSquidMWAASVOOutageException`)
doesn't touch the attribute at all. Fixed with a regex negative
lookbehind (`(?<!self\.)\bmwax_asvo_helper\.`) rather than a plain
substring replace, specifically so it could not touch the 13
`self.mwax_asvo_helper.*` attribute accesses -- confirmed by count (6
bare references replaced, 13 attribute accesses untouched) rather than
by inspection alone.

Verified: ruff check, ruff format --check, ty check src/ tests/ all clean
on the first pass -- no missing imports this time, unlike commits 3 and 4.
tests/test000_architecture.py: all 5 pass. Full test suite: 456 passed,
5 deselected, 0 failed, on a 16-minute run.

## Phase 5: docs (complete)

The 115-prose-reference estimate from the original Migration surface table
turned out to be almost entirely paid down already: every phase's own
import-site sweep fixed the stale references it created as it went (each
phase's write-up above documents that). What was actually left, found by
grepping the whole repo for every renamed file's old name:

- **README.md's "Module Reference" section** -- the real bulk of this
  phase. ~115 lines describing every module by its pre-restructure flat
  filename. Rewritten in full, reorganised to mirror the current package
  tree (`core/`, `db/`, `fits/`, `filesystem/`, `net/`, `archive/`,
  `queues/`, `processors/`, `calibration/`, `calvin/` including
  `calvin/plots/`, `beamformer/`) rather than the old roughly-alphabetical
  flat list, with each module's description re-derived from its current
  docstring rather than copied forward unchanged.
- **CALVIN.md** -- two mentions of `pad_gain_fit_info in mwax_calvin_utils.py`
  (now `calibration/fitting.py`). Everything else in it already described
  behaviour and class/method names, not file paths, so needed no change.
- **`docs/img/make_illustrations.py`** -- a standalone script (not under
  `src/` or `tests/`, but still in `ruff check .`'s scope per
  `.github/workflows/ci.yml`) that imports `fit_phase_line`,
  `iterative_poly_clip_batch`, `reject_outliers` from the long-deleted
  `mwax_calvin_utils` to render CALVIN.md's illustration PNGs. Genuinely
  broken (not just stale prose) -- would `ImportError` if run. Fixed and
  actually re-run to confirm it still produces output; the regenerated
  PNGs were reverted afterward (not byte-identical to the committed ones,
  likely rendering/metadata noise rather than a real change, and
  regenerating images is not this phase's concern).
- **`.pre-commit-config.yaml` and `pyproject.toml`** -- both checked in
  full. Neither contains a single file-path reference; both are already
  entirely restructure-agnostic. Nothing to do.
- **`CHANGELOG.md`** -- deliberately left alone. Its entries describe
  past releases using the filenames that existed *at the time of each
  entry*; rewriting them to current names would misrepresent history
  rather than fix it, the opposite of what this phase is for.

Everything else the sweep found (in `docs/RESTRUCTURE.md` itself, and in
source docstrings/test module docstrings across `calvin/`,
`calibration/`, `core/command.py`, and five test files) was already an
intentional "moved from X" / "used to live in X" historical note, not a
stale claim about current structure -- confirmed by reading each hit's
surrounding context individually rather than assuming a name match means
a fix is needed.

**Resolved:** README.md's "## mwax_mover command line tool" section
(usage, mode flags, a `./mwax_mover.py` invocation example) described a
CLI tool that no longer exists anywhere in `src/` -- confirmed by
grepping for its own documented flags (`WATCHDIR`, `WATCHEXT`,
`EXECUTABLEPATH`), which appear nowhere in the codebase, and it was
absent from `pyproject.toml`'s `[project.scripts]`. Predates the
restructure and is a different kind of problem than a stale path
reference, so raised with Greg rather than resolved unilaterally;
confirmed, then deleted -- the whole section, its horizontal-rule
separator (kept the one on the other side, so the doc still reads as
one rule between sections, not zero or two), and its bullet in the
intro's service list (also correcting "Five long-running services" to
"Four").

Verified: ruff check, ruff format --check, ty check src/ tests/ all
clean. tests/test000_architecture.py: all 5 pass. No src/ or tests/
files changed this phase, so the full suite was not re-run -- nothing
in it could have regressed.

## Remaining phases

**All five phases are now complete.** Every item originally scoped --
`core/`, `fits/`+`filesystem/`+`net/`, `calibration/`+`calvin/` in full
(including the `mwax_asvo_helper.py` -> `calvin/asvo.py` move), and the
docs pass -- is done; see the sections above.

**Still outstanding (test-side, can happen any time):** move test modules into a
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
