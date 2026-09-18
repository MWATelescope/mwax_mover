# mwax_mover: DipoleGains-aware ref tile selection, selection diagnostics, and before-plot reordering

Status: **in progress**

Four related enhancements to the Calvin reference-tile selection and
reporting, building on the existing Phase 1-3 work in
`docs/REF_TILE_SELECTION.md`.

Findings are against commit `main` HEAD.

---

## Ground rules

Same as always: never push/PR, ask before actions affecting other systems,
`ruff`/`ty` clean on every touched file, Google-style docstrings, one
phase per diff, run the relevant test module(s) for touched files (not the
full suite), new logic gets new unit tests.

---

## Enhancement 1 — DipoleGains-aware reference tile selection

### Background

The hyperdrive solution FITS file's `TILES` HDU has an optional
`DipoleGains` column: 32 float64 values per tile (first 16 for X
dipoles, second 16 for Y dipoles). Typically each value is 0.0 (dead
dipole) or 1.0 (alive). A "perfect" tile has all 32 values equal to
1.0 — no dead dipoles on either polarisation.

Today's `select_refant` scores tiles by `(gate_failures,
length_deviation, tile_id)`. It knows nothing about dipole health. A
tile with several dead dipoles can be chosen as reference if it happens
to pass the existing phase/gain quality gates and have a small length
deviation. This is undesirable because a dead-dipole tile's calibration
solution is inherently less trustworthy — the solve ran with incomplete
hardware — and using it as reference bakes that compromise into every
other tile's reference-normalised solution.

### What `DipoleGains` tells us

From the [hyperdrive solutions format
documentation](https://mwatelescope.github.io/mwa_hyperdrive/defs/cal_sols_hyp.html):

> `DipoleGains` contains the dipole gains used for each tile in
> calibration. There are 32 values per tile; the first 16 are for the X
> dipoles and the second 16 are for the Y dipoles. Typically, the values
> are either 0 (dead dipole) or 1.

The column is optional (older hyperdrive versions may not write it).

### Design

#### 1.1 Reader layer (`calibration/solutions.py`)

Add a new function:

```python
def read_dipole_gains(tiles_data) -> NDArray[np.float64] | None:
    """Read the DipoleGains column from a TILES HDU, if present.

    Returns:
        float64 array, shape (n_tiles, 32), ordered by ascending
        antenna index (matching read_tiles_hdu's sort order), or
        None if the DipoleGains column is not present in the HDU.
    """
```

This returns `None` rather than raising when the column is absent,
since older solution files won't have it and that's not an error.
The sort-by-antenna-index step matches `read_tiles_hdu`'s ordering so
the row indices align.

#### 1.2 `HyperfitsSolution` property

Add to `calvin/hyperfits_solution.py`:

```python
@property
def dipole_gains(self) -> NDArray[np.float64] | None:
    """Get per-tile dipole gains from the TILES HDU, if present.

    Returns:
        float64 array, shape (n_tiles, 32), ordered by antenna
        index, or None if the DipoleGains column is absent.
    """
    with fits.open(self.filename) as hdus:
        return read_dipole_gains(hdus["TILES"].data)
```

#### 1.3 `HyperfitsSolutionGroup` property

Add to `calvin/hyperfits_solution_group.py`:

```python
@property
def dipole_gains(self) -> NDArray[np.float64] | None:
    """Get per-tile dipole gains, consistent across all solution files.

    DipoleGains is per-tile metadata (not per-channel), so it should
    be identical in every solution file for one observation. This reads
    from the first file that has the column. Logs a warning if any
    other file disagrees.

    Returns:
        float64 array, shape (n_tiles, 32), or None if no solution
        file in the group has the DipoleGains column.
    """
```

#### 1.4 Dipole completeness metric

For each tile, compute:

```python
n_good_dipoles = int(np.sum(dipole_gains[tile_idx] == 1.0))
# Range: 0 to 32. A perfect tile scores 32.
n_dead_dipoles = 32 - n_good_dipoles
```

#### 1.5 Integration into `select_refant`

Add dipole health as **both a quality gate and a ranking tiebreaker**:

**New gate** — "dipole completeness": a tile fails this gate if fewer
than 30 of its 32 dipole gains are 1.0 (i.e. `n_good_dipoles < 30`).
This allows tiles with 1-2 dead dipoles to pass the gate while still
catching tiles with significant hardware degradation. If no tile in
the observation has ≥30 good dipoles, every tile fails this gate
equally and it effectively becomes a no-op (the "fewest-failures"
ranking handles the degradation gracefully, same as the existing
gates). The continuous `n_dead_dipoles` ranking dimension (below) still
discriminates among tiles that pass the gate — a 32/32 tile sorts
ahead of a 31/32 tile even though both pass.

**New ranking dimension** — `n_dead_dipoles` inserted between
`failures` and `length_deviation` in the sort tuple:

```python
# Before (current):
scored.append((failures, length_deviation, tile_id))

# After (proposed):
scored.append((failures, n_dead_dipoles, length_deviation, tile_id))
```

This means:
1. Tiles with fewer gate failures are preferred (unchanged).
2. Among tiles with equal failures, those with fewer dead dipoles win.
3. Among tiles with equal failures *and* equal dead dipoles, smaller
   length deviation wins.
4. Tile ID breaks any remaining tie (unchanged).

**When DipoleGains is absent** (older solution files): skip both the
gate and the ranking dimension — `n_dead_dipoles` defaults to 0 for
all tiles, and no gate failure is counted. The existing behaviour is
preserved exactly.

#### 1.6 New constant

```python
# constants.py
REFTILE_DIPOLE_GAINS_EXPECTED = 32  # Total dipole gains per tile (16 X + 16 Y)
REFTILE_DIPOLE_GOOD_MIN = 30        # Gate threshold: min good dipoles (== 1.0) to pass
```

---

## Enhancement 2 — Reference tile selection diagnostics in stats file

### Background

The `{obs_id}_stats.txt` file currently contains BEFORE/AFTER per-tile
stats tables and per-file hyperdrive convergence stats. There is no
record of *why* a particular reference tile was chosen, making it hard
to verify the selection is working as intended or debug unexpected
choices.

### Design

#### 2.1 New function: `format_refant_selection_report`

Add to `calvin/hyperfits_solution_group.py` (or a new
`calvin/refant_report.py` if we prefer to keep the group class lean —
Greg's call):

```python
def format_refant_selection_report(
    scored: list[tuple],
    tile_names: NDArray,
    tile_ids: NDArray,
    dipole_gains: NDArray[np.float64] | None,
    gate_details: dict[int, dict],
    median_length: dict[str, float],
    winner_tile_id: int,
) -> str:
    """Format a human-readable report of the ref tile selection ranking.

    Shows each candidate tile's gate pass/fail status, dipole health,
    length deviation, and overall rank, with the chosen tile highlighted.
    """
```

The `gate_details` dict (keyed by tile_id) captures per-tile
gate-by-gate pass/fail results so the report can show *which* gates
each tile failed, not just the failure count.

#### 2.2 Capture gate details during `select_refant`

Extend the scoring loop in `select_refant` to record per-tile details:

```python
gate_details[tile_id] = {
    "phase_quality_ok": bool,     # worst-of-XX/YY >= 0.8
    "phase_chi2dof_ok": bool,     # both pols in [0.2, 3.0]
    "gain_quality_ok": bool,      # worst-of-XX/YY >= 0.8
    "dipole_complete": bool,      # all 32 == 1.0 (or N/A)
    "n_good_dipoles": int,        # 0-32 (or 32 if N/A)
    "length_dev_xx": float,       # |length_xx - median_xx|
    "length_dev_yy": float,       # |length_yy - median_yy|
    "length_deviation": float,    # max of XX/YY
    "failures": int,
    "phase_quality_xx": float,    # actual value for display
    "phase_quality_yy": float,
    "phase_chi2dof_xx": float,
    "phase_chi2dof_yy": float,
    "gain_quality_xx": float,
    "gain_quality_yy": float,
}
```

#### 2.3 Return selection metadata from `select_refant`

Change `select_refant`'s return type from `Series` to a richer result.
Two options:

**Option A — return a named tuple / dataclass alongside the Series:**

```python
@dataclass
class RefantSelectionResult:
    tile: Series             # The chosen tile (same as current return)
    report: str              # Human-readable selection report text
    scored: list[tuple]      # The full ranking for programmatic use
```

**Option B — store the report on `self` and return `Series` as before:**

```python
# In select_refant, after scoring:
self._refant_selection_report = format_refant_selection_report(...)
# Return Series as before
```

Option B is simpler and avoids changing every call site. The report is
only consumed by the stats file writer and logging, both of which
already have access to the group object. **Proposing Option B.**

#### 2.4 Write to stats file

In `pipeline.py` and `cli/cal_utils.py`, write the report to the stats
file *before* the BEFORE/AFTER tables:

```python
stats_fd.write(soln_group.refant_selection_report)
stats_fd.write("\n")
```

#### 2.5 Example output

```
Reference tile selection for 1234567890:
  Bootstrap ref: Tile042 (ant 42)
  DipoleGains: available (from solution files)
  Median cable length: XX=4.231m  YY=4.198m
  Gates: phase_quality>=0.8  phase_chi2dof in [0.2,3.0]  gain_quality>=0.8  all_dipoles_alive

  Rank  Tile   Name       Ant  Fail  Dipoles  LenDev   PhQ_XX  PhQ_YY  Chi2_XX Chi2_YY  GnQ_XX  GnQ_YY  Gates
  ----  -----  ---------  ---  ----  -------  -------  ------  ------  ------- -------  ------  ------  -----
  1*    51     Tile051     51     0    32/32    0.012    0.95    0.93    1.21    1.18     0.97    0.96    PPPP
  2     73     Tile073     73     0    32/32    0.034    0.92    0.91    0.98    1.05     0.94    0.93    PPPP
  3     12     Tile012     12     0    32/32    0.089    0.88    0.86    1.45    1.52     0.91    0.90    PPPP
  4     99     Tile099     99     1    31/32    0.015    0.94    0.92    1.10    1.08     0.95    0.94    PPP.
  5     42     Tile042     42     1    32/32    0.002    0.72    0.93    1.01    0.99     0.96    0.95    .PPP
  ...

  Chosen: Tile051 (ant 51, id 51) — 0 gate failures, 32/32 dipoles, length deviation 0.012
```

Where Gates column uses `P` (pass) / `.` (fail) for
[phase_quality, phase_chi2dof, gain_quality, dipole_complete].

---

## Enhancement 3 — Reorder "before" hyperdrive plots to use final ref tile

### Background

Currently in both `pipeline.py` and `cli/cal_utils.py`, the "before"
hyperdrive binary plots (`hyperdrive solutions-plot`) run *before*
`run_flagging_pipeline()`, using the initial `refant["ant"]` from
`select_refant()`. The "after" plots run after `commit()`, using the
(potentially updated) `refant["ant"]`.

If `run_flagging_pipeline()` invalidates the original refant (e.g. a
diverged tile NaN'd by `flag_gain_max_cutoff`) and re-selects a
replacement, the before and after plots end up using *different*
reference tiles. This makes visual before/after comparison misleading.

### Design

Move the "before" `hyperdrive.generate_plots_for_files(...,
before=True)` call to **after** `run_flagging_pipeline()` returns but
**before** `commit()`.

This is safe because:

1. The "before" plots are read-only: they invoke `hyperdrive
   solutions-plot` on the on-disk solution files. Nothing in
   `run_flagging_pipeline()` writes to disk — it only mutates
   `self.jones` in memory. The on-disk files are still pristine.
2. `commit()` is what writes the modified jones back to disk (creating
   `.original.fits` backups first). So the on-disk files remain
   untouched up to that point.
3. Both before and after plots will now use the same `refant["ant"]` —
   the final one, after any re-selection.

#### 3.1 `pipeline.py` — new ordering

```
Current:                              Proposed:
─────────────────────────             ─────────────────────────
1. select_refant                      1. select_refant
2. "Before" hyp plots (initial ref)   2. run_flagging_pipeline
3. run_flagging_pipeline               3. Update refant if changed
4. Update refant if changed            4. "Before" hyp plots (final ref)  ← moved
5. plot_outlier_gains                  5. plot_outlier_gains
6. commit()                            6. commit()
7. "After" hyp plots (final ref)      7. "After" hyp plots (final ref)
8. Stats file + phase plots            8. Stats file + phase plots
```

#### 3.2 `cli/cal_utils.py` — same change

The `run_pipeline()` function in `cal_utils.py` has the same ordering
as `pipeline.py` and gets the same reordering.

#### 3.3 No new tests needed

This is a pure ordering change in the two top-level orchestration
functions. The before plots use the same read-only
`generate_plots_for_files` function as before — only the call site
moves. The existing integration test coverage (if any) exercises the
full pipeline; no new unit test logic is needed for "we call the same
function with the same arguments, just later".

---

## Implementation plan

### Phase 1 — DipoleGains reader + `select_refant` integration

**Files touched:**
- `src/mwax_mover/calibration/solutions.py` — add `read_dipole_gains()`
- `src/mwax_mover/calvin/hyperfits_solution.py` — add `dipole_gains` property
- `src/mwax_mover/calvin/hyperfits_solution_group.py` — add `dipole_gains` property; extend `select_refant` scoring loop with dipole gate + ranking dimension
- `src/mwax_mover/constants.py` — add `REFTILE_DIPOLE_GAINS_EXPECTED`
- `tests/` — new tests for `read_dipole_gains`, `HyperfitsSolution.dipole_gains`, group-level dipole_gains consistency, and `select_refant` with/without DipoleGains

**Commit split:** One commit for the reader layer (solutions.py +
hyperfits_solution.py + hyperfits_solution_group.dipole_gains + tests),
one commit for the select_refant integration (scoring changes +
constant + tests).

### Phase 2 — Selection diagnostics

**Files touched:**
- `src/mwax_mover/calvin/hyperfits_solution_group.py` — capture gate details in `select_refant`; store `_refant_selection_report`; add `refant_selection_report` property; add `format_refant_selection_report` (or new `calvin/refant_report.py`)
- `src/mwax_mover/calvin/pipeline.py` — write report to stats fd
- `src/mwax_mover/cli/cal_utils.py` — write report to stats fd
- `tests/` — test report formatting, test that report is populated after select_refant

**Commit split:** Single commit.

### Phase 3 — Before-plot reordering

**Files touched:**
- `src/mwax_mover/calvin/pipeline.py` — move "before" hyperdrive.generate_plots_for_files call
- `src/mwax_mover/cli/cal_utils.py` — same move

**Commit split:** Single commit (both files in one, since it's the
same logical change in both callers).

### Phase 4 — CALVIN.md documentation

**Files touched:**
- `docs/CALVIN.md` — new file: clear explanation of the Calvin
  calibration pipeline, with a detailed section on reference tile
  selection covering all gates, the dipole completeness metric, the
  ranking tuple, and the rationale for each step.

**Commit split:** Single commit.

### Phase ordering

**Order: 1 → 2 → 3 → 4** — get the scoring right first, then add
reporting that includes the new scoring, then reorder the plots, then
document the whole process.

---

## Confirmed decisions

1. **Dipole gate threshold**: ≥ 30/32 good dipoles to pass the gate
   (`REFTILE_DIPOLE_GOOD_MIN = 30`). Continuous `n_dead_dipoles`
   ranking still discriminates among tiles that pass.

2. **Report location**: Separate `calvin/refant_report.py` module
   (the group class is already 1400+ lines).

3. **Logging**: Report written to both `{obs_id}_stats.txt` AND
   logged at INFO level (visible in Slurm job logs).

4. **DipoleGains check**: Strict `== 1.0` (confirmed by real data:
   only 0.0 and 1.0 values observed). Dtype is float64 (`>f8`), not
   float32 as originally assumed from the hyperdrive docs.

5. **Selection report stored on `self`** (Option B): avoids changing
   every call site's return type.
