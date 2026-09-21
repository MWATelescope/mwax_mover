# mwax_mover reference-tile selection & --ref-tile plan

Status: **implemented.** Phases 1-2 (quality-aware `select_refant`) and
Phase 3 (`--ref-tile` on hyperdrive plots) are merged. Phase 3 was
implemented with `ref_tile: int` (antenna index) rather than the
`reftile: str` (tile name) originally planned below — hyperdrive's
`--ref-tile` flag takes an antenna index, not a name.

This does two related things: replaces `HyperfitsSolutionGroup.refant`'s
"lowest unflagged ID" rule with a quality-aware selection (Phases 1-2), and
threads the chosen tile through to hyperdrive's own `solutions-plot`
`--ref-tile` argument so its plots match calvin's internal ones (Phase 3).
Phase 3 depends on Phase 1/2 being in place first (it needs the final
reference tile known before the "before" plots run).

Findings are against commit `10d04a4` on branch `source_code_restructure`
("Added changelog for hyperdrive parallelisation...").

---

## Ground rules

Same as the previous plans in `docs/`: never push/PR, ask before actions
affecting other systems, `ruff`/`ty` clean on every touched file,
Google-style docstrings, one phase per diff, run the relevant test
module(s) for touched files (not the full suite), new logic gets new unit
tests.

---

## Background (from discussion, recorded here for the record)

- **Today's algorithm**: lowest-ID tile passing `combined_tile_flags`
  (metafits + TILES HDU + BASELINES-inferred). No calibration-quality
  signal at all.
- **Key subtlety**: `PhaseFitInfo.length` is fitted on *reference-
  normalised* solutions, so it's a *difference* from whichever tile was
  used as reference — not an absolute measurement. The reference tile's
  own row trivially fits to `length = 0`. Scoring by literal `|length|`
  would just re-select whatever reference was already used to compute it.
  **Resolution**: score by deviation from the *population median* of
  `length` instead of from zero — this is invariant to which reference
  tile happened to be used to compute the fits, so it doesn't matter that
  the number was computed relative to an arbitrary bootstrap choice.
- **What's usable pre-selection**: `process_phase_fits`/
  `process_gain_fits_for_db` are read-only (confirmed by reading their
  implementations — they don't mutate `self.jones`), so they're safe to
  run once against a cheap bootstrap reference purely to gather ranking
  data, before running the real (mutating) flagging pipeline once with the
  final choice. Per-tile flagged-channel counts and phase/amplitude
  outlier flags are populated only by that mutating pipeline, so they
  can't be used for *selection* — this is a real, accepted scoping limit,
  not an oversight.
- **Scoring**: staged filter-then-rank, not a weighted composite — rank by
  `(number of gates failed, length-deviation from median)`, ascending,
  tile ID as final tiebreak. This degrades gracefully when no tile passes
  every gate (the tile failing the *fewest* gates wins) rather than needing
  a separate "nothing qualified" branch.
- **Gates** (worst-of-XX/YY, i.e. a tile only as trustworthy as its worse
  polarisation): phase `quality` ≥ 0.8, phase `sigma_resid` ≤ 0.15 rad,
  gain `quality` ≥ 0.8.

  > **Update (supersedes the original design below).** The phase gate was
  > originally `chi2dof` in `[0.2, 3.0]`. That was wrong: `PhaseFitInfo.chi2dof`
  > is `Σresidual²/(N−2)` with residuals in radians and no per-channel noise
  > normalisation, so it is the mean-square phase residual (~a few ×10⁻³ rad²
  > for a good fit), not a reduced chi-square near 1 — the `[0.2, 3.0]` range
  > rejected essentially every good tile. It is now an upper-bound gate on
  > `sigma_resid` (the residual RMS in radians, unweighted), which is the same
  > information in interpretable units. `chi2dof` is retained as a stored/
  > displayed field (DB, stats table, plots, outlier rejection) but no longer
  > gates. Two further gates were added in a later round (see
  > `docs/DIPOLE_GAINS_REFTILE.md`): good dipoles ≥ 32/32 and NaN channel
  > fraction ≤ 30%. The authoritative summary of the *current* gate set and
  > sorting lives in `docs/CALVIN.md`.
- **Ranking metric**: `max(|length_xx − median(length_xx)|, |length_yy −
  median(length_yy)|)` among unflagged candidates.

---

## Phase 1 — `select_refant`, replacing the `refant` property

### 1.1 Constants

```python
# Reference-tile selection gates (calvin/hyperfits_solution_group.py
# select_refant). A tile below phase/gain fit quality, or whose phase-fit
# residual scatter exceeds the max, in EITHER polarisation, fails that
# gate -- see docs/REF_TILE_SELECTION.md.
REFTILE_PHASE_QUALITY_MIN = 0.8
REFTILE_PHASE_SIGMA_RESID_MAX = 0.15  # radians (was: REFTILE_PHASE_CHI2DOF_MIN/MAX = 0.2/3.0)
REFTILE_GAIN_QUALITY_MIN = 0.8
```

### 1.2 `df_columns.py` additions

`"length"` and `"quality"` are currently raw literals only in
`calvin/plots/phases.py`. `select_refant` becomes a second consumer in a
different file, crossing the threshold this codebase already uses for
promoting a column name to the shared module:

```python
COL_LENGTH = "length"
COL_QUALITY = "quality"
```

Update `phases.py`'s existing 5 raw-literal sites to use these too, while
touching the area (small bonus cleanup, consistent with the
`CONSTANTS_CLEANUP.md` precedent).

### 1.3 `select_refant` method

Replaces the `refant` property. `calvin/hyperfits_solution_group.py`:

```python
def select_refant(self, phase_fit_niter: int) -> pd.Series:
    """Choose the reference tile for calibration.

    Two-stage: a cheap structural bootstrap (today's lowest-unflagged-ID
    rule) is used only to compute a throwaway phase/gain fit pass -- purely
    to rank candidates by calibration quality -- then the actual best tile
    is chosen from that ranking. See docs/REFTILE_SELECTION.md for why a
    single pass can't rank tiles by their own fitted length directly.

    Ranks unflagged tiles by (number of quality gates failed, ascending;
    then length-deviation-from-population-median, ascending; then tile ID
    as a final deterministic tiebreak). Degrades gracefully when no tile
    passes every gate -- the tile failing fewest wins, no separate
    "nothing qualified" case needed.

    Args:
        phase_fit_niter: Number of iterations for the throwaway phase fit
            (see process_phase_fits).

    Returns:
        A pandas Series for the chosen tile (same shape as the old refant
        property's return value).

    Raises:
        ValueError: If no unflagged tiles are found (same as before).
    """
```

Implementation sketch (exact code to follow at implementation time, this
is the shape):

1. `bootstrap = self._bootstrap_refant()` — today's `refant` logic,
   renamed to a private helper (still just "lowest unflagged ID"; nothing
   about *that* part changes).
2. `phase_fits = self.process_phase_fits(bootstrap["name"], phase_fit_niter)`,
   `gain_fits = self.process_gain_fits_for_db(bootstrap["name"])` — both
   read-only, against pristine data.
3. Index both by `(tile_id, pol)`; compute `median(length)` separately per
   pol across all unflagged candidates.
4. For each unflagged tile: count gate failures (missing phase or gain
   rows for either pol — e.g. `_phase_fit_one`/`_gain_fit_one` returned
   `None` — count as failing every gate that row would have covered, not
   as an error); compute the length-deviation ranking metric (large
   sentinel if either phase row is missing, so it still sorts, just last
   among equal-failure-count ties).
5. Sort by `(failures, length_deviation, tile_id)`; return the winning
   tile's row from `metafits_tiles_df`.

### 1.4 Call-site updates

Both call sites currently do `refant = soln_group.refant` early, before
`run_flagging_pipeline`. Change to
`refant = soln_group.select_refant(phase_fit_niter)`:

- `calvin/pipeline.py:154`
- `cli/cal_utils.py:107`

No other change needed at either site for Phase 1/2 alone — `select_refant`
returns the same shape `refant` did, so every downstream use
(`refant["name"]`, `refant["id"]`) keeps working. (Phase 3 does need an
ordering change at both sites, on top of this.)

### 1.5 Tests

New `tests/calvin/test_hyperfits_solution_group.py` cases (that file
already covers `HyperfitsSolutionGroup`):

- A tile with visibly worse `sigma_resid`/`quality` than the rest loses to a
  clean tile, even if its length deviation is small.
- A tile with the smallest length deviation but failing a gate loses to a
  tile with a larger deviation but passing every gate.
- **Graceful degradation**: construct a case where *no* tile passes every
  gate — assert the tile failing fewest still wins (not an exception, not
  the bootstrap tile by default).
- A tile missing from `phase_fits`/`gain_fits` entirely (simulating
  `_phase_fit_one` returning `None`) doesn't crash `select_refant` and
  sorts behind tiles with real data.
- Tie-break by tile ID is deterministic (two tiles with identical
  failure-count and length-deviation).
- Regression: with all tiles equally "fine," the result is still a valid
  unflagged tile (sanity check against a total ranking collapse).

---

## Phase 2 — nothing else changes yet

Deliberately its own (trivial) phase: Phase 1 is a complete, self-contained
unit — `select_refant` slots in wherever `refant` was read, calibration
results downstream are unaffected in shape, only in *which* tile gets
chosen. Confirming this lands and is tested cleanly before touching the
plot-generation ordering in Phase 3 keeps that phase's diff focused purely
on the reordering, not mixed with the selection-logic change.

---

## Phase 3 — `--ref-tile` on hyperdrive's own plots

**Implemented.** The actual implementation differs from the plan below:
`ref_tile: int` (antenna index via `Tile.ant` / `mwalib.Antenna.ant`)
rather than `reftile: str` (tile name), since hyperdrive's `--ref-tile`
flag takes an antenna index. See CHANGELOG.md and the code for the
final implementation; the plan below is preserved for historical context.

### 3.1 `calvin/plots/hyperdrive.py`

`generate_plots`/`generate_plots_for_files` gained a `ref_tile: int | None
= None` parameter, passed through as `--ref-tile {ref_tile}` on the
command line when given. `None` (the default) omits the flag entirely,
so any caller that doesn't pass it keeps the prior behaviour exactly
(hyperdrive picks its own default).

### 3.2 `calvin/pipeline.py` / `cli/cal_utils.py`

Both now pass `ref_tile=refant["ant"]` to both the "before" and "after"
plot calls, so hyperdrive uses the same reference tile as calvin's own
phase-fit plots.

### 3.3 Tests

`tests/calvin/plots/test_hyperdrive.py` covers: `ref_tile=None` omits the
flag, `ref_tile=42` appends `--ref-tile 42`, and `generate_plots_for_files`
forwards `ref_tile` to every file's call.
