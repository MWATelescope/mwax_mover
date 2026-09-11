# mwax_mover reference-tile selection & --reftile plan

Status: **plan only — no code written yet.** All design questions from
discussion are resolved below; ready to implement as written, pending your
final look.

This does two related things: replaces `HyperfitsSolutionGroup.refant`'s
"lowest unflagged ID" rule with a quality-aware selection (Phases 1-2), and
threads the chosen tile through to hyperdrive's own `solutions-plot`
`--reftile` argument so its plots match calvin's internal ones (Phase 3).
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
  polarisation): phase `quality` ≥ 0.8, phase `chi2dof` in `[0.2, 3.0]`,
  gain `quality` ≥ 0.8. `sigma_resid` deliberately left out as a gate — it's
  largely redundant with `chi2dof` (both measure fit-residual size).
- **Ranking metric**: `max(|length_xx − median(length_xx)|, |length_yy −
  median(length_yy)|)` among unflagged candidates.

---

## Phase 1 — `select_refant`, replacing the `refant` property

### 1.1 Constants

```python
# Reference-tile selection gates (calvin/hyperfits_solution_group.py
# select_refant). A tile below phase/gain fit quality, or with too extreme
# a chi2dof, in EITHER polarisation, fails that gate -- see
# docs/REFTILE_SELECTION.md.
REFTILE_PHASE_QUALITY_MIN = 0.8
REFTILE_PHASE_CHI2DOF_MIN = 0.2
REFTILE_PHASE_CHI2DOF_MAX = 3.0
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

- A tile with visibly worse `chi2dof`/`quality` than the rest loses to a
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

## Phase 3 — `--reftile` on hyperdrive's own plots

### 3.1 `calvin/plots/hyperdrive.py`

`generate_plots`/`generate_plots_for_files` gain a new parameter:

```python
def generate_plots(
    obs_id: int,
    hyperdrive_solution_filename: str,
    hyperdrive_binary_path: str,
    metafits_filename: str,
    output_dir: str,
    before: bool,
    max_amp: int | None = None,
    reftile: str | None = None,
) -> tuple[bool, str]:
    ...
    if reftile is not None:
        hyp_soln_plot_args += f" --reftile {reftile}"
```

`generate_plots_for_files` just forwards a `reftile: str | None = None`
parameter through to each `generate_plots` call. `None` (the default)
omits the flag entirely, so any caller that doesn't pass it keeps today's
behaviour exactly (hyperdrive picks its own default).

### 3.2 Reorder `calvin/pipeline.py` / `cli/cal_utils.py`

Both currently do, in order: load → `refant` → "before" plots (no
`--reftile`) → `run_flagging_pipeline` → ... → `commit()` → "after" plots
(no `--reftile`).

New order: load → `select_refant` (Phase 1) → "before" plots **with**
`reftile=refant["name"]` → `run_flagging_pipeline` → ... → `commit()` →
"after" plots **with** `reftile=refant["name"]`.

This is a pure reordering plus threading one extra string through two
existing calls — the on-disk files are still pristine when "before" plots
run either way (nothing writes to disk before `commit()`), so moving
`select_refant` earlier doesn't change what the "before" plots show, only
what reference tile hyperdrive uses to show it.

### 3.3 Tests

`calvin/plots/hyperdrive.py`'s `generate_plots`/`generate_plots_for_files`
already have no test coverage (confirmed earlier in this engagement) —
same situation `run_hyperdrive` was in before `HYPERDRIVE_PARALLELISM.md`.
New tests here:

- `reftile=None` produces the same command line as today (no `--reftile`
  substring) — a regression guard for existing callers.
- `reftile="Tile104"` appends `--reftile Tile104` to the command line
  (mock `run_command`, assert on the constructed `cmd` string).
- `generate_plots_for_files` forwards `reftile` to every file's call.

No new test needed for the `pipeline.py`/`cal_utils.py` reordering itself
beyond what already exists — those are integration-level flows without
dedicated unit tests today (same situation noted for `run_hyperdrive`
previously); the reordering's correctness rests on the "nothing writes to
disk before `commit()`" invariant, which is already documented in the
existing code comments at both call sites.

---

## Open items

None — every design question from discussion is resolved above.
