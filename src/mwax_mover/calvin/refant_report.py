"""Reference-tile selection diagnostics report.

Formats a human-readable report of the select_refant ranking for inclusion
in {obs_id}_stats.txt and INFO-level logging. Split out of
calvin.hyperfits_solution_group to keep that class's file manageable
(1400+ lines). See docs/DIPOLE_GAINS_REFTILE.md Enhancement 2.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from mwax_mover.constants import (
    REFTILE_DIPOLE_GAINS_EXPECTED,
    REFTILE_DIPOLE_GOOD_MIN,
    REFTILE_GAIN_QUALITY_MIN,
    REFTILE_NAN_CHANNEL_FRACTION_MAX,
    REFTILE_PHASE_QUALITY_MIN,
    REFTILE_PHASE_SIGMA_RESID_MAX,
    REFTILE_REPORT_TILES_TO_DISPLAY,
)


def format_refant_selection_report(
    scored: list[tuple[int, int, int, float, int]],
    tile_names: NDArray,
    tile_ids: NDArray,
    tile_ants: NDArray,
    gate_details: dict[int, dict],
    median_length: dict[str, float],
    bootstrap_name: str,
    bootstrap_ant: int,
    dipole_gains_available: bool,
    total_chanblocks: int,
) -> str:
    """Format a human-readable report of the ref tile selection ranking.

    Shows the gate thresholds, each candidate tile's gate pass/fail
    status, dipole health, NaN channel count, length deviation, and
    overall rank, with the chosen tile highlighted.

    Args:
        scored: The sorted ranking list from select_refant. Each entry
            is (failures, n_dead_dipoles, n_nan_channels,
            length_deviation, tile_id).
        tile_names: Array of tile names, indexed by tile position.
        tile_ids: Array of tile IDs, indexed by tile position.
        tile_ants: Array of antenna indices, indexed by tile position.
        gate_details: Per-tile gate results, keyed by tile_id. Each
            value is a dict with keys: phase_quality_ok,
            phase_sigma_resid_ok, gain_quality_ok, dipole_ok,
            n_good_dipoles, nan_ok, n_nan_channels, nan_fraction,
            phase_quality_xx, phase_quality_yy, phase_sigma_resid_xx,
            phase_sigma_resid_yy, gain_quality_xx, gain_quality_yy.
        median_length: Dict of median fitted lengths per pol (XX/YY),
            from the bootstrap phase fit pass.
        bootstrap_name: Name of the bootstrap reference tile.
        bootstrap_ant: Antenna index of the bootstrap reference tile.
        dipole_gains_available: Whether the DipoleGains column was
            present in the solution files.
        total_chanblocks: Total number of chanblocks across all solution
            files in the group.

    Returns:
        A multi-line string suitable for writing to a stats file or
        logging.
    """
    lines: list[str] = []
    lines.append("Reference tile selection:")
    lines.append(f"  Bootstrap ref: {bootstrap_name} (ant {bootstrap_ant})")
    lines.append(f"  DipoleGains: {'available (from solution files)' if dipole_gains_available else 'not available'}")
    lines.append(f"  Median cable length: XX={median_length['XX']:.4f}  YY={median_length['YY']:.4f}")
    lines.append(f"  Total chanblocks: {total_chanblocks}")

    # Gate thresholds summary.
    gate_labels = [
        f"phase_quality>={REFTILE_PHASE_QUALITY_MIN}",
        f"phase_sigma_resid<={REFTILE_PHASE_SIGMA_RESID_MAX}",
        f"gain_quality>={REFTILE_GAIN_QUALITY_MIN}",
    ]
    if dipole_gains_available:
        gate_labels.append(f"good_dipoles>={REFTILE_DIPOLE_GOOD_MIN}/{REFTILE_DIPOLE_GAINS_EXPECTED}")
    gate_labels.append(f"nan_channels<={REFTILE_NAN_CHANNEL_FRACTION_MAX:.0%}")
    lines.append(f"  Gates: {' | '.join(gate_labels)}")
    lines.append("")

    # Build a tile_id -> (name, ant) lookup for the table.
    id_to_name = dict(zip(tile_ids, tile_names, strict=True))
    id_to_ant = dict(zip(tile_ids, tile_ants, strict=True))

    # Column widths.
    name_w = max(10, max((len(str(id_to_name.get(s[4], ""))) for s in scored), default=10) + 2)

    # Header.
    hdr = (
        f"  {'Rank':<5} {'Tile':>5}  {'Name':<{name_w}} {'Ant':>4}  {'Fail':>4}  "
        + (f"{'Dipoles':>7}  " if dipole_gains_available else "")
        + f"{'NaN%':>5}  "
        f"{'LenDev':>8}  {'PhQ_XX':>6} {'PhQ_YY':>6}  "
        f"{'SRes_XX':>7} {'SRes_YY':>7}  {'GnQ_XX':>6} {'GnQ_YY':>6}  Gates"
    )
    lines.append(hdr)
    lines.append(f"  {'-' * (len(hdr) - 2)}")

    # Limit to top 20 for readability; show total count if truncated.
    display_count = min(len(scored), REFTILE_REPORT_TILES_TO_DISPLAY)

    for rank, entry in enumerate(scored[:display_count], start=1):
        failures, n_dead, _n_nan, length_deviation, tile_id = entry
        name = str(id_to_name.get(tile_id, "?"))
        ant = id_to_ant.get(tile_id, -1)
        details = gate_details.get(tile_id, {})

        # Gate pass/fail letters: P=pass, .=fail, -=no data.
        phase_q_flag = "P" if details.get("phase_quality_ok") else ("." if "phase_quality_ok" in details else "-")
        sres_flag = "P" if details.get("phase_sigma_resid_ok") else ("." if "phase_sigma_resid_ok" in details else "-")
        gain_q_flag = "P" if details.get("gain_quality_ok") else ("." if "gain_quality_ok" in details else "-")
        gates_str = f"{phase_q_flag}{sres_flag}{gain_q_flag}"
        if dipole_gains_available:
            dipole_flag = "P" if details.get("dipole_ok") else "."
            gates_str += dipole_flag
        nan_flag = "P" if details.get("nan_ok") else "."
        gates_str += nan_flag

        # Format numeric values.
        def _fmt(val, spec):
            return (
                "--"
                if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val)))
                else f"{val:{spec}}"
            )

        phq_xx = _fmt(details.get("phase_quality_xx"), ".2f")
        phq_yy = _fmt(details.get("phase_quality_yy"), ".2f")
        sres_xx = _fmt(details.get("phase_sigma_resid_xx"), ".4f")
        sres_yy = _fmt(details.get("phase_sigma_resid_yy"), ".4f")
        gnq_xx = _fmt(details.get("gain_quality_xx"), ".2f")
        gnq_yy = _fmt(details.get("gain_quality_yy"), ".2f")
        len_dev = _fmt(length_deviation, ".4f")
        nan_pct = f"{details.get('nan_fraction', 0.0) * 100:.1f}"

        winner_mark = "*" if rank == 1 else " "

        n_good = REFTILE_DIPOLE_GAINS_EXPECTED - n_dead
        dipole_col = f"{n_good:>2}/{REFTILE_DIPOLE_GAINS_EXPECTED}  " if dipole_gains_available else ""

        line = (
            f"  {rank:<4}{winner_mark} {tile_id:>5}  {name:<{name_w}} {ant:>4}  {failures:>4}  "
            + dipole_col
            + f"{nan_pct:>5}  "
            f"{len_dev:>8}  {phq_xx:>6} {phq_yy:>6}  "
            f"{sres_xx:>7} {sres_yy:>7}  {gnq_xx:>6} {gnq_yy:>6}  {gates_str}"
        )
        lines.append(line)

    if len(scored) > display_count:
        lines.append(f"  ... ({len(scored) - display_count} more candidates not shown)")

    # Winner summary.
    w_entry = scored[0]
    w_failures, w_dead, w_nan, w_lendev, w_tile_id = w_entry
    w_name = str(id_to_name.get(w_tile_id, "?"))
    w_ant = id_to_ant.get(w_tile_id, -1)
    n_good = REFTILE_DIPOLE_GAINS_EXPECTED - w_dead
    dipole_part = f", {n_good}/{REFTILE_DIPOLE_GAINS_EXPECTED} dipoles" if dipole_gains_available else ""
    w_nan_frac = w_nan / total_chanblocks * 100 if total_chanblocks > 0 else 0.0
    lines.append("")
    lines.append(
        f"  Chosen: {w_name} (ant {w_ant}, id {w_tile_id})"
        f" -- {w_failures} gate failure(s){dipole_part},"
        f" {w_nan}/{total_chanblocks} NaN channels ({w_nan_frac:.1f}%),"
        f" length deviation {w_lendev:.4f}"
    )
    lines.append("")

    return "\n".join(lines)
