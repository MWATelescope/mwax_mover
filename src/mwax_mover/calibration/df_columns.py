"""Shared DataFrame column names and dict keys for the calibration pipeline.

These strings are read and written identically across calibration/outliers.py,
calvin/hyperdrive.py, calvin/pipeline.py, calvin/plots/phases.py,
calvin/plots/stats_table.py, and calvin/plots/gains.py, with no previous
central definition. Centralised here so a typo doesn't silently create a
new DataFrame column or fail with a KeyError far from the mistake. See
docs/CONSTANTS_CLEANUP.md Phase 2.
"""

# Tile/solution identifiers
COL_TILE_ID = "tile_id"
COL_SOLN_IDX = "soln_idx"

# Polarisation
COL_POL = "pol"
COL_XX = "XX"
COL_YY = "YY"
COL_GX = "gx"
COL_GY = "gy"

# Tile metadata
COL_FLAVOR = "flavor"

# Outlier-rejection results
COL_OUTLIER = "outlier"
COL_SIGMA_RESID = "sigma_resid"
COL_CHI2DOF = "chi2dof"
