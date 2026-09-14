"""Figure sizing helpers shared by every plot in calvin.plots.

resolve_plot_dpi()/scale_plot_figsize() reduce output resolution/size under pytest, so
the test suite isn't saving full-resolution production figures to disk on
every run.
"""

from mwax_mover.core.env import running_under_pytest

# DPI used for every saved figure when running under pytest. Some of these
# figures are physically large -- plot_phase_fits sizes itself from the tile
# count and can reach 20x30 inches, which at the production 300 dpi is a
# 6000x9000 (54 megapixel) PNG. Encoding those dominates the runtime of the
# plotting tests (minutes per test) without testing anything the smaller
# image doesn't: the tests assert on the presence, naming and content of the
# output, never on its resolution. Production runs are unaffected.
_PYTEST_PLOT_DPI = 40


# Linear scale applied to figure dimensions (inches) when running under pytest.
# Combined with _PYTEST_PLOT_DPI this shrinks the rendered raster substantially,
# which matters most for the paged amplitude-outlier figures: those are sized
# from the tile count and can reach 24x16 inches per page, and every page is
# also re-rendered a second time by bbox_inches="tight".
_PYTEST_PLOT_FIGSIZE_SCALE = 0.35


def resolve_plot_dpi(production_dpi: int) -> int:
    """Return the DPI to save a figure at, reduced when running under pytest.

    Args:
        production_dpi: The DPI to use in normal (non-test) operation.

    Returns:
        ``_PYTEST_PLOT_DPI`` when running under pytest, otherwise
        ``production_dpi`` unchanged.
    """
    return _PYTEST_PLOT_DPI if running_under_pytest() else production_dpi


def scale_plot_figsize(width_inches: float, height_inches: float) -> tuple[float, float]:
    """Scale a figure size down when running under pytest.

    Args:
        width_inches: Figure width in inches for normal (non-test) operation.
        height_inches: Figure height in inches for normal (non-test) operation.

    Returns:
        The (width, height) tuple, scaled by ``_PYTEST_PLOT_FIGSIZE_SCALE``
        when running under pytest, otherwise unchanged.
    """
    if not running_under_pytest():
        return (width_inches, height_inches)
    scale = _PYTEST_PLOT_FIGSIZE_SCALE
    return (width_inches * scale, height_inches * scale)
