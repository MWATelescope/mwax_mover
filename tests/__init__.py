"""Test package for mwax_mover.

This file exists so the test tree is a real package rather than a loose
collection of modules. That matters for the source-tree restructure: the tests
are being reorganised to mirror the package layout, and without a package
pytest's prepend import mode identifies a test module by its basename alone, so
two modules with the same basename in different directories collide with
"import file mismatch". As a package, each module gets a fully qualified name
and the collision goes away -- which is what allows, say,
tests/calibration/test_plots.py to coexist with tests/calvin/test_plots.py.

Making the tree a package moves the repository root onto sys.path instead of
tests/, so the shared helpers (tests_common, tests_fakedb) would stop resolving
as top-level imports. `pythonpath = ["tests"]` in pyproject.toml puts tests/
back, keeping those imports working unchanged.
"""
