"""Architecture tests: the package's internal import graph must stay layered.

This is the guard rail for the source-tree restructure. The package is being
broken up from a flat module list into layered subpackages, and the thing that
makes such a structure decay is a single expedient import pointing the wrong
way. Reviews do not reliably catch those; this does.

The rules are:

1. Every module must be assigned a layer. A new module (or a module moved to a
   new path) fails until it is classified in ``LAYERS`` below, which forces the
   "where does this belong?" question to be answered deliberately rather than by
   whatever directory it happened to land in.
2. A module may import from a *lower* layer or its *own* layer. Importing from a
   higher layer is a violation.
3. The import graph must be acyclic.

Rules 2 and 3 are enforced as a ratchet: the known violations are listed
explicitly and the test asserts the real set matches that list **exactly**. So a
new violation fails the build, and fixing a listed one also fails the build until
the entry is deleted. The lists can only shrink, and they document the remaining
debt in one place instead of scattered TODOs.

Layers are matched by longest dotted prefix, so ``calibration.fitting`` picks up
the entry for ``calibration``. That is deliberate: as the restructure moves files
into packages, whole packages stay classified by one entry and the flat
module-name entries get deleted as they disappear.

Note that imports are collected from the whole AST, not just module level, so a
function-local import (used in this codebase to dodge a circular dependency)
still counts. A deferred import is still a dependency; hiding it inside a
function changes when it resolves, not whether the cycle exists.
"""

import ast
import collections
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "src" / "mwax_mover"

# Layer number -> module names or dotted package prefixes belonging to it.
# Lower numbers are more fundamental. See the module docstring for how matching
# works and why entries are prefixes.
LAYERS: dict[int, tuple[str, ...]] = {
    # L0: constants and version only. Depends on nothing internal.
    0: ("mwax_mover", "version", "constants"),
    # L1: thin wrappers over the standard library and the OS.
    1: ("mwax_command", "core"),
    # L2: primitives -- config, filesystem, FITS, network, database. The
    # god-modules utils.py and mwax_db.py live here until they are split into
    # the packages listed alongside them.
    2: ("utils", "mwax_db", "fits", "filesystem", "net", "db"),
    # L3: the watcher/queue-worker framework and other reusable machinery,
    # built on L2 but knowing nothing about calibration or MWAX data products.
    3: (
        "mwax_watcher",
        "mwax_priority_watcher",
        "mwax_queue_worker",
        "mwax_priority_queue_worker",
        "mwax_priority_queue_data",
        "mwax_watch_queue_worker",
        "mwa_archiver",
        "mwax_bf_vdif_utils",
        "mwax_bf_filterbank_utils",
        "queues",
        "archive",
        "beamformer",
    ),
    # L4: domain logic -- the queue-worker processors, and everything
    # calibration. These know about MWA data products and pipelines.
    4: (
        "mwax_wqw_subfile_incoming_processor",
        "mwax_wqw_checksum_and_db",
        "mwax_wqw_outgoing",
        "mwax_wqw_pawsey_outgoing",
        "mwax_wqw_vis_cal_outgoing",
        "mwax_wqw_vis_stats",
        "mwax_wqw_packet_stats_processor",
        "mwax_wqw_bf_stitching_processor",
        "mwax_calvin_utils",
        "mwax_hyperdrive_solutions",
        "mwax_calvin_plots",
        "mwax_calvin_solutions",
        "mwax_asvo_helper",
        "processors",
        "calibration",
        "calvin",
    ),
    # L5: entry points. Wire the layers below together; nothing imports these.
    5: ("cli",),
}

# Imports that point up a layer. Each one is a bug to be fixed by the
# restructure, not a licence to add more.
#
# ("utils", "mwax_priority_queue_data"):
#     utils.scan_for_existing_files_and_add_to_priority_queue() needs
#     MWAXPriorityQueueData. That function is queue-population logic, not a
#     generic utility -- it belongs in the queues layer. Delete this entry when
#     it moves there.
KNOWN_UPWARD_IMPORTS: set[tuple[str, str]] = {
    ("utils", "mwax_priority_queue_data"),
}

# Import cycles, as frozensets of the modules involved.
#
# {mwax_calvin_utils, mwax_hyperdrive_solutions}:
#     mwax_hyperdrive_solutions imports ChanInfo/Metafits/ensure_system_byte_order
#     and the fitting helpers from mwax_calvin_utils at module level, while
#     mwax_calvin_utils.get_convergence_summary() needs HyperfitsSolution and
#     works around the cycle with a function-local import. The fix is to split
#     the shared primitives down into the fits/ and calibration/ layers and move
#     get_convergence_summary above the solutions reader, at which point this
#     entry goes away.
KNOWN_CYCLES: set[frozenset[str]] = {
    frozenset({"mwax_calvin_utils", "mwax_hyperdrive_solutions"}),
}


def _module_name(path: Path) -> str:
    """Convert a source file path into its dotted module name.

    Args:
        path: Path to a .py file inside the package root.

    Returns:
        The module name relative to the package, e.g. ``cli.cal_utils``.
    """
    return str(path.relative_to(PACKAGE_ROOT)).removesuffix(".py").replace("/", ".")


def _discover_modules() -> dict[str, Path]:
    """Find every importable module in the package.

    ``__init__`` files are skipped: they exist to make a directory a package and
    classifying them separately from the package would be noise.

    Returns:
        Mapping of dotted module name to file path.
    """
    return {
        name: path
        for path in sorted(PACKAGE_ROOT.rglob("*.py"))
        if not (name := _module_name(path)).endswith("__init__")
    }


def _internal_imports(path: Path, known_modules: set[str]) -> set[str]:
    """Collect the in-package modules a source file imports.

    Walks the entire AST rather than just the top-level body, so imports nested
    inside functions or ``if`` blocks are included -- see the module docstring.

    Args:
        path: Source file to scan.
        known_modules: Module names that count as internal; anything else
            (standard library, third party, or a package's ``__init__``) is
            ignored.

    Returns:
        The set of internal module names imported by this file.
    """
    found: set[str] = set()
    tree = ast.parse(path.read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                # Relative import. The package uses absolute imports
                # throughout; if that ever changes this needs resolving against
                # the importing module's own package, so fail loudly instead of
                # silently dropping the edge.
                raise AssertionError(f"{path}: relative import not supported by the architecture test")
            if not node.module or not node.module.startswith("mwax_mover"):
                continue

            base = node.module.removeprefix("mwax_mover").lstrip(".")
            if base in known_modules:
                # from mwax_mover.some_module import a_symbol
                found.add(base)
                continue

            # from mwax_mover import module_a, module_b -- the imported names
            # are modules, not symbols, so the edge is to each of them. Missing
            # this form is easy and costly: it is how 18 files in this package
            # import their dependencies.
            for alias in node.names:
                candidate = f"{base}.{alias.name}" if base else alias.name
                if candidate in known_modules:
                    found.add(candidate)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("mwax_mover"):
                    target = alias.name.removeprefix("mwax_mover").lstrip(".")
                    if target in known_modules:
                        found.add(target)

    return found


def _layer_of(module: str) -> int | None:
    """Resolve a module's layer by longest matching dotted prefix.

    An exact name always wins over a prefix, and a longer prefix wins over a
    shorter one, so a specific module can be pulled out of its package's default
    layer if it ever genuinely needs to be.

    Args:
        module: Dotted module name, e.g. ``calibration.fitting``.

    Returns:
        The layer number, or None if the module matches no entry.
    """
    best_layer: int | None = None
    best_length = -1

    for layer, prefixes in LAYERS.items():
        for prefix in prefixes:
            if module == prefix or module.startswith(f"{prefix}."):
                if len(prefix) > best_length:
                    best_layer, best_length = layer, len(prefix)

    return best_layer


@pytest.fixture(scope="module")
def graph() -> tuple[dict[str, Path], dict[str, set[str]]]:
    """Build the package's internal import graph once for all tests here."""
    modules = _discover_modules()
    names = set(modules)
    imports = {name: _internal_imports(path, names) for name, path in modules.items()}
    return modules, imports


def test_package_root_exists():
    """Guard against the tests silently passing because nothing was found.

    Every other test here iterates over discovered modules, so a wrong
    PACKAGE_ROOT would make them all vacuously pass.
    """
    assert PACKAGE_ROOT.is_dir(), f"package root not found: {PACKAGE_ROOT}"
    assert len(_discover_modules()) > 10, "suspiciously few modules discovered"


def test_every_module_is_assigned_a_layer(graph):
    """A new or moved module must be classified in LAYERS deliberately."""
    modules, _ = graph

    unassigned = sorted(name for name in modules if _layer_of(name) is None)

    assert not unassigned, (
        "These modules are not assigned a layer in LAYERS. Add each one to the "
        "layer it belongs to (see this module's docstring), rather than to "
        "whichever layer makes the test pass:\n  " + "\n  ".join(unassigned)
    )


def test_no_unexpected_upward_imports(graph):
    """Modules may import their own layer or below, never above."""
    _, imports = graph

    upward = set()
    for module, targets in imports.items():
        source_layer = _layer_of(module)
        for target in targets:
            target_layer = _layer_of(target)
            if source_layer is None or target_layer is None:
                continue  # reported by test_every_module_is_assigned_a_layer
            if target_layer > source_layer:
                upward.add((module, target))

    new = upward - KNOWN_UPWARD_IMPORTS
    fixed = KNOWN_UPWARD_IMPORTS - upward

    assert not new, "New upward imports (a module importing from a higher layer):\n  " + "\n  ".join(
        f"L{_layer_of(m)} {m} -> L{_layer_of(t)} {t}" for m, t in sorted(new)
    )
    assert not fixed, (
        "These upward imports are listed in KNOWN_UPWARD_IMPORTS but no longer "
        "exist. Delete them from the list so it keeps reflecting reality:\n  " + "\n  ".join(map(str, sorted(fixed)))
    )


def test_no_unexpected_cycles(graph):
    """The import graph must be acyclic apart from documented exceptions."""
    _, imports = graph

    found: list[list[str]] = []
    state: dict[str, int] = collections.defaultdict(int)  # 0 unseen, 1 on stack, 2 done
    stack: list[str] = []

    def visit(module: str) -> None:
        state[module] = 1
        stack.append(module)
        for target in sorted(imports.get(module, ())):
            if state[target] == 1:
                found.append(stack[stack.index(target) :])
            elif state[target] == 0:
                visit(target)
        stack.pop()
        state[module] = 2

    for module in sorted(imports):
        if state[module] == 0:
            visit(module)

    cycles = {frozenset(cycle) for cycle in found}
    new = cycles - KNOWN_CYCLES
    fixed = KNOWN_CYCLES - cycles

    assert not new, "New import cycles:\n  " + "\n  ".join(" <-> ".join(sorted(c)) for c in new)
    assert not fixed, (
        "These cycles are listed in KNOWN_CYCLES but no longer exist. Delete "
        "them from the list so it keeps reflecting reality:\n  " + "\n  ".join(" <-> ".join(sorted(c)) for c in fixed)
    )


def test_nothing_imports_the_cli_layer(graph):
    """Entry points are leaves: importing one couples a library to argparse.

    Not covered by the layering rules on their own, since cli is the top layer
    and an import between two cli modules would be a legal same-layer edge.
    """
    _, imports = graph

    offenders = sorted(
        (module, target)
        for module, targets in imports.items()
        for target in targets
        if target.startswith("cli.") and not module.startswith("cli.")
    )

    assert not offenders, "Non-cli modules importing a cli entry point:\n  " + "\n  ".join(
        f"{m} -> {t}" for m, t in offenders
    )
