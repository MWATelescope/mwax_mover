import os
import shutil
import tempfile
import time
from pathlib import Path

from mwax_mover.utils import write_mock_subfile

# Environment variable used to override where the tests create their scratch
# directory tree. Set this in CI (or locally) to relocate the tree; if it is
# not set, DEFAULT_TEST_BASE_DIR is used.
TEST_BASE_DIR_ENV_VAR = "MWAX_MOVER_TEST_DIR"

# Default scratch location. Deliberately under the system temp dir rather than
# a developer's home directory so the suite runs anywhere (including CI)
# without per-machine configuration.
DEFAULT_TEST_BASE_DIR = str(Path(tempfile.gettempdir()) / "mwax_mover_testing")

# Tokens used in the tests/data/*/*.cfg template files. render_test_config()
# substitutes these for real, per-test paths before the config is handed to a
# processor's initialise() method.
CONFIG_TOKEN_BASE_DIR = "@TEST_BASE_DIR@"
CONFIG_TOKEN_BIN_DIR = "@TEST_BIN_DIR@"

# The tests/ directory itself, and the read-only fixture tree inside it.
#
# Anchored on this file's own location rather than the process working
# directory. Fixture paths used to be written as relative literals
# (data_path("...")), which meant the whole suite only worked when pytest was
# invoked from the repository root -- running it from anywhere else failed at
# collection. Anchoring here also decouples the fixture location from where a
# test module happens to live, which is what allows test modules to be moved
# into subdirectories mirroring the package layout without rewriting a single
# fixture path.
TESTS_DIR = Path(__file__).resolve().parent
DATA_DIR = TESTS_DIR / "data"


def data_path(*parts: str) -> str:
    """Build an absolute path into the shared tests/data fixture tree.

    Args:
        *parts: Path components below tests/data, e.g.
            ``data_path("1391522232", "1391522232_metafits.fits")``.

    Returns:
        The absolute path as a string. A string rather than a Path because
        almost every caller passes it straight into production code that takes
        str paths (os.path.join, open, astropy, mwalib).
    """
    return str(DATA_DIR.joinpath(*parts))


def obs_data_dir(obs_id: int | str) -> str:
    """Return the fixture directory for one observation.

    Args:
        obs_id: The observation ID, which is also the directory name.

    Returns:
        Absolute path to ``tests/data/<obs_id>``.
    """
    return data_path(str(obs_id))


def obs_metafits_path(obs_id: int | str) -> str:
    """Return the metafits fixture for one observation.

    The single most repeated fixture path in the suite, hence its own helper.

    Args:
        obs_id: The observation ID.

    Returns:
        Absolute path to ``tests/data/<obs_id>/<obs_id>_metafits.fits``.
    """
    return data_path(str(obs_id), f"{obs_id}_metafits.fits")


# Stub files created inside the per-test bin directory. The processors under
# test only check that these paths exist (via os.path.exists) during
# initialise(); nothing is ever executed or read, so empty files suffice.
STUB_BIN_FILENAMES = (
    "mwax_stats",
    "giant-squid",
    "birli",
    "hyperdrive",
    "srclist.txt",
)


def get_test_base_dir() -> str:
    """Return the root directory the tests use for their scratch directory tree.

    Returns:
        The value of the ``MWAX_MOVER_TEST_DIR`` environment variable if it is
        set and non-empty, otherwise ``DEFAULT_TEST_BASE_DIR``.
    """
    return os.environ.get(TEST_BASE_DIR_ENV_VAR) or DEFAULT_TEST_BASE_DIR


def get_test_bin_dir(test_code: str) -> str:
    """Return the stub binary directory for a given test.

    Args:
        test_code: Short test identifier, e.g. ``"test001"``.

    Returns:
        Full path to the test's stub binary directory (no trailing slash).
    """
    return os.path.join(get_test_base_dir(), test_code, "bin")


def render_test_config(test_code: str, cfg_filename: str | None = None) -> str:
    """Render a test config template into the test's own scratch directory.

    Reads ``tests/data/<test_code>/<cfg_filename>``, substitutes
    ``CONFIG_TOKEN_BASE_DIR`` and ``CONFIG_TOKEN_BIN_DIR`` for this test's real
    paths, and writes the result alongside the test's scratch directories. Also
    creates the stub binary directory and the empty files named in
    ``STUB_BIN_FILENAMES``, so the ``os.path.exists`` guards in each processor's
    ``initialise()`` are satisfied without needing real Rust binaries or
    sibling repository checkouts.

    Call this *after* ``setup_test_directories()`` for the same ``test_code``,
    since that function clears the directory tree this writes into.

    Args:
        test_code: Short test identifier, e.g. ``"test001"``.
        cfg_filename: Name of the template config file within
            ``tests/data/<test_code>/``. Defaults to ``"<test_code>.cfg"``.

    Returns:
        Full path to the rendered config file, ready to pass to an
        ``initialise()`` method.

    Raises:
        FileNotFoundError: If the template config file does not exist.
    """
    if cfg_filename is None:
        cfg_filename = f"{test_code}.cfg"

    template_path = Path(data_path(test_code, cfg_filename))
    if not template_path.is_file():
        raise FileNotFoundError(f"Test config template not found: {template_path}")

    test_root = Path(get_test_base_dir()) / test_code
    bin_dir = Path(get_test_bin_dir(test_code))
    bin_dir.mkdir(parents=True, exist_ok=True)
    for stub_filename in STUB_BIN_FILENAMES:
        (bin_dir / stub_filename).touch(exist_ok=True)

    rendered = (
        template_path.read_text(encoding="utf-8")
        .replace(CONFIG_TOKEN_BASE_DIR, str(test_root))
        .replace(CONFIG_TOKEN_BIN_DIR, str(bin_dir))
    )

    rendered_path = test_root / cfg_filename
    rendered_path.parent.mkdir(parents=True, exist_ok=True)
    rendered_path.write_text(rendered, encoding="utf-8")

    return str(rendered_path)


def setup_test_directories(test_code: str, base_dir: str | None = None) -> str:
    """Create (or clear) the scratch directory tree used by a test.

    Ensures every directory the test configs refer to exists. If a directory
    already exists its contents are removed but the directory itself is kept.

    Args:
        test_code: Short test identifier, e.g. ``"test001"``. Used as the name
            of this test's subdirectory so tests do not interfere with each
            other.
        base_dir: Root directory to create the tree under. Defaults to
            ``get_test_base_dir()``.

    Returns:
        The test's base directory, with a trailing slash.

    Raises:
        ValueError: If a path resolves somewhere shallow enough to be
            dangerous to clear.
        NotADirectoryError: If one of the expected paths exists but is not a
            directory.
    """
    if base_dir is None:
        base_dir = get_test_base_dir()

    # The directories we create will be based on the test file name
    # e.g. /tmp/mwax_mover_testing/test001
    base = f"{base_dir}/{test_code}/"

    paths = [
        "/tmp",
        "/bin",
        "/dev/shm/mwax",
        "/logs",
        "/logs/scripts",
        "/data",
        "/data/calvin",
        "/data/calvin/in_jobs",
        "/data/calvin/out_jobs",
        "/data/calvin/plots",
        "/shared/data/calvin11/plots",
        "/shared/data/calvin12/plots",
        "tmp/jobs",
        "tmp/cal",
        "/voltdata/incoming",
        "/voltdata/outgoing",
        "/voltdata/dont_archive",
        "/voltdata/bf/incoming",
        "/voltdata/bf/stitching",
        "/voltdata/bf/outgoing",
        "/voltdata/bf/dont_archive",
        "/visdata/incoming",
        "/visdata/dont_archive",
        "/visdata/processing_stats",
        "/visdata/outgoing",
        "/visdata/cal_outgoing",
        "/volume1/incoming",
        "/volume1/outgoing",
        "/volume2/incoming",
        "/volume2/outgoing",
        "/volume3/incoming",
        "/volume3/outgoing",
        "/vulcan/packet_stats_dump",
        "/vulcan/packet_stats_destination",
        "/vulcan/mwax_stats_dump",
        "/vulcan/metafits",
        "/vulcan/mwax_aocal",
    ]

    def _is_dangerous_path(p: Path) -> bool:
        s = str(p.resolve())
        if s == "/":
            return True
        # Guardrail: require deeper-than /data/mwax_mover_testing
        if len(p.resolve().parts) < 4:
            return True
        return False

    def _clear_directory(dir_path: Path) -> None:
        for entry in dir_path.iterdir():
            if entry.is_symlink() or entry.is_file():
                entry.unlink(missing_ok=True)
            elif entry.is_dir():
                shutil.rmtree(entry)
            else:
                # For FIFOs/sockets/etc.
                entry.unlink(missing_ok=True)

    for p_str in paths:
        p = Path(f"{base}{p_str}")

        if _is_dangerous_path(p):
            raise ValueError(f"Refusing to operate on potentially dangerous path: {p}")

        if p.exists():
            if not p.is_dir():
                raise NotADirectoryError(f"Path exists but is not a directory: {p}")
            _clear_directory(p)
        else:
            p.mkdir(parents=True, exist_ok=True)

    return base


def create_observation_subfiles(
    obs_id: int,
    subfile_count: int,
    mode: str,
    rec_chan: int,
    corr_chan: int,
    dev_shm_temp_dir: str,
    dev_shm_dir: str,
    subfile_creation_delay: int = 0,
):
    """Creates some test subfiles for an obs"""
    sub_obs_id = obs_id
    offset = 0

    for _ in range(subfile_count):
        tmp_subfile_filename = os.path.join(
            dev_shm_temp_dir,
            f"{obs_id}_{sub_obs_id}_{rec_chan}.$$$",
        )

        # Write new subfile to dev_shm_tmp
        write_mock_subfile(
            tmp_subfile_filename,
            obs_id,
            sub_obs_id,
            mode,
            offset,
            rec_chan,
            corr_chan,
        )

        # Now rename to real subfile for processing
        # This is what subfile processor triggers on (RENAME)
        subfile_filename = os.path.join(
            dev_shm_dir,
            f"{obs_id}_{sub_obs_id}_{rec_chan}.sub",
        )
        os.rename(tmp_subfile_filename, subfile_filename)

        # simulate gap between subobs
        time.sleep(subfile_creation_delay)

        # Increment subobsid and offset
        sub_obs_id += 8
        offset += 8
