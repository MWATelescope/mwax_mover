"""Module-level constants shared across the mwax_mover package.

Defines the __FILE__ and __FILENOEXT__ substitution tokens used when building
executable command strings; the three directory watch-mode string constants
(MODE_WATCH_DIR_FOR_NEW, MODE_WATCH_DIR_FOR_RENAME, MODE_WATCH_DIR_FOR_RENAME_OR_NEW);
the INI config section names used across the CLI daemons (SECTION_*, except
the three metadata-database sections -- see the comment above SECTION_MWAX_MOVER);
the MWA webservice hosts (MWA_WEBSERVICE_HOSTS); the SECONDS_PER_MINUTE/
SECONDS_PER_HOUR time-unit conversions; and EXIT_FAILURE, the single non-zero
process exit code used for every abnormal CLI termination.
"""

# The full filename with path
FILE_REPLACEMENT_TOKEN = "__FILE__"

# Full filename/path but with no extension
FILENOEXT_REPLACEMENT_TOKEN = "__FILENOEXT__"

MODE_WATCH_DIR_FOR_RENAME = "WATCH_DIR_FOR_RENAME"
MODE_WATCH_DIR_FOR_NEW = "WATCH_DIR_FOR_NEW"
MODE_WATCH_DIR_FOR_RENAME_OR_NEW = "WATCH_DIR_FOR_RENAME_OR_NEW"

# INI config section names. The three metadata-database sections (mro/remote/
# mwa metadata database) are deliberately not included here -- they are
# consolidated into a single [mwa database] section in a later cleanup phase.
SECTION_MWAX_MOVER = "mwax mover"
SECTION_CORRELATOR = "correlator"
SECTION_BEAMFORMER = "beamformer"
SECTION_CALVIN = "calvin"
SECTION_BIRLI = "birli"
SECTION_HYPERDRIVE = "hyperdrive"
SECTION_GIANT_SQUID = "giant squid"
SECTION_PLOTS_UPLOAD = "plots upload"
SECTION_DOWNLOADING = "downloading"
SECTION_PROCESSING = "processing"
SECTION_ARCHIVING = "archiving"

# The single consolidated metadata-database section. Previously three
# separate sections (mwa/mro/remote metadata database) all pointing at the
# same database -- see docs/CLEANUP.md 5.2.
SECTION_MWA_DATABASE = "mwa database"

# The two MWA webservice hosts, tried in order (MRO-local first, then
# public) by every caller that queries the metadata webservice. Identical
# in every deployment, so these are constants rather than config.
MWA_WEBSERVICE_HOSTS = ("http://mro.mwa128t.org", "http://ws.mwatelescope.org")

SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600

# Process exit code for any abnormal termination. The specific value carries
# no meaning to any caller; only zero vs non-zero is significant.
EXIT_FAILURE = 1
