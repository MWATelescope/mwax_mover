"""Module-level constants shared across the mwax_mover package.

Defines the __FILE__ and __FILENOEXT__ substitution tokens used when building
executable command strings, and the three directory watch-mode string constants
(WATCH_DIR_FOR_NEW, WATCH_DIR_FOR_RENAME, WATCH_DIR_FOR_RENAME_OR_NEW).
"""

# The full filename with path
FILE_REPLACEMENT_TOKEN = "__FILE__"

# Full filename/path but with no extension
FILENOEXT_REPLACEMENT_TOKEN = "__FILENOEXT__"

MODE_WATCH_DIR_FOR_RENAME = "WATCH_DIR_FOR_RENAME"
MODE_WATCH_DIR_FOR_NEW = "WATCH_DIR_FOR_NEW"
MODE_WATCH_DIR_FOR_RENAME_OR_NEW = "WATCH_DIR_FOR_RENAME_OR_NEW"
