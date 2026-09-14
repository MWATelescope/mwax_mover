"""Unit tests for MWAXDBHandler.from_config in mwax_mover.db.handler.

Uses an in-memory ConfigParser rather than the directory-based test-config
fixtures (render_test_config/setup_test_directories), since from_config()
only needs a ConfigParser with a [mwa database] section -- no filesystem
scaffolding is involved.
"""

from configparser import ConfigParser

from mwax_mover.db.handler import MWAXDBHandler


def _make_config(host: str, db: str, user: str, password: str, port: str) -> ConfigParser:
    """Build a minimal ConfigParser with just a [mwa database] section."""
    config = ConfigParser()
    config.read_dict(
        {
            "mwa database": {
                "host": host,
                "db": db,
                "user": user,
                "pass": password,
                "port": port,
            }
        }
    )
    return config


def test_from_config_reads_dummy_database_without_decoding_password():
    """A "dummy" db does not base64-decode pass, matching every daemon's own predicate."""
    config = _make_config(host="dummy", db="dummy", user="dummy", password="dummy", port="5432")

    handler = MWAXDBHandler.from_config(config)

    assert handler.host == "dummy"
    assert handler.db_name == "dummy"
    assert handler.user == "dummy"
    assert handler.password == "dummy"
    assert handler.port == 5432


def test_from_config_base64_decodes_password_for_a_real_database():
    """A real (non-dummy) db has its pass value base64-decoded."""
    # base64.b64encode(b"realpassword").decode() -- computed once, not at test time,
    # so a bug in the encoding step can't also hide a bug in the decoding step.
    config = _make_config(
        host="db.example.org",
        db="mwa_metadata",
        user="mwax",
        password="cmVhbHBhc3N3b3Jk",
        port="5433",
    )

    handler = MWAXDBHandler.from_config(config)

    assert handler.host == "db.example.org"
    assert handler.db_name == "mwa_metadata"
    assert handler.user == "mwax"
    assert handler.password == "realpassword"
    assert handler.port == 5433


def test_from_config_port_is_an_int():
    """The port key is read as a string but stored as an int."""
    config = _make_config(host="dummy", db="dummy", user="dummy", password="dummy", port="5432")

    handler = MWAXDBHandler.from_config(config)

    assert isinstance(handler.port, int)
