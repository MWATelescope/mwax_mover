"""Test double for MWAXDBHandler that avoids real database connections.

This module provides FakeMWAXDBHandler, a subclass of MWAXDBHandler that
replaces all database I/O with in-memory queues and call logs. It is intended
for use in unit tests that need to exercise domain logic in mwax_mover.mwax_db
without spinning up a real PostgreSQL instance.

Use in conjunction with "dummy" values in the tests config files. That will
ensure that no actual db connection is used.

For MWACacheArchiver, MWAXSubfileDistributor, MWAXCalvinController and MWAXCalvinProcessor
you call Initialise() first, the override the MWAXDbHandler e.g.

x = MWAXCalvinController()
x.Initialise()
# Override db_handler
x.db_handler = FakeDBHandler()

Typical usage::

    from tests.fake_db import FakeMWAXDBHandler
    from mwax_mover.mwax_db import get_unattempted_calsolution_requests

    def test_returns_none_when_no_requests():
        db = FakeMWAXDBHandler()
        db.select_results = [[]]
        assert get_unattempted_calsolution_requests(db) is None
"""

from mwax_mover.mwax_db import MWAXDBHandler


class FakeMWAXDBHandler(MWAXDBHandler):
    """A test double for MWAXDBHandler that stores calls and returns canned data.

    All database I/O methods are overridden so that no real PostgreSQL
    connection is required. SELECT results are consumed from a FIFO queue
    (``select_results``) and DML calls are appended to a log (``dml_calls``),
    both of which tests can inspect or pre-populate as needed.

    The parent ``__init__`` is called with ``host="dummy"`` which suppresses
    connection-pool creation — a code path that already exists in
    ``MWAXDBHandler.__init__``.

    Attributes:
        select_results: A list of result sets to be returned by SELECT calls,
            consumed in FIFO order. Each element should be a list of dicts
            mirroring what ``psycopg`` would return with ``dict_row`` — for
            example ``[{"obs_id": 1234567890}]``. Append one entry per
            expected SELECT call before running the code under test.
        dml_calls: A list of dicts recording every DML call made. Each entry
            has the keys ``"sql"`` (str) and ``"params"`` (the parameter list
            passed to the statement). Tests can assert on length, SQL content,
            or parameter values after calling the code under test.

    Example::

        db = FakeMWAXDBHandler()
        db.select_results = [
            [{"request_id": 1, "obs_id": 1234567890, "realtime": True}]
        ]
        result = get_unattempted_calsolution_requests(db)
        assert result == [(1, 1234567890, True)]
        # No DML was issued, so dml_calls is empty.
        assert db.dml_calls == []
    """

    def __init__(self):
        """Initialise the fake handler with empty queues.

        Calls the parent ``__init__`` with ``host="dummy"`` so that the
        connection pool is not created. Both ``select_results`` and
        ``dml_calls`` are initialised to empty lists ready for each test to
        configure.
        """
        super().__init__(host="dummy", port=0, db_name="", user="", password="")
        self.select_results: list[list[dict]] = []
        self.dml_calls: list[dict] = []

    def close(self):
        """Overrides the close method- nothing to do here since we don't have
        connections or a pool"""
        pass

    def select_postgres(self, sql: str, parm_list, expected_rows):
        """Return the next pre-loaded result set without hitting the database.

        Pops and returns the first item from ``select_results``. Tests must
        enqueue one entry per SELECT that the code under test will issue,
        otherwise an ``IndexError`` will be raised (which is intentional — it
        signals that more results were consumed than were staged).

        Args:
            sql: The SQL SELECT statement (ignored by the fake).
            parm_list: The query parameter list (ignored by the fake).
            expected_rows: The expected row count hint (ignored by the fake).

        Returns:
            The first element popped from ``self.select_results``, which
            should be a list of row dicts matching the shape the real query
            would return.

        Raises:
            IndexError: If ``select_results`` is empty when called, indicating
                the test did not pre-load enough result sets.
        """
        return self.select_results.pop(0)

    def execute_dml(self, sql: str, parm_list, expected_rows):
        """Record a DML call without executing it against the database.

        Appends a dict containing the SQL and parameters to ``dml_calls`` so
        that tests can verify which statements were issued and with what
        values.

        Args:
            sql: The SQL DML statement (INSERT / UPDATE / DELETE).
            parm_list: The parameter list for the statement.
            expected_rows: The expected affected-row count (ignored by the
                fake).
        """
        self.dml_calls.append({"sql": sql, "params": parm_list})

    def execute_single_dml_row(self, sql: str, parm_list):
        """Record a single-row DML call without executing it against the database.

        Delegates to the same ``dml_calls`` log as ``execute_dml`` so that
        tests have a single place to inspect all DML activity regardless of
        which execute method the production code called.

        Args:
            sql: The SQL DML statement (INSERT / UPDATE / DELETE).
            parm_list: The parameter list for the statement.
        """
        self.dml_calls.append({"sql": sql, "params": parm_list})

    def execute_dml_row_within_transaction(self, sql: str, parm_list, transaction_cursor):
        """Record a transactional DML call without executing it against the database.

        Like ``execute_single_dml_row``, this appends to ``dml_calls`` and
        ignores the supplied cursor. The caller's transaction management code
        (commit / rollback) will still run, but because no real cursor is
        involved it has no effect.

        Args:
            sql: The SQL DML statement (INSERT / UPDATE / DELETE).
            parm_list: The parameter list for the statement.
            transaction_cursor: The psycopg cursor from the active transaction
                (ignored by the fake).
        """
        self.dml_calls.append({"sql": sql, "params": parm_list})
