"""
Shared pytest fixtures for the DataBUS test suite.

Two database strategies are supported:
  1. A real PostgreSQL connection via the PGDB_TANK environment variable.
  2. A lightweight mock cursor (MockCursor / MockConnection) for offline runs
     when no database is reachable.

All test modules should use the `cur` and `conn` fixtures so that the tests
are automatically skipped when a real connection is unavailable and a mock
is not sufficient (e.g. tests that explicitly require real DB semantics).
"""

import json
import os
from unittest.mock import MagicMock

import pytest

# ── paths ─────────────────────────────────────────────────────────────────────
TESTS_DIR  = os.path.dirname(__file__)
TOY_DIR    = os.path.join(TESTS_DIR, "toy_data")
DATA_DIR   = os.path.join(TESTS_DIR, "..", "data")


# ── toy data helpers ──────────────────────────────────────────────────────────
def toy_csv(name):
    """Return absolute path to a toy CSV file."""
    return os.path.join(TOY_DIR, name)


def real_csv(folder, name):
    """Return absolute path to a real data CSV file."""
    return os.path.join(DATA_DIR, folder, name)


def real_yml(folder, name):
    """Return absolute path to a real template YAML file."""
    return os.path.join(DATA_DIR, folder, name)


# ── mock database objects ─────────────────────────────────────────────────────
class MockCursor:
    """A minimal psycopg2-cursor substitute for offline testing.

    Behaviour:
      * execute() stores the last query/params for inspection.
      * fetchone() returns a configurable value (default: None).
      * fetchall() returns a configurable list (default: []).
      * description is set to None by default.

    Per-test customisation is done via the ``mock_fetchone`` and
    ``mock_fetchall`` attributes.
    """

    def __init__(self):
        self.last_query  = None
        self.last_params = None
        self.mock_fetchone  = None
        self.mock_fetchall  = []
        self.description    = []
        self._execute_calls = []

    def execute(self, query, params=None):
        self.last_query  = query
        self.last_params = params
        self._execute_calls.append((query, params))

    def fetchone(self):
        return self.mock_fetchone

    def fetchall(self):
        return self.mock_fetchall


class MockConnection:
    """Minimal psycopg2-connection substitute."""

    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def rollback(self):
        pass

    def execute(self, sql):
        """Allow SAVEPOINT / RELEASE / ROLLBACK TO statements."""
        pass


# ── fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def real_connection():
    """Attempt to open a real DB connection from PGDB_TANK env var.

    Returns None if not available (tests that need a real connection
    should skip when this fixture is None).
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
        import psycopg2
        conn_str = os.getenv("PGDB_TANK")
        if not conn_str:
            return None
        conn_data = json.loads(conn_str)
        conn = psycopg2.connect(**conn_data, connect_timeout=5)
        return conn
    except Exception:
        return None


@pytest.fixture
def mock_cur():
    """Return a fresh MockCursor for each test."""
    return MockCursor()


@pytest.fixture
def mock_conn(mock_cur):
    """Return a fresh MockConnection wrapping mock_cur."""
    return MockConnection(mock_cur)


@pytest.fixture
def cur(real_connection, mock_cur):
    """Provide the best available cursor.

    Prefers the real DB cursor; falls back to mock when the DB is offline.
    """
    if real_connection is not None:
        return real_connection.cursor()
    return mock_cur


@pytest.fixture
def conn(real_connection, mock_conn):
    """Provide the best available connection (real or mock)."""
    if real_connection is not None:
        return real_connection
    return mock_conn


# ── template loading helpers ──────────────────────────────────────────────────
@pytest.fixture
def sisal_pair():
    """Return (csv_file_rows, yml_dict) for the SISAL toy dataset."""
    import DataBUS.neotomaHelpers as nh
    csv_path = real_csv("SISAL", "sisal_entity_13.csv")
    yml_path = real_yml("SISAL", "template.yml")
    return nh.read_csv(csv_path), nh.template_to_dict(yml_path)


@pytest.fixture
def pb210_pair():
    """Return (csv_file_rows, yml_dict) for the 210Pb toy dataset."""
    import DataBUS.neotomaHelpers as nh
    csv_path = real_csv("210Pb", "Cayou 1993 VOYA.csv")
    yml_path = real_yml("210Pb", "template.yml")
    return nh.read_csv(csv_path), nh.template_to_dict(yml_path)


@pytest.fixture
def node_pair():
    """Return (csv_file_rows, yml_dict) for the NODE toy dataset."""
    import DataBUS.neotomaHelpers as nh
    csv_path = real_csv("NODE-OST", "NODE-R32.csv")
    yml_path = real_yml("NODE-OST", "node_template.yml")
    return nh.read_csv(csv_path), nh.template_to_dict(yml_path)


@pytest.fixture
def eanode_pair():
    """Return (csv_file_rows, yml_dict) for the EANODE toy dataset."""
    import DataBUS.neotomaHelpers as nh
    csv_path = real_csv("EANODE-OST", "EA000017.csv")
    yml_path = real_yml("EANODE-OST", "eanode_template.yml")
    return nh.read_csv(csv_path), nh.template_to_dict(yml_path)
