"""Tests for the safe_step savepoint wrapper."""

from unittest.mock import MagicMock

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2 import extensions as pg_extensions

import DataBUS.neotomaHelpers as nh
from DataBUS import Response


def make_conn(status=pg_extensions.TRANSACTION_STATUS_INTRANS):
    conn = MagicMock()
    conn.get_transaction_status.return_value = status
    return conn


def executed(conn):
    return [call.args[0] for call in conn.cursor.return_value.execute.call_args_list]


class TestSafeStepSuccess:
    def test_returns_fn_result_and_releases(self):
        conn = make_conn()
        response = Response()
        response.valid.append(True)
        logfile = []

        result = nh.safe_step("sites", lambda: response, logfile, conn)

        assert result is response
        assert result.validAll
        assert executed(conn) == ["SAVEPOINT sp_sites", "RELEASE SAVEPOINT sp_sites"]
        assert logfile == []

    def test_works_with_mock_connection(self, mock_conn):
        response = Response()
        response.valid.append(True)
        assert nh.safe_step("sites", lambda: response, [], mock_conn) is response


class TestSafeStepSwallowedDatabaseError:
    """fn() caught a database error itself and left the transaction aborted."""

    def test_keeps_the_step_messages(self):
        conn = make_conn(pg_extensions.TRANSACTION_STATUS_INERROR)
        response = Response()
        response.valid.append(False)
        response.message.append("✗ Sample Age could not be inserted. null value in column")

        result = nh.safe_step("sample_age", lambda: response, [], conn)

        assert result is response
        assert result.message[0] == "✗ Sample Age could not be inserted. null value in column"
        assert "rolled back" in result.message[-1]
        assert not result.validAll

    def test_rolls_back_to_savepoint(self):
        conn = make_conn(pg_extensions.TRANSACTION_STATUS_INERROR)

        nh.safe_step("sample_age", lambda: Response(), [], conn)

        assert executed(conn) == [
            "SAVEPOINT sp_sample_age",
            "ROLLBACK TO SAVEPOINT sp_sample_age",
            "RELEASE SAVEPOINT sp_sample_age",
        ]

    def test_marks_a_passing_response_invalid(self):
        conn = make_conn(pg_extensions.TRANSACTION_STATUS_INERROR)
        response = Response()
        response.valid.append(True)

        result = nh.safe_step("sample_age", lambda: response, [], conn)

        assert not result.validAll


class TestSafeStepRaises:
    def test_plain_exception_reports_type_message_and_location(self):
        conn = make_conn()

        def boom():
            raise ValueError("age is not a number")

        logfile = []
        result = nh.safe_step("sample_age", boom, logfile, conn)

        assert isinstance(result, Response)
        assert not result.validAll
        msg = result.message[0]
        assert "ValueError: age is not a number" in msg
        assert "test_safe_step.py" in msg and "in boom" in msg
        assert "Unexpected error" not in msg
        assert "ROLLBACK TO SAVEPOINT sp_sample_age" in executed(conn)
        # The message travels in the Response only, so it is logged once.
        assert logfile == []

    def test_database_error_reports_message(self):
        conn = make_conn()

        def boom():
            raise psycopg2.IntegrityError("null value in column sampleid")

        result = nh.safe_step("sample_age", boom, [], conn)

        msg = result.message[0]
        assert "Database error (IntegrityError)" in msg
        assert "null value in column sampleid" in msg

    def test_database_error_reports_diagnostics(self):
        class FakeDiag:
            message_primary = "null value in column sampleid"
            message_detail = "Failing row contains (null, 5)."
            message_hint = None
            table_name = "sampleages"
            column_name = "sampleid"
            constraint_name = None
            context = "PL/pgSQL function ts.insertsampleage"

        described = nh.safe_step.__globals__["_describe_error"]

        class FakeError(psycopg2.Error):
            diag = FakeDiag()

        msg = described(FakeError("raw"))

        assert "null value in column sampleid" in msg
        assert "Detail: Failing row contains (null, 5)." in msg
        assert "Table: sampleages" in msg
        assert "Column: sampleid" in msg
        assert "Context: PL/pgSQL function ts.insertsampleage" in msg
        assert "Hint" not in msg

    def test_aborted_transaction_is_explained(self):
        conn = make_conn()

        def boom():
            raise pg_errors.InFailedSqlTransaction("current transaction is aborted")

        result = nh.safe_step("sample_age", boom, [], conn)

        msg = result.message[0]
        assert "failed earlier in this step" in msg
        assert "current transaction is aborted" not in msg


class TestSafeStepSavepointUnavailable:
    def test_returns_failed_response(self):
        conn = make_conn()
        conn.cursor.return_value.execute.side_effect = psycopg2.OperationalError("gone")
        logfile = []

        result = nh.safe_step("sites", lambda: Response(), logfile, conn)

        assert not result.validAll
        assert "Could not establish savepoint" in result.message[0]
        assert len(logfile) == 1
