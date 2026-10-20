"""Tests for the run_step wrapper."""

from unittest.mock import MagicMock

from psycopg2 import extensions as pg_extensions

import DataBUS.neotomaHelpers as nh
from DataBUS import Response


def make_conn():
    conn = MagicMock()
    conn.get_transaction_status.return_value = pg_extensions.TRANSACTION_STATUS_INTRANS
    return conn


def executed(conn):
    return [call.args[0] for call in conn.cursor.return_value.execute.call_args_list]


def valid_response(message):
    response = Response()
    response.valid.append(True)
    response.message.append(message)
    return response


class TestRunStep:
    def test_stores_response_under_key_and_logs_it(self):
        logfile, databus = [], {}
        response = valid_response("✔ Site found.")

        nh.run_step("Sites", "sites", lambda: response, logfile, databus, make_conn())

        assert databus["sites"] is response
        assert logfile[0] == "\n === Sites ==="
        assert any("✔ Site found." in line for line in logfile[1:])

    def test_title_and_key_are_independent(self):
        logfile, databus = [], {}
        conn = make_conn()

        nh.run_step("GPUs", "gpuid", lambda: valid_response("ok"), logfile, databus, conn)

        assert "gpuid" in databus
        assert "\n === GPUs ===" in logfile
        assert executed(conn) == ["SAVEPOINT sp_gpuid", "RELEASE SAVEPOINT sp_gpuid"]

    def test_failing_step_is_stored_invalid_and_later_steps_still_run(self):
        logfile, databus = [], {}
        conn = make_conn()

        def boom():
            raise ValueError("broken")

        nh.run_step("Sites", "sites", boom, logfile, databus, conn)
        nh.run_step("CUs", "collunits", lambda: valid_response("ok"), logfile, databus, conn)

        assert not databus["sites"].validAll
        assert any("broken" in line for line in logfile)
        assert databus["collunits"].validAll
        assert "ROLLBACK TO SAVEPOINT sp_sites" in executed(conn)

    def test_works_with_mock_connection(self, mock_conn):
        logfile, databus = [], {}

        nh.run_step("Sites", "sites", lambda: valid_response("ok"), logfile, databus, mock_conn)

        assert databus["sites"].validAll
