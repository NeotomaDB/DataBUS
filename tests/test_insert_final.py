"""Tests for insert_final, the last step of an upload.

It writes one row to ``ndb.datasetsubmissions`` with a fixed submissiontypeid
of 6 and today's date, pulling the three ids straight off the databus.
"""

from datetime import datetime

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _response(id_int):
    response = Response()
    response.id_int = id_int
    return response


@pytest.fixture
def databus():
    return {
        "datasets": _response(74655),
        "database": _response(17),
        "contacts": _response(42),
    }


class TestInsertFinal:
    def test_inserts_submission(self, mock_cur, databus):
        response = nv.insert_final(mock_cur, databus)

        assert response.validAll
        assert response.message == ["✔ Dataset submission has been finalized"]
        assert "ndb.datasetsubmissions" in mock_cur.last_query

    def test_submission_row_contents(self, mock_cur, databus):
        nv.insert_final(mock_cur, databus)

        assert mock_cur.last_params == {
            "datasetid": 74655,
            "databaseid": 17,
            "contactid": 42,
            "submissiontypeid": 6,
            "submissiondate": datetime.now().date(),
        }

    def test_execute_failure_is_reported(self, mock_cur, databus):
        def boom(query, params=None):
            raise RuntimeError("connection lost")

        mock_cur.execute = boom
        response = nv.insert_final(mock_cur, databus)

        assert not response.validAll
        assert any("cannot be finalized" in m for m in response.message)
        assert any("connection lost" in m for m in response.message)

    def test_missing_databus_key_raises(self, mock_cur):
        """The ids are read before the try block, so a missing key propagates."""
        with pytest.raises(KeyError):
            nv.insert_final(mock_cur, {"datasets": _response(1)})
