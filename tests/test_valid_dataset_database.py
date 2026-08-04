"""Tests for the valid_dataset_database validator.

``valid_dataset_database`` is the only validator with no ``csv_file`` argument —
it reads the database name from a ``value:`` constant in the template and
resolves it against ``ndb.constituentdatabases``.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _db_yml(name="Neotoma"):
    return {
        "metadata": [
            {
                "neotoma": "ndb.datasetdatabases.databasename",
                "column": "databasename",
                "value": name,
                "rowwise": False,
                "required": False,
                "type": "string",
            }
        ]
    }


@pytest.fixture
def databus():
    datasets = Response()
    datasets.id_int = 74655
    return {"datasets": datasets}


class TestValidDatasetDatabase:
    def test_resolves_and_inserts(self, mock_cur, databus):
        mock_cur.mock_fetchone = (17,)
        response = nv.valid_dataset_database(mock_cur, _db_yml(), databus=databus)

        assert response.validAll
        assert response.id_int == 17
        assert "ts.insertdatasetdatabase" in mock_cur.last_query
        assert mock_cur.last_params == {"datasetid": 74655, "databaseid": 17}

    def test_lookup_is_lowercased_and_stripped(self, mock_cur, databus):
        mock_cur.mock_fetchone = (17,)
        nv.valid_dataset_database(mock_cur, _db_yml("  NEOTOMA  "), databus=databus)

        first_query, first_params = mock_cur._execute_calls[0]
        assert "ndb.constituentdatabases" in first_query
        assert first_params == {"databasename": "neotoma"}

    def test_unknown_database_returns_early(self, mock_cur, databus):
        mock_cur.mock_fetchone = None
        response = nv.valid_dataset_database(mock_cur, _db_yml("Nowhere"), databus=databus)

        assert not response.validAll
        assert any("not found in Neotoma" in m for m in response.message)
        # Returned before any DatasetDatabase was built.
        assert response.id_int is None
        assert len(mock_cur._execute_calls) == 1

    def test_missing_databus_uses_placeholder_dataset(self, mock_cur):
        mock_cur.mock_fetchone = (17,)
        response = nv.valid_dataset_database(mock_cur, _db_yml(), databus=None)

        assert not response.validAll
        assert any("Cannot retrieve Dataset ID" in m for m in response.message)
        # Still builds the object against the placeholder datasetid of 1.
        assert response.id_int == 17
        assert mock_cur.last_params == {"datasetid": 1, "databaseid": 17}

    def test_non_string_name_skips_lookup(self, mock_cur, databus):
        """A numeric databasename means no lookup runs, so databaseid is absent."""
        response = nv.valid_dataset_database(mock_cur, _db_yml(5), databus=databus)

        assert isinstance(response, Response)
        assert not response.validAll
        assert any("Cannot create Database object" in m for m in response.message)
