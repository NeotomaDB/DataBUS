"""Tests for the valid_pbmodel validator.

A 210Pb model applies one basis + cumulative inventory across every analysis
unit in the core, so the validator emits one ``insert_lead_model`` call per
analysis unit id on the databus.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _entry(field, column, type_spec="string"):
    return {
        "neotoma": f"ndb.leadmodels.{field}",
        "column": column,
        "rowwise": False,
        "required": False,
        "type": type_spec,
    }


def _pb(basis="constant rate of supply", inventory="145.3"):
    """(yml_dict, csv_file) describing one lead model.

    ``pull_params`` reads from the CSV by column name, so every field needs a
    metadata entry naming its column plus a row carrying the value.
    """
    yml = {
        "metadata": [
            _entry("pbbasisid", "PbBasis"),
            _entry("cumulativeinventory", "CumInventory", "float"),
        ]
    }
    return yml, [{"PbBasis": basis, "CumInventory": inventory}]


@pytest.fixture
def databus():
    aus = Response()
    aus.id_list = [201, 202, 203]
    return {"analysisunits": aus}


def _model_inserts(cur):
    """Params of every insert_lead_model SELECT.

    ``LeadModel.insert_to_db`` first replays the CREATE FUNCTION DDL — whose
    body also mentions insert_lead_model — with no params, so require params
    to pick out the real inserts.
    """
    return [p for q, p in cur._execute_calls if p and "insert_lead_model" in q]


class TestValidPbModel:
    def test_no_parameters_is_valid(self, mock_cur, databus):
        response = nv.valid_pbmodel(mock_cur, {"metadata": []}, csv_file=[], databus=databus)

        assert response.validAll
        assert any("No lead model parameters provided" in m for m in response.message)
        assert mock_cur._execute_calls == []

    def test_one_model_per_analysis_unit(self, mock_cur, databus):
        mock_cur.mock_fetchone = (4,)
        yml, csv_file = _pb()
        response = nv.valid_pbmodel(mock_cur, yml, csv_file, databus)

        assert response.validAll
        assert "✔  Lead model can be inserted." in response.message
        inserts = _model_inserts(mock_cur)
        assert [p["analysisunitid"] for p in inserts] == [201, 202, 203]

    def test_basis_name_resolved_to_id(self, mock_cur, databus):
        mock_cur.mock_fetchone = (4,)
        yml, csv_file = _pb(basis="constant rate of supply")
        nv.valid_pbmodel(mock_cur, yml, csv_file, databus)

        basis_query, basis_params = mock_cur._execute_calls[0]
        assert "ndb.leadmodelbasis" in basis_query
        assert basis_params == {"pbbasisid": "constant rate of supply"}
        assert all(p["pbbasisid"] == 4 for p in _model_inserts(mock_cur))

    def test_cumulative_inventory_is_carried_through(self, mock_cur, databus):
        mock_cur.mock_fetchone = (4,)
        yml, csv_file = _pb(inventory="145.3")
        nv.valid_pbmodel(mock_cur, yml, csv_file, databus)

        assert all(p["cumulativeinventory"] == 145.3 for p in _model_inserts(mock_cur))

    def test_unknown_basis_leaves_id_none(self, mock_cur, databus):
        mock_cur.mock_fetchone = None
        yml, csv_file = _pb(basis="not a real basis")
        nv.valid_pbmodel(mock_cur, yml, csv_file, databus)

        assert all(p["pbbasisid"] is None for p in _model_inserts(mock_cur))

    def test_missing_analysis_units_uses_placeholder_range(self, mock_cur):
        mock_cur.mock_fetchone = (4,)
        yml, csv_file = _pb()
        response = nv.valid_pbmodel(mock_cur, yml, csv_file, databus={})

        assert not response.validAll
        assert any("using placeholder range" in m for m in response.message)
        # The placeholder is range(1, 10) -> nine models.
        assert len(_model_inserts(mock_cur)) == 9

    def test_insert_failure_is_reported_once(self, mock_cur, databus):
        calls = []

        def execute(query, params=None):
            calls.append((query, params))
            if "insert_lead_model" in query:
                raise RuntimeError("no such function")

        mock_cur.execute = execute
        mock_cur.mock_fetchone = (4,)
        yml, csv_file = _pb()
        response = nv.valid_pbmodel(mock_cur, yml, csv_file, databus)

        assert not response.validAll
        failures = [m for m in response.message if "Lead model cannot be inserted" in m]
        assert len(failures) == 1
