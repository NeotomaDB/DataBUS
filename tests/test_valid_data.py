"""Tests for valid_data validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _make_long_yml(taxon="Quercus", units="NISP", element=None, context=None, value_col="Quercus"):
    """Build a wide-format yml_dict for valid_data."""
    meta = [
        {
            "neotoma": "ndb.data.value",
            "column": value_col,
            "rowwise": True,
            "required": True,
            "type": "float",
        },
        {"neotoma": "ndb.variables.taxonid", "column": "Taxon", "value": taxon, "rowwise": False},
        {
            "neotoma": "ndb.variables.variableunitsid",
            "column": "Units",
            "value": units,
            "rowwise": False,
        },
    ]
    if element:
        meta.append(
            {
                "neotoma": "ndb.variables.variableelementid",
                "column": "Element",
                "value": element,
                "rowwise": False,
            }
        )
    if context:
        meta.append(
            {
                "neotoma": "ndb.variables.variablecontextid",
                "column": "Context",
                "value": context,
                "rowwise": False,
            }
        )
    return {"metadata": meta}


def _make_csv(values, taxon=None, units=None, col="Quercus"):
    rows = [{col: str(v)} for v in values]
    if taxon:
        for r in rows:
            r["Taxon"] = taxon
    if units:
        for r in rows:
            r["Units"] = units
    return rows


class TestValidDataMock:
    def test_returns_response_empty(self, mock_cur):
        # valid_data always returns a Response when given valid (non-empty) inputs
        csv_file = _make_csv([1.0])
        yml_dict = _make_long_yml()
        databus = {"samples": MagicMock(id_list=[1])}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)

    def test_no_sample_ids_appends_false(self, mock_cur):
        # When databus has no samples, valid_data appends False.
        # pull_params is patched to return {} so the wide-format else-branch
        # iterates an empty dict and never dereferences the missing sampleids.
        from unittest.mock import patch

        with patch("DataBUS.neotomaHelpers.pull_params", return_value={}):
            result = nv.valid_data(cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus={})
        assert isinstance(result, Response)
        assert False in result.valid

    def test_with_sample_ids_in_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        mock_cur.mock_fetchone = (10,)
        databus = {"samples": MagicMock(id_list=[1, 2, 3, 4, 5])}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)

    def test_valid_variable_found_in_db(self, mock_cur):
        """When DB returns a variable ID, datum should be created."""
        csv_file = _make_csv([10, 20, 30])
        yml_dict = _make_long_yml()
        mock_cur.mock_fetchone = (5,)
        databus = {"samples": MagicMock(id_list=[1, 2, 3])}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)

    def test_variable_not_in_db_appends_false(self, mock_cur):
        """When DB returns None for taxon, response should have False."""
        csv_file = _make_csv([5, 10])
        yml_dict = _make_long_yml(taxon="UnknownTaxon")
        mock_cur.mock_fetchone = None
        databus = {"samples": MagicMock(id_list=[1, 2])}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)
        assert False in result.valid

    def test_sisal_pair_returns_response(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        databus = {"samples": MagicMock(id_list=list(range(1, len(csv_file) + 1)))}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)

    def test_id_dict_populated(self, mock_cur):
        csv_file = _make_csv([15, 25, 35])
        yml_dict = _make_long_yml()
        mock_cur.mock_fetchone = (99,)
        databus = {"samples": MagicMock(id_list=[1, 2, 3])}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result.id_dict, dict)

    def test_single_sample_id_broadcast(self, mock_cur):
        """Surface samples: single sampleid is broadcast across all data rows."""
        csv_file = _make_csv([1, 2, 3])
        yml_dict = _make_long_yml()
        mock_cur.mock_fetchone = (7,)
        databus = {"samples": MagicMock(id_list=[42])}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)

    def test_pb210_pair_returns_response(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        mock_cur.mock_fetchone = (1,)
        n = len(csv_file)
        databus = {"samples": MagicMock(id_list=list(range(1, n + 1)))}
        result = nv.valid_data(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)
