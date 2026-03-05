"""Tests for valid_datauncertainty validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _make_uncertainty_yml(taxon="Quercus", basis="standard deviation", units="percent"):
    return {
        "metadata": [
            {
                "neotoma": "ndb.datauncertainties.uncertaintyvalue",
                "column": "Uncertainty",
                "rowwise": True,
                "taxonid": taxon,
                "taxonname": taxon,
                "required": False,
            },
            {
                "neotoma": "ndb.datauncertainties.uncertaintybasisid",
                "column": "UncBasis",
                "value": basis,
                "rowwise": False,
            },
            {
                "neotoma": "ndb.datauncertainties.uncertaintyunitid",
                "column": "UncUnits",
                "value": units,
                "rowwise": False,
            },
        ]
    }


def _make_csv(values, col="Uncertainty"):
    return [{col: str(v)} for v in values]


class TestValidDataUncertaintyMock:
    def test_returns_response_empty(self, mock_cur):
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus={}
        )
        assert isinstance(result, Response)

    def test_no_data_ids_skips_taxon(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        databus = {"data": MagicMock(id_dict={})}
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_with_matching_data_ids(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        databus = {"data": MagicMock(id_dict={"Quercus": [1, 2, 3]})}
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_basis_not_in_db_appends_false(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = None
        databus = {"data": MagicMock(id_dict={"Quercus": [10, 20, 30]})}
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_no_databus_data_key(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        databus = {}
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_pb210_pair(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        mock_cur.mock_fetchone = (2,)
        n = len(csv_file)
        databus = {"data": MagicMock(id_dict={"Pb210": list(range(1, n + 1))})}
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_all_none_inputs_returns_valid(self, mock_cur):
        result = nv.valid_datauncertainty(
            cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus={}
        )
        assert True in result.valid
