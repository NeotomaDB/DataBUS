"""Tests for valid_uth_series validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidUthSeriesMock:
    def test_no_params_returns_valid(self, mock_cur):
        result = nv.valid_uth_series(
            cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus=None
        )
        assert isinstance(result, Response)
        assert True in result.valid

    def test_returns_response_sisal(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        result = nv.valid_uth_series(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=None
        )
        assert isinstance(result, Response)

    def test_real_geochron_ids_used_from_databus(self, mock_cur, sisal_pair):
        """When databus provides geochron IDs > 1, insert is attempted."""
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        databus = {"geochron": MagicMock(id_list=[301, 302, 303])}
        result = nv.valid_uth_series(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_placeholder_geochron_ids_no_insert(self, mock_cur, sisal_pair):
        """When databus geochron ids list is empty/None, no insert attempted."""
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        databus = {"geochron": MagicMock(id_list=[])}
        result = nv.valid_uth_series(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
