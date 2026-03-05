"""Tests for valid_sample validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidSampleMock:
    def test_returns_response_no_databus_ids(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        databus = {
            "analysisunits": MagicMock(id_list=[], counter=5),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_with_real_au_ids(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (10,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert result.counter >= 1

    def test_counter_equals_rows(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (5,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert result.counter == n

    def test_sisal_pair(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (1,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_no_au_ids_uses_placeholder(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        databus = {
            "analysisunits": MagicMock(id_list=[], counter=3),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert False in result.valid

    def test_id_list_populated_on_insert(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (99,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result.id_list, list)
