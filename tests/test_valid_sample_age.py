"""Tests for valid_sample_age validator."""
from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidSampleAgeMock:
    def test_returns_response_no_sample_ages(self, mock_cur):
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus={}
        )
        assert isinstance(result, Response)
        assert True in result.valid

    def test_no_chronologies_in_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        databus = {
            "samples": MagicMock(id_list=list(range(1, n + 1))),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_no_samples_returns_early(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        databus = {
            "chronologies": MagicMock(name={"default": 1}),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_with_full_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (1,)
        databus = {
            "chronologies": MagicMock(name={"default": 1, "CRS": 2}),
            "samples": MagicMock(id_list=list(range(1, n + 1))),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_sisal_pair(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (1,)
        databus = {
            "chronologies": MagicMock(name={"default": 1}),
            "samples": MagicMock(id_list=list(range(1, n + 1))),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_empty_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus={}
        )
        assert isinstance(result, Response)
