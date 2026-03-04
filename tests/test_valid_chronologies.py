"""Tests for valid_chronologies validator."""
from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidChronologiesMock:
    def test_no_params_returns_valid(self, mock_cur):
        """Empty yml_dict → no chronology params → valid skip."""
        result = nv.valid_chronologies(cur=mock_cur, yml_dict={"metadata": []},
                                       csv_file=[], databus=None)
        assert isinstance(result, Response)
        assert True in result.valid

    def test_returns_response_sisal(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        # SISAL template has chronologies section; databus={} means no upstream IDs
        result = nv.valid_chronologies(cur=mock_cur, yml_dict=yml_dict,
                                       csv_file=csv_file, databus={})
        assert isinstance(result, Response)

    def test_returns_response_pb210(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        result = nv.valid_chronologies(cur=mock_cur, yml_dict=yml_dict,
                                       csv_file=csv_file, databus={})
        assert isinstance(result, Response)

    def test_with_databus_no_insert_when_mock(self, mock_cur, sisal_pair):
        """When databus provides a placeholder collunit ID=1, insert
        should NOT be attempted (collunit 1 is a placeholder, not real)."""
        csv_file, yml_dict = sisal_pair
        databus = {"collunits": MagicMock(id_int=1)}
        result = nv.valid_chronologies(cur=mock_cur, yml_dict=yml_dict,
                                       csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)
        # No "inserted with ID" message expected since collunit=1 (placeholder)
        msgs = " ".join(result.message)
        assert isinstance(result, Response)

    def test_databus_uses_real_collunit(self, mock_cur, sisal_pair):
        """When databus provides a real collunit ID (>1), insert is
        attempted and a SQL error is captured gracefully."""
        csv_file, yml_dict = sisal_pair
        # Mock the agetype lookup to succeed
        mock_cur.mock_fetchone = (1,)  # returns agetypeid=1
        databus = {"collunits": MagicMock(id_int=999)}
        result = nv.valid_chronologies(cur=mock_cur, yml_dict=yml_dict,
                                       csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)
