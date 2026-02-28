"""Tests for valid_chroncontrols validator."""
import pytest
from unittest.mock import MagicMock
import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidChroncontrolsMock:
    def test_no_params_returns_valid(self, mock_cur):
        result = nv.valid_chroncontrols(cur=mock_cur, yml_dict={"metadata": []},
                                        csv_file=[], databus=None)
        assert isinstance(result, Response)
        assert True in result.valid

    def test_returns_response_pb210(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        # Mock lookup queries to return valid IDs
        mock_cur.mock_fetchone = (1,)
        result = nv.valid_chroncontrols(cur=mock_cur, yml_dict=yml_dict,
                                        csv_file=csv_file, databus=None)
        assert isinstance(result, Response)

    def test_signature_accepts_cur_first(self, mock_cur):
        """Regression: old signature had yml_dict first. Verify new order."""
        result = nv.valid_chroncontrols(cur=mock_cur,
                                        yml_dict=[], csv_file=[])
        assert isinstance(result, Response)

    def test_databus_provides_analysisunit_ids(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        mock_cur.mock_fetchone = (1,)
        databus = {
            "chronologies": MagicMock(id_int=999),
            "analysisunits": MagicMock(id_list=[101, 102, 103]),
        }
        result = nv.valid_chroncontrols(cur=mock_cur, yml_dict=yml_dict,
                                        csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)
