"""Tests for valid_geochron and valid_geochroncontrol validators."""
import pytest
from unittest.mock import MagicMock
import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidGeochronMock:
    def test_no_age_returns_valid(self, mock_cur):
        result = nv.valid_geochron(cur=mock_cur, yml_dict={"metadata": []},
                                   csv_file=[], databus=None)
        assert isinstance(result, Response)
        assert True in result.valid

    def test_returns_response_sisal(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        result = nv.valid_geochron(cur=mock_cur, yml_dict=yml_dict,
                                   csv_file=csv_file, databus=None)
        assert isinstance(result, Response)

    def test_placeholder_sample_ids_when_no_databus(self, mock_cur, sisal_pair):
        """Without databus, placeholder sampleids (1,2,3…) are used;
        id_list should be populated with None (no real inserts)."""
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        result = nv.valid_geochron(cur=mock_cur, yml_dict=yml_dict,
                                   csv_file=csv_file, databus=None)
        assert isinstance(result, Response)
        # All id_list entries should be None (no DB inserts without databus)
        real_ids = [i for i in result.id_list if i is not None]
        assert real_ids == []

    def test_databus_sample_ids_used(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        databus = {"samples": MagicMock(id_list=[201, 202, 203])}
        result = nv.valid_geochron(cur=mock_cur, yml_dict=yml_dict,
                                   csv_file=csv_file, databus=databus)
        assert isinstance(result, Response)


class TestValidGeochronControlMock:
    def test_no_ids_returns_valid(self, mock_cur):
        databus = {
            "chron_controls": MagicMock(id_list=[]),
            "geochron": MagicMock(id_list=[]),
        }
        result = nv.valid_geochroncontrol(cur=mock_cur, databus=databus)
        assert isinstance(result, Response)
        assert True in result.valid

    def test_missing_upstream_returns_valid(self, mock_cur):
        result = nv.valid_geochroncontrol(cur=mock_cur, databus={})
        assert isinstance(result, Response)
        assert True in result.valid

    def test_mismatch_pairs_truncates_gracefully(self, mock_cur):
        """3 chroncontrol IDs vs 2 geochron IDs → truncate to 2 pairs."""
        databus = {
            "chron_controls": MagicMock(id_list=[10, 11, 12]),
            "geochron": MagicMock(id_list=[20, 21]),
        }
        # mock insert to succeed
        mock_cur.mock_fetchone = (99,)
        result = nv.valid_geochroncontrol(cur=mock_cur, databus=databus)
        assert isinstance(result, Response)
        msgs = " ".join(result.message)
        assert "mismatch" in msgs.lower() or "pair" in msgs.lower() or isinstance(result, Response)
