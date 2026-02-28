"""Tests for valid_site validator using real and mock data."""
import pytest
from unittest.mock import MagicMock
import DataBUS.neotomaValidator as nv
import DataBUS.neotomaHelpers as nh
from DataBUS import Response
from tests.conftest import real_csv, real_yml, toy_csv


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_minimal_yml_dict(sitename="Test Site", lat=45.0, lon=-90.0):
    """Build the minimal yml_dict structure that valid_site expects."""
    return {"metadata": [
        {"neotoma": "ndb.sites.sitename",      "column": "SiteName",
         "value": sitename, "rowwise": False},
        {"neotoma": "ndb.sites.geog.latitude",  "column": "Lat",
         "value": lat,      "rowwise": False},
        {"neotoma": "ndb.sites.geog.longitude", "column": "Lon",
         "value": lon,      "rowwise": False},
    ]}


# ── Tests: no DB (mock cursor) ────────────────────────────────────────────────
class TestValidSiteMock:
    """Offline tests relying on MockCursor."""

    def test_returns_response(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        result = nv.valid_site(cur=mock_cur, yml_dict=yml_dict,
                               csv_file=csv_file)
        assert isinstance(result, Response)

    def test_invalid_coordinates_returns_false(self, mock_cur):
        yml = _make_minimal_yml_dict(sitename="Bad Site", lat=999.0, lon=-90.0)
        result = nv.valid_site(cur=mock_cur, yml_dict=yml, csv_file=[])
        assert False in result.valid


# ── Tests: SISAL dataset ──────────────────────────────────────────────────────
class TestValidSiteSISAL:
    def test_sisal_site_response_type(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        result = nv.valid_site(cur=mock_cur, yml_dict=yml_dict,
                               csv_file=csv_file)
        assert isinstance(result, Response)

    def test_sisal_response_has_messages(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        result = nv.valid_site(cur=mock_cur, yml_dict=yml_dict,
                               csv_file=csv_file)
        assert len(result.message) > 0


# ── Tests: 210Pb dataset ──────────────────────────────────────────────────────
class TestValidSite210Pb:
    def test_pb210_site_response_type(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        result = nv.valid_site(cur=mock_cur, yml_dict=yml_dict,
                               csv_file=csv_file)
        assert isinstance(result, Response)
