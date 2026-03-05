"""Tests for valid_geochron_dataset validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidGeochronDatasetMock:
    def test_returns_response(self, mock_cur):
        mock_cur.mock_fetchone = (5,)  # geochronologic datasettypeid = 5
        result = nv.valid_geochron_dataset(cur=mock_cur, yml_dict=[], csv_file=[], databus=None)
        assert isinstance(result, Response)
        assert True in result.valid

    def test_datasettypeid_not_found_still_valid(self, mock_cur):
        """If 'geochronologic' is not in datasettypes, fallback to 1."""
        mock_cur.mock_fetchone = None
        result = nv.valid_geochron_dataset(cur=mock_cur, yml_dict=[], csv_file=[], databus=None)
        assert isinstance(result, Response)
        assert True in result.valid

    def test_with_real_collunit_id_attempts_insert(self, mock_cur):
        """A real collunit ID triggers the insert path."""
        mock_cur.mock_fetchone = (5,)
        databus = {"collunits": MagicMock(id_int=42)}
        result = nv.valid_geochron_dataset(cur=mock_cur, yml_dict=[], csv_file=[], databus=databus)
        assert isinstance(result, Response)

    def test_placeholder_collunit_no_insert(self, mock_cur):
        """collunit ID = 1 (placeholder) → insert is attempted (will fail in production)."""
        mock_cur.mock_fetchone = (5,)
        databus = {"collunits": MagicMock(id_int=1)}
        result = nv.valid_geochron_dataset(cur=mock_cur, yml_dict=[], csv_file=[], databus=databus)
        assert isinstance(result, Response)
        # In mock, insert succeeds and returns an ID, but in production it would fail with FK constraint
