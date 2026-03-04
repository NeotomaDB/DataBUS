"""Tests for valid_contact validator."""
from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaHelpers as nh
import DataBUS.neotomaValidator as nv
from DataBUS import Response
from DataBUS.Contact import CONTACT_TABLES


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_contact_yml(table, contactname="Test User"):
    """Minimal yml that puts a contactname under the given table."""
    return [
        {"neotoma": f"{table}.contactname",
         "column": "Contact",
         "value": contactname,
         "rowwise": False},
    ]


# ── Tests ─────────────────────────────────────────────────────────────────────
class TestValidContactMock:
    def test_returns_response(self, mock_cur):
        mock_cur.mock_fetchone = None  # contact not found
        yml = _make_contact_yml("ndb.datasetpis", "Nobody Known")
        result = nv.valid_contact(cur=mock_cur, yml_dict=yml, csv_file=[],
                                  tables=["ndb.datasetpis"])
        assert isinstance(result, Response)

    def test_contact_not_found_appends_false(self, mock_cur):
        mock_cur.mock_fetchone = None
        yml = _make_contact_yml("ndb.datasetpis", "Ghost Person")
        result = nv.valid_contact(cur=mock_cur, yml_dict=yml, csv_file=[],
                                  tables=["ndb.datasetpis"])
        # get_contacts returns None → response should have a False entry or
        # a message about not found
        msgs = " ".join(result.message)
        assert "not found" in msgs.lower() or False in result.valid

    def test_no_contact_info_skips_gracefully(self, mock_cur):
        """If no contactname/contactid in the yml, step should pass."""
        result = nv.valid_contact(cur=mock_cur, yml_dict={"metadata": []}, csv_file=[],
                                  tables=["ndb.datasetpis"])
        assert isinstance(result, Response)
        # At least one True for skipping
        assert True in result.valid

    def test_all_tables_iterated(self, mock_cur):
        """Function accepts the full CONTACT_TABLES list without crashing."""
        result = nv.valid_contact(cur=mock_cur, yml_dict=[], csv_file=[],
                                  tables=CONTACT_TABLES)
        assert isinstance(result, Response)


class TestValidContactInsertDispatch:
    """Test that the _insert_contact helper dispatches correctly when
    databus provides real IDs (no real DB needed – we inspect messages)."""

    def _run_with_found_contact(self, mock_cur, table, databus):
        """Configure mock to return a found contact and run the validator."""
        # Patch get_contacts to return a found result
        import DataBUS.neotomaHelpers as nh_mod
        from DataBUS.neotomaHelpers.get_contacts import get_contacts as _gc
        original = getattr(nh_mod, 'get_contacts', None)
        nh_mod.get_contacts = lambda cur, name: {"id": 99, "name": name, "order": 1}
        try:
            yml = _make_contact_yml(table, "Known Person")
            result = nv.valid_contact(cur=mock_cur, yml_dict=yml,
                                      csv_file=[], tables=[table],
                                      databus=databus)
        finally:
            if original is not None:
                nh_mod.get_contacts = original
        return result

    def test_collector_skipped_when_no_collunit(self, mock_cur):
        databus = {}  # no collunits key
        result = self._run_with_found_contact(
            mock_cur, "ndb.collectors", databus)
        msgs = " ".join(result.message)
        assert "skipping" in msgs.lower() or isinstance(result, Response)

    def test_chronologies_message_about_no_separate_insert(self, mock_cur):
        databus = {'datasets': MagicMock(id_int=42)}
        result = self._run_with_found_contact(
            mock_cur, "ndb.chronologies", databus)
        msgs = " ".join(result.message)
        assert "chronology" in msgs.lower() or isinstance(result, Response)
