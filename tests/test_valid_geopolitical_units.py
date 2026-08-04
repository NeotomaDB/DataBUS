"""Tests for the valid_geopolitical_units validator.

Geopolitical units are resolved top-down: the national unit is looked up by
name alone, then each subnational level is looked up by name *and* by the id of
the level above it, so 'Washington' the county resolves under the right state.
Each level found is linked to the site via ``ts.insertsitegeopol``.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _entry(field):
    return {
        "neotoma": f"ndb.sitegeopolitical.{field}",
        "column": field,
        "rowwise": False,
        "required": False,
        "type": "string",
    }


def _geopol(**levels):
    """Build the (yml_dict, csv_file) pair for a set of geopolitical levels.

    ``pull_params`` reads values out of the CSV rows by column name — a
    ``value:`` constant in the template is only honoured by the
    ``retrieve_dict``-based validators — so each level needs both a metadata
    entry naming the column and a CSV row carrying it.
    """
    yml = {"metadata": [_entry(k) for k in levels]}
    csv_file = [dict(levels)]
    return yml, csv_file


@pytest.fixture
def databus():
    sites = Response()
    sites.id_int = 5001
    return {"sites": sites}


def _lookups(cur):
    """Params of the geopoliticalunits SELECTs, in order (excludes the inserts)."""
    return [p for q, p in cur._execute_calls if "ndb.geopoliticalunits" in q]


def _inserts(cur):
    return [p for q, p in cur._execute_calls if "insertsitegeopol" in q]


class TestValidGeopoliticalUnits:
    def test_no_units_given_is_valid(self, mock_cur, databus):
        response = nv.valid_geopolitical_units(
            mock_cur, {"metadata": []}, csv_file=[], databus=databus
        )

        assert response.validAll
        assert response.message == ["? No Geopolitical Units given."]
        assert mock_cur._execute_calls == []

    def test_national_unit_not_found_returns_early(self, mock_cur, databus):
        mock_cur.mock_fetchone = None
        yml, csv_file = _geopol(national_unit="Atlantis")
        response = nv.valid_geopolitical_units(mock_cur, yml, csv_file, databus=databus)

        assert not response.validAll
        assert any("National Unit Atlantis not found" in m for m in response.message)
        assert response.id_list == []
        assert _inserts(mock_cur) == []

    def test_national_unit_found_and_linked(self, mock_cur, databus):
        mock_cur.mock_fetchone = (300,)
        yml, csv_file = _geopol(national_unit="United States")
        response = nv.valid_geopolitical_units(mock_cur, yml, csv_file, databus=databus)

        assert response.validAll
        assert response.id_list == [300]
        assert _lookups(mock_cur)[0] == {"geopoliticalname": "united states"}
        assert _inserts(mock_cur) == [{"siteid": 5001, "geopolid": 300}]

    def test_subnational_lookup_uses_parent_id(self, mock_cur, databus):
        mock_cur.mock_fetchone = (300,)
        yml, csv_file = _geopol(national_unit="United States", subnational_unit_lv1="Iowa")
        nv.valid_geopolitical_units(mock_cur, yml, csv_file, databus=databus)

        state_lookup = _lookups(mock_cur)[1]
        assert state_lookup == {"geopoliticalname": "iowa", "highergeopoliticalid": 300}

    def test_all_levels_collected_in_id_list(self, mock_cur, databus):
        # Each successive fetchone stands in for the next level down.
        ids = iter([(300,), (301,), (302,)])
        mock_cur.fetchone = lambda: next(ids, None)
        yml, csv_file = _geopol(
            national_unit="United States",
            subnational_unit_lv1="Iowa",
            subnational_unit_lv2="Dickinson",
        )
        response = nv.valid_geopolitical_units(mock_cur, yml, csv_file, databus=databus)

        assert response.validAll
        assert response.id_list == [300, 301, 302]
        assert _inserts(mock_cur) == [
            {"siteid": 5001, "geopolid": 300},
            {"siteid": 5001, "geopolid": 301},
            {"siteid": 5001, "geopolid": 302},
        ]

    def test_unknown_subregion_stops_the_walk(self, mock_cur, databus):
        ids = iter([(300,), None])
        mock_cur.fetchone = lambda: next(ids, None)
        yml, csv_file = _geopol(
            national_unit="United States",
            subnational_unit_lv1="Nowhere",
            subnational_unit_lv2="Dickinson",
        )
        response = nv.valid_geopolitical_units(mock_cur, yml, csv_file, databus=databus)

        # The national unit still validated, and only it was linked.
        assert response.id_list == [300]
        assert any("Subregional Unit Nowhere not found" in m for m in response.message)
        assert _inserts(mock_cur) == [{"siteid": 5001, "geopolid": 300}]

    def test_missing_site_id_reports_link_failure(self, mock_cur):
        mock_cur.mock_fetchone = (300,)
        yml, csv_file = _geopol(national_unit="United States")
        response = nv.valid_geopolitical_units(mock_cur, yml, csv_file, databus={})

        assert not response.validAll
        assert any("Could not link national unit to site" in m for m in response.message)
