"""Tests for valid_speleothem and valid_external_speleothem.

``valid_speleothem`` resolves every ``*id`` field whose CSV value is a name
(drip type, entity status, rock type, …) against its own vocabulary table
before building the Speleothem entity. ``valid_external_speleothem`` links that
entity to a record in an outside database such as SISAL.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _entry(table, field, column, type_spec="string"):
    return {
        "neotoma": f"ndb.{table}.{field}",
        "column": column,
        "rowwise": False,
        "required": False,
        "type": type_spec,
    }


def _speleothem(**overrides):
    """(yml_dict, csv_file) for a speleothem entity.

    ``Speleothem`` refuses to build without a siteid and a speleothemtypeid, so
    the type is always present unless a test explicitly drops it by passing
    ``speleothemtypeid=None``.
    """
    fields = {"entityname": "Cave-1", "speleothemtypeid": "stalagmite"}
    fields.update(overrides)
    fields = {k: v for k, v in fields.items() if v is not None}
    yml = {"metadata": [_entry("speleothems", k, k) for k in fields]}
    return yml, [dict(fields)]


def _fetches(cur, *values):
    """Return each value in turn from cur.fetchone(), then None."""
    it = iter(values)
    cur.fetchone = lambda: next(it, None)


def _external(**fields):
    yml = {"metadata": [_entry("externalspeleothemdata", k, k) for k in fields]}
    return yml, [dict(fields)]


@pytest.fixture
def site_databus():
    sites = Response()
    sites.id_int = 5001
    return {"sites": sites}


@pytest.fixture
def speleothem_databus():
    speleothems = Response()
    speleothems.id_int = 88
    return {"speleothems": speleothems}


def _params(cur, marker):
    return [p for q, p in cur._execute_calls if p and marker in q]


class TestValidSpeleothem:
    def test_no_parameters_is_valid(self, mock_cur, site_databus):
        response = nv.valid_speleothem(
            mock_cur, {"metadata": []}, csv_file=[], databus=site_databus
        )

        assert response.validAll
        assert response.message == ["✔ No speleothem parameters provided."]
        assert mock_cur._execute_calls == []

    def test_entity_inserted_against_site(self, mock_cur, site_databus):
        _fetches(mock_cur, (7,), (88,))
        yml, csv_file = _speleothem(monitoring="yes")
        response = nv.valid_speleothem(mock_cur, yml, csv_file, databus=site_databus)

        assert response.validAll
        assert response.id_int == 88
        assert "✔ Speleothem can be created." in response.message
        (insert,) = _params(mock_cur, "insert_speleothem(")
        assert insert["siteid"] == 5001
        assert insert["entityname"] == "Cave-1"

    def test_monitoring_yes_becomes_true(self, mock_cur, site_databus):
        _fetches(mock_cur, (7,), (88,))
        yml, csv_file = _speleothem(monitoring="yes")
        nv.valid_speleothem(mock_cur, yml, csv_file, databus=site_databus)

        assert _params(mock_cur, "insert_speleothem(")[0]["monitoring"] is True

    def test_monitoring_anything_else_becomes_false(self, mock_cur, site_databus):
        _fetches(mock_cur, (7,), (88,))
        yml, csv_file = _speleothem(monitoring="no")
        nv.valid_speleothem(mock_cur, yml, csv_file, databus=site_databus)

        assert _params(mock_cur, "insert_speleothem(")[0]["monitoring"] is False

    def test_blank_monitoring_becomes_false(self, mock_cur, site_databus):
        """An unfilled monitoring column must not crash the validator."""
        _fetches(mock_cur, (7,), (88,))
        yml, csv_file = _speleothem()
        response = nv.valid_speleothem(mock_cur, yml, csv_file, databus=site_databus)

        assert response.validAll
        assert _params(mock_cur, "insert_speleothem(")[0]["monitoring"] is False

    def test_named_vocabulary_field_is_resolved(self, mock_cur, site_databus):
        _fetches(mock_cur, (7,), (88,))
        yml, csv_file = _speleothem(speleothemtypeid="stalagmite")
        response = nv.valid_speleothem(mock_cur, yml, csv_file, databus=site_databus)

        assert _params(mock_cur, "ndb.speleothemtypes") == [{"element": "stalagmite"}]
        assert _params(mock_cur, "insert_speleothem(")[0]["speleothemtypeid"] == 7
        assert any("speleothemtypeid for 7 found" in m for m in response.message)

    def test_unknown_vocabulary_value_is_invalid(self, mock_cur, site_databus):
        mock_cur.mock_fetchone = None
        yml, csv_file = _speleothem(speleothemtypeid="not a type")
        response = nv.valid_speleothem(mock_cur, yml, csv_file, databus=site_databus)

        assert not response.validAll
        assert any("speleothemtypeid for not a type not found" in m for m in response.message)

    def test_missing_site_uses_placeholder(self, mock_cur):
        _fetches(mock_cur, (7,), (88,))
        yml, csv_file = _speleothem()
        response = nv.valid_speleothem(mock_cur, yml, csv_file, databus={})

        assert not response.validAll
        assert any("Site ID not available" in m for m in response.message)
        assert _params(mock_cur, "insert_speleothem(")[0]["siteid"] == 1


class TestValidExternalSpeleothem:
    def test_no_parameters_is_valid(self, mock_cur, speleothem_databus):
        response = nv.valid_external_speleothem(
            mock_cur, {"metadata": []}, csv_file=[], databus=speleothem_databus
        )

        assert response.validAll
        assert any("No external speleothem parameters" in m for m in response.message)
        assert mock_cur._execute_calls == []

    def test_links_entity_to_external_record(self, mock_cur, speleothem_databus):
        mock_cur.mock_fetchone = (3,)
        yml, csv_file = _external(extdatabaseid="SISAL", externalid="entity_42")
        response = nv.valid_external_speleothem(mock_cur, yml, csv_file, databus=speleothem_databus)

        assert response.validAll
        assert "✔  ExternalSpeleothem inserted." in response.message
        (insert,) = _params(mock_cur, "insert_externalspeleothem(")
        assert insert["entityid"] == 88
        assert insert["extdatabaseid"] == 3
        assert insert["externalid"] == "entity_42"

    def test_database_name_lookup_is_lowercased(self, mock_cur, speleothem_databus):
        mock_cur.mock_fetchone = (3,)
        yml, csv_file = _external(extdatabaseid="SISAL", externalid="entity_42")
        nv.valid_external_speleothem(mock_cur, yml, csv_file, databus=speleothem_databus)

        assert _params(mock_cur, "ndb.externaldatabases") == [{"extdatabaseid": "sisal"}]

    def test_unknown_external_database_is_invalid(self, mock_cur, speleothem_databus):
        mock_cur.mock_fetchone = None
        yml, csv_file = _external(extdatabaseid="Nowhere", externalid="entity_42")
        response = nv.valid_external_speleothem(mock_cur, yml, csv_file, databus=speleothem_databus)

        assert not response.validAll
        assert any("extdatabaseid for Nowhere not found" in m for m in response.message)

    def test_description_is_stripped(self, mock_cur, speleothem_databus):
        mock_cur.mock_fetchone = (3,)
        yml, csv_file = _external(
            extdatabaseid="SISAL", externalid="entity_42", externaldescription="  a cave, "
        )
        nv.valid_external_speleothem(mock_cur, yml, csv_file, databus=speleothem_databus)

        assert _params(mock_cur, "insert_externalspeleothem(")[0]["externaldescription"] == "a cave"

    def test_missing_speleothem_uses_placeholder_entity(self, mock_cur):
        mock_cur.mock_fetchone = (3,)
        yml, csv_file = _external(extdatabaseid="SISAL", externalid="entity_42")
        response = nv.valid_external_speleothem(mock_cur, yml, csv_file, databus={})

        assert not response.validAll
        assert any("Speleothem entity ID not available" in m for m in response.message)
        assert _params(mock_cur, "insert_externalspeleothem(")[0]["entityid"] == 2
