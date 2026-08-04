"""Tests for the valid_project validator and its extraction helpers.

``valid_project`` walks the whole project hierarchy in one pass: it inserts (or
finds) the project, links it to the dataset, then works through grants,
funding institutions, awardees, participants, keywords and contact-institution
pairings. Every one of those steps is a different SQL statement, so these tests
use a routing cursor that answers each query by pattern.

Unlike most validators, valid_project reads ``value:`` constants straight off
the template via ``entry.get("value", entry.get("column"))`` — it never touches
the CSV — so the yml_dicts here carry the data directly.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response
from DataBUS.neotomaValidator.valid_project import (
    _extract_contact_institutions,
    _extract_grant_info,
    _extract_keywords,
    _extract_participants,
    _extract_project_info,
    _resolve_contact,
    _resolve_keyword,
)


class RoutingCursor:
    """A cursor whose fetchone() answer depends on the query it just saw.

    ``routes`` maps a distinctive substring of a query to the tuple that
    fetchone() should return for it. Anything unmatched returns None, which is
    how "not found" reads to the validator.
    """

    def __init__(self, routes=None):
        self.routes = routes or {}
        self.calls = []
        self._next = None

    def execute(self, query, params=None):
        self.calls.append((query, params))
        self._next = None
        for marker, value in self.routes.items():
            if marker in query:
                self._next = value
                break

    def fetchone(self):
        return self._next

    def fetchall(self):
        return []

    def queries_matching(self, marker):
        return [p for q, p in self.calls if marker in q]


def _entry(neotoma, value, **extra):
    entry = {"neotoma": neotoma, "value": value}
    entry.update(extra)
    return entry


def _project_yml(name="Okoboji aeDNA", **extra_entries):
    metadata = [
        _entry("ndb.projects.projectname", name),
        _entry("ndb.projects.projectdescription", "Lake sediment eDNA survey"),
    ]
    metadata.extend(extra_entries.get("metadata", []))
    return {"metadata": metadata}


@pytest.fixture
def databus():
    datasets = Response()
    datasets.id_int = 74655
    contacts = Response()
    contacts.id_int = 42
    return {"datasets": datasets, "contacts": contacts}


# Default happy-path routing: the project is new, everything else resolves.
BASE_ROUTES = {
    "SELECT projectid FROM ndb.projects": None,
    "INSERT INTO ndb.projects": (900,),
}


class TestExtractProjectInfo:
    def test_returns_none_without_project_entries(self):
        assert _extract_project_info({"metadata": []}) is None
        assert _extract_project_info({"metadata": [_entry("ndb.sites.sitename", "X")]}) is None

    def test_collects_fields_by_last_path_segment(self):
        info = _extract_project_info(_project_yml())
        assert info == {
            "projectname": "Okoboji aeDNA",
            "projectdescription": "Lake sediment eDNA survey",
        }

    def test_falls_back_to_column_when_no_value(self):
        yml = {"metadata": [{"neotoma": "ndb.projects.projectname", "column": "ProjName"}]}
        assert _extract_project_info(yml) == {"projectname": "ProjName"}


class TestExtractGrantInfo:
    def test_no_grants(self):
        assert _extract_grant_info({"metadata": []}) == []

    def test_single_grant(self):
        yml = {
            "metadata": [
                _entry("ndb.grants.grantname", "NSF BIO"),
                _entry("ndb.grants.grantnumber", "2055632"),
            ]
        }
        assert _extract_grant_info(yml) == [{"grantname": "NSF BIO", "grantnumber": "2055632"}]

    def test_nested_institution_and_awardee(self):
        yml = {
            "metadata": [
                _entry(
                    "ndb.grants.grantname",
                    "NSF BIO",
                    institutionid="https://ror.org/01y2jtd41",
                    institutionname="UW-Madison",
                    institutionlocation="Madison, WI",
                    awardee="Goring, Simon",
                ),
            ]
        }
        (grant,) = _extract_grant_info(yml)
        assert grant["institution"]["institutionid"] == "https://ror.org/01y2jtd41"
        assert grant["awardee"] == "Goring, Simon"

    def test_non_grant_entry_closes_the_current_group(self):
        yml = {
            "metadata": [
                _entry("ndb.grants.grantnumber", "111"),
                _entry("ndb.projects.projectname", "P"),
                _entry("ndb.grants.grantnumber", "222"),
            ]
        }
        assert _extract_grant_info(yml) == [{"grantnumber": "111"}, {"grantnumber": "222"}]


class TestExtractListFields:
    def test_participants_split_on_pipe(self):
        yml = {
            "metadata": [
                _entry("ndb.projectparticipants.contactname", "Goring, Simon | Williams, John")
            ]
        }
        assert _extract_participants(yml) == ["Goring, Simon", "Williams, John"]

    def test_participants_accept_a_list(self):
        yml = {"metadata": [_entry("ndb.projectparticipants.contactname", ["A", "B"])]}
        assert _extract_participants(yml) == ["A", "B"]

    def test_participants_absent(self):
        assert _extract_participants({"metadata": []}) == []

    def test_keywords_split_on_pipe_and_strip(self):
        yml = {"metadata": [_entry("ndb.projectkeywords.keyword", " eDNA | lake  |  ")]}
        assert _extract_keywords(yml) == ["eDNA", "lake"]

    def test_contact_institutions(self):
        yml = {
            "metadata": [
                _entry(
                    "ndb.contactinstitutions.contactname",
                    "Goring, Simon",
                    institutionid="https://ror.org/01y2jtd41",
                    institutionname="UW-Madison",
                )
            ]
        }
        (ci,) = _extract_contact_institutions(yml)
        assert ci["contactname"] == "Goring, Simon"
        assert ci["institutionname"] == "UW-Madison"


class TestResolveKeyword:
    def test_none_keyword(self):
        assert _resolve_keyword(RoutingCursor(), None) is None

    def test_found(self):
        cur = RoutingCursor({"ndb.keywords": (12,)})
        assert _resolve_keyword(cur, "  eDNA ") == 12
        assert cur.queries_matching("ndb.keywords") == [{"kw": "edna"}]

    def test_not_found(self):
        assert _resolve_keyword(RoutingCursor(), "nope") is None


class ContactCursor:
    """Answers get_contacts' exact-name lookup for a fixed set of known contacts.

    get_contacts issues the same SQL text no matter which name it is given, so
    routing here keys off the bound parameter instead of the query. Only the
    exact-contactname lookup is answered; the surname/givennames fallback query
    carries different params and so returns None, which reads as "not found".
    """

    def __init__(self, known=None):
        self.known = known or {}
        self.seen = []
        self._next = None

    def execute(self, query, params=None):
        self.seen.append(params)
        self._next = None
        if params and "contactname" in params:
            self._next = self.known.get(params["contactname"])

    def fetchone(self):
        return self._next

    @property
    def names_looked_up(self):
        return [p["contactname"] for p in self.seen if p and "contactname" in p]


# Neotoma stores contacts as "Surname, Given"; FAIRe templates carry recordedBy
# the other way round, so _resolve_contact has to bridge the two.
SPANBAUER = {"spanbauer, trisha": (10234, "Spanbauer, Trisha")}


class TestResolveContact:
    def test_empty_name_is_none(self):
        assert _resolve_contact(ContactCursor(), None) is None
        assert _resolve_contact(ContactCursor(), "") is None

    def test_canonical_surname_first_matches_directly(self):
        cur = ContactCursor(SPANBAUER)
        assert _resolve_contact(cur, "Spanbauer, Trisha") == 10234
        assert cur.names_looked_up == ["spanbauer, trisha"]

    def test_given_name_first_is_reordered_on_retry(self):
        cur = ContactCursor(SPANBAUER)
        assert _resolve_contact(cur, "Trisha L. Spanbauer") == 10234
        # First attempt uses the template's order and misses; the retry reorders
        # it into Neotoma's convention, dropping the middle initial.
        assert cur.names_looked_up == ["trisha l. spanbauer", "spanbauer, trisha"]

    def test_surrounding_whitespace_is_stripped(self):
        cur = ContactCursor(SPANBAUER)
        assert _resolve_contact(cur, "  Trisha L. Spanbauer  ") == 10234

    def test_name_with_a_comma_is_not_reordered(self):
        cur = ContactCursor()
        assert _resolve_contact(cur, "Nobody, Here") is None
        # A comma means the caller already used Neotoma's order, so there is
        # nothing to retry — exactly one lookup, not two.
        assert cur.names_looked_up == ["nobody, here"]

    def test_unknown_name_is_none(self):
        cur = ContactCursor()
        assert _resolve_contact(cur, "Trisha L. Spanbauer") is None


class TestValidProject:
    def test_no_project_entry_is_valid(self, databus):
        response = nv.valid_project(
            RoutingCursor(), {"metadata": []}, csv_file=None, databus=databus
        )

        assert response.validAll
        assert response.message == ["? No project entry found in template."]

    def test_missing_project_name_is_invalid(self, databus):
        yml = {"metadata": [_entry("ndb.projects.projectdescription", "no name here")]}
        response = nv.valid_project(RoutingCursor(), yml, csv_file=None, databus=databus)

        assert not response.validAll
        assert any("Project name is required" in m for m in response.message)

    def test_new_project_is_inserted_and_linked(self, databus):
        cur = RoutingCursor(dict(BASE_ROUTES))
        response = nv.valid_project(cur, _project_yml(), csv_file=None, databus=databus)

        assert response.validAll
        assert response.id_int == 900
        assert any("inserted (ID: 900)" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.projectdatasets") == [
            {"projectid": 900, "datasetid": 74655}
        ]

    def test_existing_project_is_reused(self, databus):
        cur = RoutingCursor({"SELECT projectid FROM ndb.projects": (777,)})
        response = nv.valid_project(cur, _project_yml(), csv_file=None, databus=databus)

        assert response.id_int == 777
        assert any("already exists (ID: 777)" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.projects") == []

    def test_project_lookup_is_lowercased(self, databus):
        cur = RoutingCursor(dict(BASE_ROUTES))
        nv.valid_project(cur, _project_yml("  Okoboji aeDNA "), csv_file=None, databus=databus)

        assert cur.queries_matching("SELECT projectid FROM ndb.projects") == [
            {"name": "okoboji aedna"}
        ]

    def test_missing_dataset_does_not_abort(self, databus):
        cur = RoutingCursor(dict(BASE_ROUTES))
        response = nv.valid_project(cur, _project_yml(), csv_file=None, databus={})

        # Project still inserted; only the link is reported as a soft failure.
        assert response.id_int == 900
        assert any("Could not link project to dataset" in m for m in response.message)

    def test_grant_inserted_and_linked(self, databus):
        yml = _project_yml()
        yml["metadata"].append(_entry("ndb.grants.grantnumber", "2055632"))
        routes = dict(BASE_ROUTES)
        routes["SELECT grantid FROM ndb.grants"] = None
        routes["INSERT INTO ndb.grants"] = (55,)
        cur = RoutingCursor(routes)

        response = nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert any("Grant inserted (ID: 55)" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.projectgrants") == [
            {"projectid": 900, "grantid": 55}
        ]

    def test_existing_grant_is_reused(self, databus):
        yml = _project_yml()
        yml["metadata"].append(_entry("ndb.grants.grantnumber", "2055632"))
        routes = dict(BASE_ROUTES)
        routes["SELECT grantid FROM ndb.grants"] = (55,)
        cur = RoutingCursor(routes)

        response = nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert any("already exists (ID: 55)" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.grants") == []

    def test_funding_institution_linked_to_grant(self, databus):
        yml = _project_yml()
        yml["metadata"].append(
            _entry(
                "ndb.grants.grantnumber",
                "2055632",
                institutionid="https://ror.org/01y2jtd41",
                institutionname="UW-Madison",
            )
        )
        routes = dict(BASE_ROUTES)
        routes["SELECT grantid FROM ndb.grants"] = None
        routes["INSERT INTO ndb.grants"] = (55,)
        routes["INSERT INTO ndb.institutions"] = ("https://ror.org/01y2jtd41",)
        cur = RoutingCursor(routes)

        nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert cur.queries_matching("INSERT INTO ndb.fundinginstitutions") == [
            {"institutionid": "https://ror.org/01y2jtd41", "grantid": 55}
        ]

    def test_participant_resolved_and_linked(self, databus):
        yml = _project_yml()
        yml["metadata"].append(_entry("ndb.projectparticipants.contactname", "Goring, Simon"))
        routes = dict(BASE_ROUTES)
        routes["FROM ndb.contacts"] = (42, "Goring, Simon")
        cur = RoutingCursor(routes)

        response = nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert response.validAll
        assert cur.queries_matching("INSERT INTO ndb.projectparticipants") == [
            {"projectid": 900, "contactid": 42}
        ]

    def test_unknown_participant_is_reported_not_fatal(self, databus):
        yml = _project_yml()
        yml["metadata"].append(_entry("ndb.projectparticipants.contactname", "Nobody, Real"))
        cur = RoutingCursor(dict(BASE_ROUTES))

        response = nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert response.validAll
        assert any("Participant 'Nobody, Real' not found" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.projectparticipants") == []

    def test_keyword_resolved_and_linked(self, databus):
        yml = _project_yml()
        yml["metadata"].append(_entry("ndb.projectkeywords.keyword", "eDNA | lake"))
        routes = dict(BASE_ROUTES)
        routes["ndb.keywords"] = (12,)
        cur = RoutingCursor(routes)

        nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert cur.queries_matching("INSERT INTO ndb.projectkeywords") == [
            {"projectid": 900, "keywordid": 12},
            {"projectid": 900, "keywordid": 12},
        ]

    def test_unknown_keyword_is_reported(self, databus):
        yml = _project_yml()
        yml["metadata"].append(_entry("ndb.projectkeywords.keyword", "nonsense"))
        cur = RoutingCursor(dict(BASE_ROUTES))

        response = nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert any("Keyword 'nonsense' not found" in m for m in response.message)

    def test_contact_institution_linked(self, databus):
        yml = _project_yml()
        yml["metadata"].append(
            _entry(
                "ndb.contactinstitutions.contactname",
                "Goring, Simon",
                institutionid="https://ror.org/01y2jtd41",
                institutionname="UW-Madison",
            )
        )
        routes = dict(BASE_ROUTES)
        routes["FROM ndb.contacts"] = (42, "Goring, Simon")
        routes["INSERT INTO ndb.institutions"] = ("https://ror.org/01y2jtd41",)
        cur = RoutingCursor(routes)

        nv.valid_project(cur, yml, csv_file=None, databus=databus)

        assert cur.queries_matching("INSERT INTO ndb.contactinstitutions") == [
            {"institutionid": "https://ror.org/01y2jtd41", "contactid": 42}
        ]

    def test_project_insert_failure_returns_early(self, databus):
        class Failing(RoutingCursor):
            def execute(self, query, params=None):
                super().execute(query, params)
                if "INSERT INTO ndb.projects" in query:
                    raise RuntimeError("permission denied")

        cur = Failing(dict(BASE_ROUTES))
        response = nv.valid_project(cur, _project_yml(), csv_file=None, databus=databus)

        assert not response.validAll
        assert any("Cannot insert project" in m for m in response.message)
        assert response.id_int is None
