"""Tests for the valid_sequence validator and its extraction helpers.

``valid_sequence`` runs after ``valid_data``. For every DNA entry in the
template it inserts one ``ndb.sequences`` row, one ``ndb.sequencedata`` row per
dataid the taxon produced, and one ``ndb.aednamodels`` row carrying the taxon
call. The tests drive it with a routing cursor because each of those steps is a
different statement.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response
from DataBUS.neotomaValidator.valid_sequence import (
    _build_key_to_dna_map,
    _extract_dna_entries,
    _extract_model_info,
)


class RoutingCursor:
    """Answers fetchone() based on a substring match against the last query."""

    def __init__(self, routes=None, rowcount=0):
        self.routes = routes or {}
        self.calls = []
        self.rowcount = rowcount
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


def _dna_entry(taxonname, dnasequence, asv, column=None):
    return {
        "neotoma": "ndb.data.value",
        "taxonname": taxonname,
        "dnasequence": dnasequence,
        "asv": asv,
        "column": column or asv,
    }


def _seq_yml(entries=None, model="DADA2", superseeds=None):
    metadata = list(entries if entries is not None else [_dna_entry("Picea", "ACGTACGT", "ASV1")])
    if model is not None:
        entry = {"neotoma": "ndb.aednamodels.modelid", "value": model}
        if superseeds is not None:
            entry["superseeds"] = superseeds
        metadata.append(entry)
    return {"metadata": metadata}


def _databus(id_dict, datasetid=74655):
    datasets, data = Response(), Response()
    datasets.id_int = datasetid
    data.id_dict = id_dict
    return {"datasets": datasets, "data": data}


# Sequence insert -> 500, model insert -> 900, taxonid lookup -> 42.
BASE_ROUTES = {
    "INSERT INTO ndb.sequences": (500,),
    "INSERT INTO ndb.aednamodels": (900,),
    "SELECT v.taxonid": (42,),
}


class TestExtractDnaEntries:
    def test_no_entries(self):
        assert _extract_dna_entries({"metadata": []}) == []

    def test_ignores_entries_without_both_dnasequence_and_asv(self):
        yml = {
            "metadata": [
                {"neotoma": "ndb.data.value", "taxonname": "Picea", "asv": "ASV1"},
                {"neotoma": "ndb.data.value", "taxonname": "Picea", "dnasequence": "ACGT"},
                {"neotoma": "ndb.sites.sitename", "dnasequence": "ACGT", "asv": "ASV1"},
            ]
        }
        assert _extract_dna_entries(yml) == []

    def test_collects_complete_entries(self):
        yml = {"metadata": [_dna_entry("Picea", "ACGTACGT", "ASV1", column="col_a")]}
        assert _extract_dna_entries(yml) == [
            {
                "taxonname": "Picea",
                "dnasequence": "ACGTACGT",
                "asv": "ASV1",
                "column": "col_a",
            }
        ]


class TestExtractModelInfo:
    def test_absent_model(self):
        assert _extract_model_info({"metadata": []}) == (None, [])

    def test_model_without_superseeds(self):
        yml = {"metadata": [{"neotoma": "ndb.aednamodels.modelid", "value": "DADA2"}]}
        assert _extract_model_info(yml) == ("DADA2", [])

    def test_superseeds_string_is_split_and_stripped(self):
        yml = {
            "metadata": [
                {
                    "neotoma": "ndb.aednamodels.modelid",
                    "value": "DADA2",
                    "superseeds": "OBITools, mothur ",
                }
            ]
        }
        assert _extract_model_info(yml) == ("DADA2", ["OBITools", "mothur"])

    def test_superseeds_list_is_kept(self):
        yml = {
            "metadata": [
                {
                    "neotoma": "ndb.aednamodels.modelid",
                    "value": "DADA2",
                    "superseeds": ["OBITools"],
                }
            ]
        }
        assert _extract_model_info(yml) == ("DADA2", ["OBITools"])


class TestBuildKeyToDnaMap:
    def test_prefers_compound_key(self):
        entries = _extract_dna_entries(_seq_yml())
        mapping = _build_key_to_dna_map(entries, {"Picea::ASV1": [1], "Picea": [2]})
        assert list(mapping) == ["Picea::ASV1"]

    def test_falls_back_to_plain_taxon_key(self):
        entries = _extract_dna_entries(_seq_yml())
        mapping = _build_key_to_dna_map(entries, {"Picea": [2]})
        assert mapping == {"Picea": {"dnasequence": "ACGTACGT", "asv": "ASV1"}}

    def test_unmatched_entry_is_dropped(self):
        entries = _extract_dna_entries(_seq_yml())
        assert _build_key_to_dna_map(entries, {"Abies": [1]}) == {}

    def test_two_asvs_for_one_taxon_keep_separate_compound_keys(self):
        yml = _seq_yml(
            entries=[
                _dna_entry("Picea", "ACGT", "ASV1"),
                _dna_entry("Picea", "TGCA", "ASV2"),
            ]
        )
        entries = _extract_dna_entries(yml)
        mapping = _build_key_to_dna_map(entries, {"Picea::ASV1": [1], "Picea::ASV2": [2]})
        assert mapping["Picea::ASV1"]["dnasequence"] == "ACGT"
        assert mapping["Picea::ASV2"]["dnasequence"] == "TGCA"

    def test_plain_key_taken_only_once(self):
        """With no compound keys, the first entry wins and the second is dropped."""
        yml = _seq_yml(
            entries=[
                _dna_entry("Picea", "ACGT", "ASV1"),
                _dna_entry("Picea", "TGCA", "ASV2"),
            ]
        )
        entries = _extract_dna_entries(yml)
        mapping = _build_key_to_dna_map(entries, {"Picea": [1]})
        assert mapping == {"Picea": {"dnasequence": "ACGT", "asv": "ASV1"}}


class TestValidSequence:
    def test_no_dna_entries_is_valid(self):
        cur = RoutingCursor()
        response = nv.valid_sequence(
            cur, {"metadata": []}, csv_file=None, databus=_databus({"Picea": [1]})
        )

        assert response.validAll
        assert response.message == ["? No aeDNA sequence entries found in template."]
        assert cur.calls == []

    def test_missing_dataset_id_aborts(self):
        response = nv.valid_sequence(RoutingCursor(), _seq_yml(), csv_file=None, databus={})

        assert not response.validAll
        assert any("Dataset ID not available" in m for m in response.message)

    def test_missing_data_ids_aborts(self):
        datasets = Response()
        datasets.id_int = 74655
        response = nv.valid_sequence(
            RoutingCursor(), _seq_yml(), csv_file=None, databus={"datasets": datasets}
        )

        assert not response.validAll
        assert any("Data IDs not available" in m for m in response.message)

    def test_no_matching_data_entries_is_valid(self):
        response = nv.valid_sequence(
            RoutingCursor(), _seq_yml(), csv_file=None, databus=_databus({"Abies": [1]})
        )

        assert response.validAll
        assert any("Could not match any DNA entries" in m for m in response.message)

    def test_full_insert_chain(self):
        cur = RoutingCursor(dict(BASE_ROUTES))
        response = nv.valid_sequence(
            cur, _seq_yml(), csv_file=None, databus=_databus({"Picea::ASV1": [11, 12]})
        )

        assert response.validAll
        assert response.id_dict == {"Picea::ASV1": {"sequenceid": 500, "modelid": 900}}
        assert cur.queries_matching("INSERT INTO ndb.sequences") == [
            {"datasetid": 74655, "sequence": "ACGTACGT", "asv": "ASV1", "primername": None}
        ]

    def test_one_sequencedata_row_per_dataid(self):
        cur = RoutingCursor(dict(BASE_ROUTES))
        nv.valid_sequence(
            cur, _seq_yml(), csv_file=None, databus=_databus({"Picea::ASV1": [11, 12, 13]})
        )

        assert cur.queries_matching("INSERT INTO ndb.sequencedata") == [
            {"dataid": 11, "sequenceid": 500},
            {"dataid": 12, "sequenceid": 500},
            {"dataid": 13, "sequenceid": 500},
        ]

    def test_model_row_carries_taxonid_from_first_dataid(self):
        cur = RoutingCursor(dict(BASE_ROUTES))
        nv.valid_sequence(
            cur, _seq_yml(), csv_file=None, databus=_databus({"Picea::ASV1": [11, 12]})
        )

        assert cur.queries_matching("SELECT v.taxonid") == [{"dataid": 11}]
        (model,) = cur.queries_matching("INSERT INTO ndb.aednamodels")
        assert model["sequenceid"] == 500
        assert model["taxonid"] == 42
        assert model["model"] == "DADA2"

    def test_no_model_entry_skips_aednamodels(self):
        cur = RoutingCursor(dict(BASE_ROUTES))
        response = nv.valid_sequence(
            cur,
            _seq_yml(model=None),
            csv_file=None,
            databus=_databus({"Picea::ASV1": [11]}),
        )

        assert any("No aeDNA model entry" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.aednamodels") == []
        assert response.id_dict == {"Picea::ASV1": {"sequenceid": 500, "modelid": None}}

    def test_missing_taxonid_leaves_model_none(self):
        routes = dict(BASE_ROUTES)
        routes["SELECT v.taxonid"] = None
        cur = RoutingCursor(routes)
        response = nv.valid_sequence(
            cur, _seq_yml(), csv_file=None, databus=_databus({"Picea::ASV1": [11]})
        )

        assert not response.validAll
        assert any("Could not retrieve taxonid for dataid 11" in m for m in response.message)
        assert response.id_dict == {"Picea::ASV1": {"sequenceid": 500, "modelid": None}}

    def test_superseded_models_are_updated(self):
        cur = RoutingCursor(dict(BASE_ROUTES), rowcount=2)
        response = nv.valid_sequence(
            cur,
            _seq_yml(superseeds="OBITools"),
            csv_file=None,
            databus=_databus({"Picea::ASV1": [11]}),
        )

        updates = cur.queries_matching("UPDATE ndb.aednamodels")
        assert updates == [{"new_modelid": 900, "taxonid": 42, "old_model": "OBITools"}]
        assert any("2 previous model(s) marked as superseded" in m for m in response.message)

    def test_empty_dataid_list_is_skipped(self):
        cur = RoutingCursor(dict(BASE_ROUTES))
        response = nv.valid_sequence(
            cur, _seq_yml(), csv_file=None, databus=_databus({"Picea::ASV1": []})
        )

        assert response.validAll
        assert any("No DNA data entries matched" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.sequences") == []

    def test_sequence_insert_failure_skips_the_entry(self):
        class Failing(RoutingCursor):
            def execute(self, query, params=None):
                super().execute(query, params)
                if "INSERT INTO ndb.sequences" in query:
                    raise RuntimeError("duplicate key")

        cur = Failing(dict(BASE_ROUTES))
        response = nv.valid_sequence(
            cur, _seq_yml(), csv_file=None, databus=_databus({"Picea::ASV1": [11]})
        )

        assert not response.validAll
        assert any("Sequence cannot be inserted" in m for m in response.message)
        assert cur.queries_matching("INSERT INTO ndb.sequencedata") == []
        assert response.id_dict == {}

    def test_two_taxa_each_get_their_own_sequence(self):
        yml = _seq_yml(
            entries=[
                _dna_entry("Picea", "ACGT", "ASV1"),
                _dna_entry("Abies", "TGCA", "ASV2"),
            ]
        )
        cur = RoutingCursor(dict(BASE_ROUTES))
        response = nv.valid_sequence(
            cur,
            yml,
            csv_file=None,
            databus=_databus({"Picea::ASV1": [11], "Abies::ASV2": [22]}),
        )

        assert response.validAll
        assert set(response.id_dict) == {"Picea::ASV1", "Abies::ASV2"}
        sequences = cur.queries_matching("INSERT INTO ndb.sequences")
        assert [s["asv"] for s in sequences] == ["ASV1", "ASV2"]
