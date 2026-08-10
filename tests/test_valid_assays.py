"""Tests for the valid_assays / valid_aednalibraries validators.

Both read their fields from ``value:`` constants in the template rather than
from a sheet, so these tests build small yml_dicts by hand.  A file that
describes no assay must still validate — that is what lets non-aeDNA uploads
run through the same pipeline.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _constant(neotoma, value):
    return {"column": neotoma.split(".")[-1], "neotoma": neotoma, "value": value,
            "rowwise": False, "required": False, "type": "string"}


def _assay_yml(**overrides):
    """Template entries describing the Okoboji assay."""
    fields = {
        "ndb.aednaassays.assayname": "18SrRNAV7",
        "ndb.aednaassays.assaytypeid": "metabarcoding",
        "ndb.aednaassays.targetgene": "18S rRNA (SSU eukaryote)",
        "ndb.aednaassays.pcrprimerforward": "5'-GGCTTAATTTGACTCAACRCG-3'",
    }
    fields.update(overrides)
    return {"metadata": [_constant(k, v) for k, v in fields.items()]}


def _library_yml():
    return {"metadata": [
        _constant("ndb.aednalibraries.platform", "ILLUMINA"),
        _constant("ndb.aednalibraries.instrument", "Miseq"),
    ]}


@pytest.fixture
def databus():
    """Prior validation results supplying the parent ids."""
    datasets, assays = Response(), Response()
    datasets.id_int = 74655
    assays.id_int = 1
    return {"datasets": datasets, "assays": assays}


class TestValidAssays:
    def test_inserts_assay(self, mock_cur, databus):
        mock_cur.mock_fetchone = (7,)
        response = nv.valid_assays(mock_cur, _assay_yml(), csv_file=None, databus=databus)
        assert response.validAll
        assert response.id_int == 7
        assert "ndb.aednaassays" in mock_cur.last_query

    def test_assay_type_resolved_to_id(self, mock_cur, databus):
        mock_cur.mock_fetchone = (3,)
        nv.valid_assays(mock_cur, _assay_yml(), csv_file=None, databus=databus)
        assert mock_cur.last_params["assaytypeid"] == 3

    def test_dataset_id_taken_from_databus(self, mock_cur, databus):
        mock_cur.mock_fetchone = (7,)
        nv.valid_assays(mock_cur, _assay_yml(), csv_file=None, databus=databus)
        assert mock_cur.last_params["datasetid"] == 74655

    def test_unknown_assay_type_is_invalid(self, mock_cur, databus):
        mock_cur.mock_fetchone = None
        response = nv.valid_assays(mock_cur, _assay_yml(), csv_file=None, databus=databus)
        assert not response.validAll
        assert any("not known to Neotoma" in m for m in response.message)

    def test_no_assay_described_still_valid(self, mock_cur, databus):
        """No assay in the template means nothing to insert, not a failure."""
        response = nv.valid_assays(mock_cur, {"metadata": []}, csv_file=None, databus=databus)
        assert response.validAll
        assert mock_cur.last_query is None


class TestValidAednaLibraries:
    def test_inserts_library_linked_to_assay(self, mock_cur, databus):
        mock_cur.mock_fetchone = (2,)
        response = nv.valid_aednalibraries(
            mock_cur, _library_yml(), csv_file=None, databus=databus
        )
        assert response.validAll
        assert response.id_int == 2
        assert mock_cur.last_params["assayid"] == 1
        assert mock_cur.last_params["datasetid"] == 74655
        assert mock_cur.last_params["platform"] == "ILLUMINA"

    def test_no_library_described_still_valid(self, mock_cur, databus):
        response = nv.valid_aednalibraries(
            mock_cur, {"metadata": []}, csv_file=None, databus=databus
        )
        assert response.validAll
        assert mock_cur.last_query is None

    def test_missing_assay_id_is_invalid(self, mock_cur):
        datasets = Response()
        datasets.id_int = 74655
        response = nv.valid_aednalibraries(
            mock_cur, _library_yml(), csv_file=None, databus={"datasets": datasets}
        )
        assert not response.validAll


def _bool_constant(neotoma, value):
    entry = _constant(neotoma, value)
    entry["type"] = "bool"
    return entry


class TestFaireProjectTerms:
    """sterilise_method and the two control flags ride on the assay row.

    The FAIRe checklist records them once per project, and they are what the
    exporter reads back into projectMetadata D6/D7/D8 — so a value dropped here
    is a round-trip loss that surfaces as a blank cell, not as an error.
    """

    def test_sterilise_method_is_inserted(self, mock_cur, databus):
        mock_cur.mock_fetchone = (7,)
        yml = _assay_yml(**{
            "ndb.aednaassays.sterilise_method": "processed in UToledo clean lab facility",
        })
        nv.valid_assays(mock_cur, yml, csv_file=None, databus=databus)
        assert mock_cur.last_params["sterilise_method"] == (
            "processed in UToledo clean lab facility"
        )

    def test_control_flags_are_inserted_as_booleans(self, mock_cur, databus):
        mock_cur.mock_fetchone = (7,)
        yml = _assay_yml()
        yml["metadata"] += [
            _bool_constant("ndb.aednaassays.neg_cont", 1),
            _bool_constant("ndb.aednaassays.pos_cont", 0),
        ]
        nv.valid_assays(mock_cur, yml, csv_file=None, databus=databus)
        assert mock_cur.last_params["neg_cont"] is True
        assert mock_cur.last_params["pos_cont"] is False

    def test_unstated_flags_insert_null_not_false(self, mock_cur, databus):
        # The important case: a file that says nothing about controls must not
        # be recorded as "no controls were used".
        mock_cur.mock_fetchone = (7,)
        nv.valid_assays(mock_cur, _assay_yml(), csv_file=None, databus=databus)
        assert mock_cur.last_params["neg_cont"] is None
        assert mock_cur.last_params["pos_cont"] is None
        assert mock_cur.last_params["sterilise_method"] is None
