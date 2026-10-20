"""Tests for valid_sample_age validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestChronologyScalarBroadcast:
    """A chronology-wide scalar must not truncate the per-sample age rows.

    ``agemodel`` is pulled once for the whole chronology while ages are pulled
    per sample. Wrapping the scalar in a one-element list made the zip stop after
    a single row, so a 42-sample dataset landed exactly one sample age.
    """

    AGES = ["100", "200", "300"]

    @pytest.fixture
    def crs_pair(self):
        yml_dict = {
            "metadata": [
                {
                    "column": "Age",
                    "neotoma": "ndb.sampleages.age",
                    "rowwise": True,
                    "type": "float",
                    "chronologyname": "DefaultChronology",
                },
                {
                    "column": "AgeModel",
                    "neotoma": "ndb.sampleages.agemodel",
                    "rowwise": False,
                    "type": "string",
                    "chronologyname": "DefaultChronology",
                },
            ]
        }
        csv_file = [{"Age": a, "AgeModel": "CRS"} for a in self.AGES]
        return csv_file, yml_dict

    def test_every_sample_gets_an_age(self, mock_cur, crs_pair):
        csv_file, yml_dict = crs_pair
        mock_cur.mock_fetchone = (1,)
        # `name` is a reserved MagicMock constructor kwarg, so it has to be
        # assigned after construction to become a real attribute.
        chronologies = MagicMock()
        chronologies.name = {"DefaultChronology": 5}
        databus = {
            "chronologies": chronologies,
            "samples": MagicMock(id_list=[11, 12, 13]),
        }
        nv.valid_sample_age(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)

        inserts = [
            params for q, params in mock_cur._execute_calls if "insertsampleage" in q.lower()
        ]
        assert len(inserts) == len(self.AGES)
        assert {p["sampleid"] for p in inserts} == {11, 12, 13}
        assert {p["age"] for p in inserts} == {100.0, 200.0, 300.0}

    def test_unknown_chronology_is_named(self, mock_cur, crs_pair):
        csv_file, yml_dict = crs_pair
        chronologies = MagicMock()
        chronologies.name = {"SomeOtherChronology": 5}
        databus = {
            "chronologies": chronologies,
            "samples": MagicMock(id_list=[11, 12, 13]),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )

        assert not result.validAll
        assert any(
            "DefaultChronology" in m and "SomeOtherChronology" in m for m in result.message
        )
        assert not [q for q, _ in mock_cur._execute_calls if "insertsampleage" in q.lower()]

    def test_stops_after_first_database_failure(self, crs_pair):
        csv_file, yml_dict = crs_pair
        cur = MagicMock()
        cur.execute.side_effect = RuntimeError("null value in column sampleid")
        chronologies = MagicMock()
        chronologies.name = {"DefaultChronology": 5}
        databus = {
            "chronologies": chronologies,
            "samples": MagicMock(id_list=[11, 12, 13]),
        }
        result = nv.valid_sample_age(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)

        assert cur.execute.call_count == 1
        assert not result.validAll
        failure = next(m for m in result.message if "could not be inserted" in m)
        assert "sample ID 11" in failure
        assert "'DefaultChronology'" in failure
        assert "null value in column sampleid" in failure


class TestValidSampleAgeMock:
    def test_returns_response_no_sample_ages(self, mock_cur):
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus={}
        )
        assert isinstance(result, Response)
        assert True in result.valid

    def test_no_chronologies_in_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        databus = {
            "samples": MagicMock(id_list=list(range(1, n + 1))),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_no_samples_returns_early(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        databus = {
            "chronologies": MagicMock(name={"default": 1}),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_with_full_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (1,)
        databus = {
            "chronologies": MagicMock(name={"default": 1, "CRS": 2}),
            "samples": MagicMock(id_list=list(range(1, n + 1))),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_sisal_pair(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (1,)
        databus = {
            "chronologies": MagicMock(name={"default": 1}),
            "samples": MagicMock(id_list=list(range(1, n + 1))),
        }
        result = nv.valid_sample_age(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_empty_databus(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        result = nv.valid_sample_age(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus={})
        assert isinstance(result, Response)
