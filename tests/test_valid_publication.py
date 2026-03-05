"""Tests for valid_publication validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


def _pub_yml(doi=None, citation=None, pubid=None):
    meta = []
    if doi:
        meta.append(
            {"neotoma": "ndb.publications.doi", "column": "DOI", "value": doi, "rowwise": False}
        )
    if citation:
        meta.append(
            {
                "neotoma": "ndb.publications.citation",
                "column": "Citation",
                "value": citation,
                "rowwise": False,
            }
        )
    if pubid:
        meta.append(
            {
                "neotoma": "ndb.publications.publicationid",
                "column": "PubID",
                "value": pubid,
                "rowwise": False,
            }
        )
    return {"metadata": meta}


class TestValidPublicationMock:
    def test_no_pub_info_returns_valid(self, mock_cur):
        result = nv.valid_publication(
            cur=mock_cur, yml_dict={"metadata": []}, csv_file=[], databus={}
        )
        assert isinstance(result, Response)
        assert True in result.valid
        assert any("No publication" in m for m in result.message)

    def test_publicationid_found(self, mock_cur):
        mock_cur.mock_fetchone = ("Smith 2001",)
        databus = {"datasets": MagicMock(id_int=5)}
        result = nv.valid_publication(
            cur=mock_cur, yml_dict=_pub_yml(pubid=42), csv_file=[], databus=databus
        )
        assert isinstance(result, Response)
        assert True in result.valid

    def test_publicationid_not_found(self, mock_cur):
        mock_cur.mock_fetchone = None
        databus = {"datasets": MagicMock(id_int=5)}
        # csv_file must contain the column referenced in _pub_yml so pull_params
        # can extract the value; with an empty csv it returns None and the
        # function exits early with True ("no publication info").
        result = nv.valid_publication(
            cur=mock_cur,
            yml_dict=_pub_yml(pubid=9999),
            csv_file=[{"PubID": "9999"}],
            databus=databus,
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_doi_found_in_db(self, mock_cur):
        mock_cur.mock_fetchone = (7,)
        databus = {"datasets": MagicMock(id_int=5)}
        result = nv.valid_publication(
            cur=mock_cur,
            yml_dict=_pub_yml(doi="10.1234/test", citation="Smith 2001"),
            csv_file=[],
            databus=databus,
        )
        assert isinstance(result, Response)
        assert True in result.valid

    def test_doi_not_found_falls_back_to_citation(self, mock_cur):
        # First call (doi lookup) returns None; second (citation) returns id
        responses = [None, (8,), (8,)]
        idx = [0]

        def side_effect():
            val = responses[idx[0]]
            idx[0] = min(idx[0] + 1, len(responses) - 1)
            return val

        mock_cur.fetchone = side_effect
        databus = {"datasets": MagicMock(id_int=3)}
        result = nv.valid_publication(
            cur=mock_cur,
            yml_dict=_pub_yml(doi="10.9999/missing", citation="Jones 1999"),
            csv_file=[],
            databus=databus,
        )
        assert isinstance(result, Response)

    def test_citation_not_found(self, mock_cur):
        mock_cur.mock_fetchone = None
        databus = {"datasets": MagicMock(id_int=3)}
        # csv_file must contain the "Citation" column so pull_params extracts
        # the value; an empty csv returns None and the function exits early with True.
        result = nv.valid_publication(
            cur=mock_cur,
            yml_dict=_pub_yml(citation="Unknown Author 1800"),
            csv_file=[{"Citation": "Unknown Author 1800"}],
            databus=databus,
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_no_dataset_id_uses_placeholder(self, mock_cur):
        mock_cur.mock_fetchone = (5,)
        databus = {}
        result = nv.valid_publication(
            cur=mock_cur, yml_dict=_pub_yml(pubid=1), csv_file=[], databus=databus
        )
        assert isinstance(result, Response)

    def test_sisal_pair_returns_response(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        mock_cur.mock_fetchone = (1,)
        databus = {"datasets": MagicMock(id_int=1)}
        result = nv.valid_publication(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
