"""Tests for the valid_hiatus validator and its _find_clusters helper.

A hiatus is expressed in the CSV as a flag on the rows that bound the gap.
``valid_hiatus`` collects the flagged row indices, groups the consecutive ones
into clusters, then maps each cluster's first/last index onto the analysis unit
ids produced earlier in the pipeline.
"""

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response
from DataBUS.neotomaValidator.valid_hiatus import _find_clusters


def _hiatus_yml():
    return {
        "metadata": [
            {
                "neotoma": "ndb.hiatuses.hiatus",
                "column": "Hiatus",
                "rowwise": True,
                "required": False,
            }
        ]
    }


def _csv(flags):
    """One row per flag; 'NA' becomes None once pull_params has cleaned it."""
    return [{"Hiatus": f} for f in flags]


def _spans(cur):
    """The (start, end) AU pair of every insert_hiatus call the cursor saw.

    ``Hiatus.insert_to_db`` issues two statements per hiatus — the function DDL
    with no params, then the parameterised SELECT — so filter to the latter.
    """
    return [
        (p["analysisunitstart"], p["analysisunitend"])
        for _, p in cur._execute_calls
        if p and "analysisunitstart" in p
    ]


@pytest.fixture
def databus():
    """Ten analysis units, ids 100..109, one per CSV row."""
    aus = Response()
    aus.id_list = list(range(100, 110))
    return {"analysisunits": aus}


class TestFindClusters:
    def test_empty(self):
        assert _find_clusters([]) == []

    def test_single_run(self):
        assert _find_clusters([2, 3, 4]) == [[2, 3, 4]]

    def test_splits_on_gap(self):
        assert _find_clusters([0, 1, 5, 6, 9]) == [[0, 1], [5, 6], [9]]

    def test_sorts_before_grouping(self):
        assert _find_clusters([6, 1, 5, 0]) == [[0, 1], [5, 6]]

    def test_isolated_indices(self):
        assert _find_clusters([0, 2, 4]) == [[0], [2], [4]]


class TestValidHiatus:
    def test_no_hiatus_column_returns_early(self, mock_cur, databus):
        response = nv.valid_hiatus(mock_cur, {"metadata": []}, csv_file=[], databus=databus)

        assert response.validAll
        assert response.message == ["✔ No hiatuses found in the data."]
        assert mock_cur._execute_calls == []

    def test_all_rows_blank_produces_no_hiatus(self, mock_cur, databus):
        """A present-but-empty hiatus column is not the same as an absent one.

        The early return only fires when the column is missing entirely, so an
        all-blank column falls through to an empty cluster list: no inserts and
        no per-hiatus messages.
        """
        response = nv.valid_hiatus(
            mock_cur, _hiatus_yml(), csv_file=_csv(["NA", "NA", "NA"]), databus=databus
        )

        assert response.validAll
        assert response.message == []
        assert mock_cur._execute_calls == []

    def test_consecutive_flags_become_one_hiatus(self, mock_cur, databus):
        # Rows 2 and 3 flagged -> one hiatus spanning AU 102..103.
        mock_cur.mock_fetchone = (1,)
        response = nv.valid_hiatus(
            mock_cur,
            _hiatus_yml(),
            csv_file=_csv(["NA", "NA", "yes", "yes", "NA"]),
            databus=databus,
        )

        assert response.validAll
        assert "✔ Hiatus can be created." in response.message
        assert "✔ Hiatus inserted." in response.message
        assert mock_cur.last_params["analysisunitstart"] == 102
        assert mock_cur.last_params["analysisunitend"] == 103
        assert _spans(mock_cur) == [(102, 103)]

    def test_two_separate_gaps_insert_twice(self, mock_cur, databus):
        mock_cur.mock_fetchone = (1,)
        response = nv.valid_hiatus(
            mock_cur,
            _hiatus_yml(),
            csv_file=_csv(["yes", "NA", "NA", "yes", "yes"]),
            databus=databus,
        )

        assert response.validAll
        assert _spans(mock_cur) == [(100, 100), (103, 104)]

    def test_single_flagged_row_uses_same_au_both_ends(self, mock_cur, databus):
        mock_cur.mock_fetchone = (1,)
        nv.valid_hiatus(
            mock_cur, _hiatus_yml(), csv_file=_csv(["NA", "yes", "NA"]), databus=databus
        )

        assert mock_cur.last_params["analysisunitstart"] == 101
        assert mock_cur.last_params["analysisunitend"] == 101

    def test_missing_analysisunits_falls_back_to_indices(self, mock_cur):
        response = nv.valid_hiatus(
            mock_cur, _hiatus_yml(), csv_file=_csv(["NA", "yes", "yes"]), databus={}
        )

        assert not response.validAll
        assert any("Could not resolve analysis unit IDs" in m for m in response.message)
        # Fell back to the raw row indices 1 and 2.
        assert mock_cur.last_params["analysisunitstart"] == 1
        assert mock_cur.last_params["analysisunitend"] == 2

    def test_insert_failure_is_reported(self, mock_cur, databus):
        def boom(query, params=None):
            raise RuntimeError("no such table")

        mock_cur.execute = boom
        response = nv.valid_hiatus(
            mock_cur, _hiatus_yml(), csv_file=_csv(["yes", "yes"]), databus=databus
        )

        assert not response.validAll
        assert any("Could not insert hiatus" in m for m in response.message)
