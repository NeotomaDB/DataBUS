"""Tests for valid_analysisunit validator.

Key invariant: the number of AnalysisUnits created must equal the number of
depth/age values reported in the CSV (one AU per sample row).
"""

import pytest

import DataBUS.neotomaHelpers as nh
import DataBUS.neotomaValidator as nv
from DataBUS import Response
from DataBUS.AnalysisUnit import ANALYSIS_UNIT_PARAMS

# ── helpers ───────────────────────────────────────────────────────────────────


def _au_yml(extra_cols=None):
    """Minimal yml_dict with a rowwise depth column.

    extra_cols: list of additional metadata dicts to append.
    """
    entries = [
        {
            "column": "Depth",
            "neotoma": "ndb.analysisunits.depth",
            "rowwise": True,
            "type": "float",
            "required": True,
        }
    ]
    if extra_cols:
        entries.extend(extra_cols)
    return {"metadata": entries}


def _au_csv(depths, extra_fields=None):
    """Build a CSV (list of dicts) with the supplied depth values.

    extra_fields: dict of column → list of values to add to each row.
    """
    rows = [{"Depth": str(d)} for d in depths]
    if extra_fields:
        for i, row in enumerate(rows):
            for col, vals in extra_fields.items():
                row[col] = str(vals[i]) if i < len(vals) else ""
    return rows


# ── AU counter tests ───────────────────────────────────────────────────────────


class TestAnalysisUnitCounter:
    """The number of AUs created must equal the number of depth rows."""

    def test_10_depths_produce_10_aus(self, mock_cur):
        depths = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = nv.valid_analysisunit(cur=mock_cur, yml_dict=_au_yml(), csv_file=_au_csv(depths))
        assert isinstance(result, Response)
        assert result.counter == len(depths), f"Expected {len(depths)} AUs but got {result.counter}"

    def test_5_depths_produce_5_aus(self, mock_cur):
        depths = [0.5, 1.5, 2.5, 3.5, 4.5]
        result = nv.valid_analysisunit(cur=mock_cur, yml_dict=_au_yml(), csv_file=_au_csv(depths))
        assert result.counter == len(depths)

    def test_1_depth_produces_1_au(self, mock_cur):
        depths = [5.0]
        result = nv.valid_analysisunit(cur=mock_cur, yml_dict=_au_yml(), csv_file=_au_csv(depths))
        assert result.counter == 1

    def test_au_count_matches_pb210_data(self, mock_cur, pb210_pair):
        """AU counter equals the number of depth rows in the 210Pb dataset."""
        csv_file, yml_dict = pb210_pair
        inputs = nh.pull_params(ANALYSIS_UNIT_PARAMS, yml_dict, csv_file, "ndb.analysisunits")
        depths = inputs.get("depth")
        expected = len(depths) if isinstance(depths, list) else 1

        result = nv.valid_analysisunit(cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file)
        assert result.counter == expected, (
            f"Expected {expected} AUs for 210Pb dataset but got {result.counter}"
        )

    def test_au_count_with_thickness(self, mock_cur):
        """Counter stays correct when thickness column is also provided."""
        depths = [1.0, 2.0, 3.0]
        thickness_col = {
            "column": "Thickness",
            "neotoma": "ndb.analysisunits.thickness",
            "rowwise": True,
            "type": "float",
        }
        csv_file = _au_csv(depths, extra_fields={"Thickness": [0.5, 0.5, 0.5]})
        result = nv.valid_analysisunit(
            cur=mock_cur,
            yml_dict=_au_yml(extra_cols=[thickness_col]),
            csv_file=csv_file,
        )
        assert result.counter == len(depths)


# ── other basic valid_analysisunit tests ─────────────────────────────────────


class TestValidAnalysisUnitBasic:
    def test_returns_response(self, mock_cur):
        result = nv.valid_analysisunit(cur=mock_cur, yml_dict={"metadata": []}, csv_file=[])
        assert isinstance(result, Response)

    def test_empty_csv_still_returns_response(self, mock_cur):
        result = nv.valid_analysisunit(cur=mock_cur, yml_dict=_au_yml(), csv_file=[])
        assert isinstance(result, Response)
