"""Tests for long-format files: grouping rows into analysis units and reading datasetname.

A long file has one row per measurement. Marking the depth entry of the template with
``groupby: true`` makes rows with the same depth one analysis unit, one sample, and gives every
datum the sample of its unit. The dataset name is read from the data, never invented.
"""

from unittest.mock import MagicMock

import DataBUS.neotomaHelpers as nh
import DataBUS.neotomaValidator as nv

# ── helpers ───────────────────────────────────────────────────────────────────

DEPTH = {
    "column": "Depth",
    "neotoma": "ndb.analysisunits.depth",
    "rowwise": True,
    "type": "float",
}


def _yml(*entries, groupby=True):
    depth = {**DEPTH, "groupby": True} if groupby else dict(DEPTH)
    return {"metadata": [depth, *entries]}


def _rows(spec):
    """spec: list of (depth, taxon) pairs -> long rows with a value of 1.0."""
    return [{"Depth": str(d), "Taxon": t, "Units": "NISP", "Value": "1.0"} for d, t in spec]


LONG_SPEC = [(10, "A"), (10, "B"), (20, "A"), (30, "A"), (30, "B")]


# ── group_rows ────────────────────────────────────────────────────────────────


class TestGroupRows:
    def test_no_groupby_is_identity(self):
        rows = _rows(LONG_SPEC)
        unit_rows, row_to_unit = nh.group_rows(_yml(groupby=False), rows)
        assert unit_rows is rows
        assert row_to_unit == [0, 1, 2, 3, 4]

    def test_same_depth_is_one_unit_in_order_of_appearance(self):
        unit_rows, row_to_unit = nh.group_rows(_yml(), _rows(LONG_SPEC))
        assert [r["Depth"] for r in unit_rows] == ["10", "20", "30"]
        assert row_to_unit == [0, 0, 1, 2, 2]

    def test_non_adjacent_rows_with_same_depth_share_a_unit(self):
        rows = _rows([(10, "A"), (20, "A"), (10, "B")])
        unit_rows, row_to_unit = nh.group_rows(_yml(), rows)
        assert len(unit_rows) == 2
        assert row_to_unit == [0, 1, 0]

    def test_blank_depth_rows_are_never_merged(self):
        rows = _rows([(10, "A"), ("", "A"), ("", "B"), ("NA", "C")])
        unit_rows, row_to_unit = nh.group_rows(_yml(), rows)
        assert len(unit_rows) == 4
        assert row_to_unit == [0, 1, 2, 3]

    def test_several_groupby_columns_group_by_the_combination(self):
        sector = {"column": "Sector", "neotoma": "ndb.analysisunits.analysisunitname"}
        yml = _yml({**sector, "groupby": True})
        rows = [
            {"Depth": "10", "Sector": "N"},
            {"Depth": "10", "Sector": "S"},
            {"Depth": "10", "Sector": "N"},
        ]
        _, row_to_unit = nh.group_rows(yml, rows)
        assert row_to_unit == [0, 1, 0]

    def test_groupby_on_another_table_is_ignored(self):
        yml = {
            "metadata": [
                {"column": "Depth", "neotoma": "ndb.samples.samplename", "groupby": True},
            ]
        }
        unit_rows, row_to_unit = nh.group_rows(yml, _rows(LONG_SPEC))
        assert len(unit_rows) == 5
        assert row_to_unit == [0, 1, 2, 3, 4]

    def test_sheet_dict_is_returned_unchanged(self):
        sheets = {"Sheet1": _rows(LONG_SPEC)}
        unit_rows, row_to_unit = nh.group_rows(_yml(), sheets)
        assert unit_rows is sheets
        assert row_to_unit is None


# ── analysis units and samples ────────────────────────────────────────────────


class TestLongFormatUnits:
    def test_grouped_rows_make_one_analysis_unit_per_depth(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        databus = {"collunits": MagicMock(id_int=1)}
        result = nv.valid_analysisunit(
            cur=mock_cur, yml_dict=_yml(), csv_file=_rows(LONG_SPEC), databus=databus
        )
        assert result.counter == 3
        assert len(result.id_list) == 3
        assert result.validAll

    def test_without_groupby_every_row_is_a_unit(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        databus = {"collunits": MagicMock(id_int=1)}
        result = nv.valid_analysisunit(
            cur=mock_cur, yml_dict=_yml(groupby=False), csv_file=_rows(LONG_SPEC), databus=databus
        )
        assert result.counter == 5

    def test_grouped_rows_make_one_sample_per_unit(self, mock_cur):
        mock_cur.mock_fetchone = (10,)
        name = {"column": "Name", "neotoma": "ndb.samples.samplename", "rowwise": True}
        rows = [{**r, "Name": f"S{r['Depth']}"} for r in _rows(LONG_SPEC)]
        databus = {
            "analysisunits": MagicMock(id_list=[1, 2, 3]),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(cur=mock_cur, yml_dict=_yml(name), csv_file=rows, databus=databus)
        assert not any("mismatched row counts" in m for m in result.message)
        assert result.counter == 3
        assert result.validAll


# ── data ──────────────────────────────────────────────────────────────────────


def _data_yml(**kwargs):
    entries = [
        {"column": "Value", "neotoma": "ndb.data.value", "rowwise": True, "type": "float"},
        {
            "column": "Taxon",
            "neotoma": "ndb.variables.taxonid",
            "rowwise": True,
            "type": "string",
        },
        {
            "column": "Units",
            "neotoma": "ndb.variables.variableunitsid",
            "rowwise": True,
            "type": "string",
        },
    ]
    return _yml(*entries, **kwargs)


def _datum_sampleids(cur):
    return [p["sampleid"] for q, p in cur._execute_calls if "ts.insertdata" in q]


class TestLongFormatData:
    def test_each_datum_gets_the_sample_of_its_depth(self, mock_cur):
        mock_cur.mock_fetchone = (7,)
        databus = {"samples": MagicMock(id_list=[101, 102, 103])}
        result = nv.valid_data(
            cur=mock_cur, yml_dict=_data_yml(), csv_file=_rows(LONG_SPEC), databus=databus
        )
        assert _datum_sampleids(mock_cur) == [101, 101, 102, 103, 103]
        assert result.validAll

    def test_without_groupby_rows_pair_with_samples_by_position(self, mock_cur):
        mock_cur.mock_fetchone = (7,)
        rows = _rows([(10, "A"), (20, "A"), (30, "A")])
        databus = {"samples": MagicMock(id_list=[101, 102, 103])}
        nv.valid_data(
            cur=mock_cur, yml_dict=_data_yml(groupby=False), csv_file=rows, databus=databus
        )
        assert _datum_sampleids(mock_cur) == [101, 102, 103]

    def test_unit_and_sample_counts_that_disagree_are_invalid(self, mock_cur):
        mock_cur.mock_fetchone = (7,)
        databus = {"samples": MagicMock(id_list=[101, 102])}
        result = nv.valid_data(
            cur=mock_cur, yml_dict=_data_yml(), csv_file=_rows(LONG_SPEC), databus=databus
        )
        assert False in result.valid
        assert any("Cannot assign data to samples" in m for m in result.message)
        assert _datum_sampleids(mock_cur) == []

    def test_single_sample_is_still_broadcast(self, mock_cur):
        mock_cur.mock_fetchone = (7,)
        databus = {"samples": MagicMock(id_list=[42])}
        nv.valid_data(
            cur=mock_cur, yml_dict=_data_yml(), csv_file=_rows(LONG_SPEC), databus=databus
        )
        assert _datum_sampleids(mock_cur) == [42] * 5


# ── dataset name ──────────────────────────────────────────────────────────────


def _dataset_yml(*entries):
    type_entry = {
        "column": "datasettypeid",
        "neotoma": "ndb.datasettypes.datasettypeid",
        "value": "plant macrofossil",
    }
    return {"metadata": [type_entry, *entries]}


NAME_COLUMN = {
    "column": "DatasetName",
    "neotoma": "ndb.datasets.datasetname",
    "rowwise": False,
    "type": "string",
}


def _inserted_datasetname(cur):
    (params,) = [p for q, p in cur._execute_calls if "insertdataset" in q]
    return params["datasetname"]


class TestDatasetName:
    def _run(self, cur, yml_dict, csv_file):
        cur.mock_fetchone = (5,)
        return nv.valid_dataset(
            cur=cur,
            yml_dict=yml_dict,
            csv_file=csv_file,
            databus={"collunits": MagicMock(id_int=1)},
        )

    def test_name_comes_from_the_csv_column(self, mock_cur):
        rows = [{"DatasetName": "Darko macrofossils"}, {"DatasetName": "Darko macrofossils"}]
        result = self._run(mock_cur, _dataset_yml(NAME_COLUMN), rows)
        assert result.validAll
        assert _inserted_datasetname(mock_cur) == "Darko macrofossils"

    def test_blank_column_inserts_null_not_a_made_up_name(self, mock_cur):
        rows = [{"DatasetName": ""}, {"DatasetName": ""}]
        result = self._run(mock_cur, _dataset_yml(NAME_COLUMN), rows)
        assert result.validAll
        assert _inserted_datasetname(mock_cur) is None

    def test_template_constant_still_works_when_the_column_is_absent(self, mock_cur):
        constant = {**NAME_COLUMN, "value": "Constant name"}
        result = self._run(mock_cur, _dataset_yml(constant), [{"Other": "x"}])
        assert result.validAll
        assert _inserted_datasetname(mock_cur) == "Constant name"

    def test_no_datasetname_entry_inserts_null(self, mock_cur):
        result = self._run(mock_cur, _dataset_yml(), [{"Other": "x"}])
        assert result.validAll
        assert _inserted_datasetname(mock_cur) is None


# ── unresolved names ──────────────────────────────────────────────────────────


class LookupCursor:
    """Answers valid_data's lookups from fixed tables, like a database would.

    taxa/units map a lower-cased name to an id; a name that is not listed has no row.
    A variable's id is derived from its taxon id, so rows with different taxa never share one.
    """

    def __init__(self, taxa, units):
        self.taxa, self.units = taxa, units
        self.calls = []
        self._row = None

    def execute(self, query, params=None):
        self.calls.append((query, params))
        if "FROM ndb.taxa" in query:
            taxonid = self.taxa.get(params["taxonid"])
            self._row = (taxonid,) if taxonid else None
        elif "FROM ndb.variableunits" in query:
            unitid = self.units.get(params["variableunitsid"])
            self._row = (unitid,) if unitid else None
        elif "FROM ndb.variables" in query:
            self._row = (1000 + params["taxonid"],)
        elif "ts.insertdata(" in query:
            self._row = (len(self.calls),)
        else:
            self._row = None

    def fetchone(self):
        return self._row


class TestUnresolvedNames:
    """A row whose taxon or units is not in Neotoma is reported FALSE, never borrowed from a neighbour.

    Variable() fails for such a row; valid_data used to carry on with the Variable of the previous
    row and insert the datum under it.
    """

    def _run(self, spec, taxa):
        cur = LookupCursor(taxa=taxa, units={"nisp": 7})
        depths = sorted({depth for depth, _ in spec})
        databus = {"samples": MagicMock(id_list=[101 + i for i in range(len(depths))])}
        result = nv.valid_data(cur=cur, yml_dict=_data_yml(), csv_file=_rows(spec), databus=databus)
        return cur, result

    def test_unknown_taxon_is_false_and_not_inserted_under_another_variable(self):
        spec = [(10, "Alnus"), (20, "Nonexistentia"), (30, "Betula")]
        cur, result = self._run(spec, taxa={"alnus": 1, "betula": 2})
        inserted = [p for q, p in cur.calls if "ts.insertdata(" in q]
        assert False in result.valid
        assert not result.validAll
        assert any("taxonid with value Nonexistentia does not exist" in m for m in result.message)
        assert [(p["sampleid"], p["variableid"]) for p in inserted] == [(101, 1001), (103, 1002)]

    def test_unknown_taxon_in_the_first_row_is_false(self):
        spec = [(10, "Nonexistentia"), (20, "Alnus")]
        cur, result = self._run(spec, taxa={"alnus": 1})
        inserted = [p for q, p in cur.calls if "ts.insertdata(" in q]
        assert not result.validAll
        assert [(p["sampleid"], p["variableid"]) for p in inserted] == [(102, 1001)]

    def test_all_names_known_is_valid(self):
        spec = [(10, "Alnus"), (20, "Betula"), (30, "Alnus")]
        _, result = self._run(spec, taxa={"alnus": 1, "betula": 2})
        assert result.validAll
