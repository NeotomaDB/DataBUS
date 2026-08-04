"""Tests for valid_sample validator."""

from unittest.mock import MagicMock

import pytest

import DataBUS.neotomaValidator as nv
from DataBUS import Response


class TestValidSampleMock:
    def test_returns_response_no_databus_ids(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        databus = {
            "analysisunits": MagicMock(id_list=[], counter=5),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert False in result.valid

    def test_with_real_au_ids(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (10,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)
        assert result.counter >= 1

    def test_counter_equals_rows(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (5,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert result.counter == n

    def test_sisal_pair(self, mock_cur, sisal_pair):
        csv_file, yml_dict = sisal_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (1,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result, Response)

    def test_no_au_ids_uses_placeholder(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        databus = {
            "analysisunits": MagicMock(id_list=[], counter=3),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert False in result.valid

    def test_id_list_populated_on_insert(self, mock_cur, pb210_pair):
        csv_file, yml_dict = pb210_pair
        n = len(csv_file)
        mock_cur.mock_fetchone = (99,)
        databus = {
            "analysisunits": MagicMock(id_list=list(range(1, n + 1))),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert isinstance(result.id_list, list)


class TestValidSampleRowCountGuard:
    """Sample names are paired with analysis units by position alone.

    When the rowwise columns cover different numbers of samples the zip used to
    truncate in silence, dropping samples and leaving the survivors carrying
    names that belong to other rows. It must fail loudly instead. This is what
    put WLO56 at 0.5 cm in the Okoboji load.
    """

    NAMES = ["WLO17", "WLO18", "WLO19"]

    @pytest.fixture
    def named_samples(self):
        """A file with one rowwise samplename column, so row counts are visible."""
        yml_dict = {
            "metadata": [
                {
                    "column": "SampleName",
                    "neotoma": "ndb.samples.samplename",
                    "rowwise": True,
                    "type": "string",
                }
            ]
        }
        csv_file = [{"SampleName": n} for n in self.NAMES]
        return csv_file, yml_dict

    def test_fewer_analysis_units_than_samples_is_invalid(self, mock_cur, named_samples):
        csv_file, yml_dict = named_samples
        databus = {
            # One analysis unit short of the three named samples.
            "analysisunits": MagicMock(id_list=[1, 2], counter=2),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert False in result.valid
        assert any("mismatched row counts" in m for m in result.message)
        # No samples are built from a mismatched file, rather than a silent two.
        assert result.counter == 0

    def test_message_names_both_counts(self, mock_cur, named_samples):
        csv_file, yml_dict = named_samples
        databus = {
            "analysisunits": MagicMock(id_list=[1, 2], counter=2),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        msg = next(m for m in result.message if "mismatched row counts" in m)
        assert "samplename=3" in msg
        assert "analysisunitid=2" in msg

    def test_matching_row_counts_pass_the_guard(self, mock_cur, named_samples):
        csv_file, yml_dict = named_samples
        mock_cur.mock_fetchone = (10,)
        databus = {
            "analysisunits": MagicMock(id_list=[1, 2, 3]),
            "datasets": MagicMock(id_int=1),
        }
        result = nv.valid_sample(
            cur=mock_cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus
        )
        assert not any("mismatched row counts" in m for m in result.message)
        assert result.counter == len(self.NAMES)
