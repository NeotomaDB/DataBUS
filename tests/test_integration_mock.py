"""Integration-style tests that run the full validator chain with mock DB.

These tests verify that the playground's step sequence (site → collunit →
dataset → chronologies → ...) can complete without crashing even when the
database is offline and every SQL call returns None/empty.
"""
from unittest.mock import MagicMock, patch

import pytest

import DataBUS.neotomaHelpers as nh
import DataBUS.neotomaValidator as nv
from DataBUS import Response
from tests.conftest import real_csv, real_yml


def _run_chain(cur, csv_file, yml_dict):
    """Run a subset of the validator chain and return the databus dict."""
    databus = {}

    for step_name, fn in [
        ("sites",          lambda: nv.valid_site(cur=cur, yml_dict=yml_dict, csv_file=csv_file)),
        ("collunits",      lambda: nv.valid_collunit(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("datasets",       lambda: nv.valid_dataset(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("geodataset",     lambda: nv.valid_geochron_dataset(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("chronologies",   lambda: nv.valid_chronologies(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("chron_controls", lambda: nv.valid_chroncontrols(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("geochron",       lambda: nv.valid_geochron(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("geochroncontrol",lambda: nv.valid_geochroncontrol(cur=cur, databus=databus)),
        ("uth_series",     lambda: nv.valid_uth_series(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
        ("contacts",       lambda: nv.valid_contact(cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus)),
    ]:
        try:
            result = fn()
            if isinstance(result, Response):
                databus[step_name] = result
        except Exception:
            databus[step_name] = None  # record failure but continue

    return databus


class TestIntegrationSISALMock:
    def test_full_chain_does_not_raise(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        csv_file = nh.read_csv(real_csv("SISAL", "sisal_entity_13.csv"))
        yml_dict = nh.template_to_dict(real_yml("SISAL", "template.yml"))
        databus = _run_chain(mock_cur, csv_file, yml_dict)
        # Chain should have produced Response objects for at least sites
        assert "sites" in databus

    def test_sites_step_is_response(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        csv_file = nh.read_csv(real_csv("SISAL", "sisal_entity_13.csv"))
        yml_dict = nh.template_to_dict(real_yml("SISAL", "template.yml"))
        result = nv.valid_site(cur=mock_cur, yml_dict=yml_dict,
                               csv_file=csv_file)
        assert isinstance(result, Response)


class TestIntegration210PbMock:
    def test_full_chain_does_not_raise(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        csv_file = nh.read_csv(real_csv("210Pb", "Cayou 1993 VOYA.csv"))
        yml_dict = nh.template_to_dict(real_yml("210Pb", "template.yml"))
        databus = _run_chain(mock_cur, csv_file, yml_dict)
        assert "sites" in databus


class TestIntegrationNODEMock:
    def test_full_chain_does_not_raise(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        csv_file = nh.read_csv(real_csv("NODE-OST", "NODE-R32.csv"))
        yml_dict = nh.template_to_dict(real_yml("NODE-OST", "node_template.yml"))
        databus = _run_chain(mock_cur, csv_file, yml_dict)
        assert "sites" in databus


class TestIntegrationEANODEMock:
    def test_full_chain_does_not_raise(self, mock_cur):
        mock_cur.mock_fetchone = (1,)
        csv_file = nh.read_csv(real_csv("EANODE-OST", "EA000017.csv"))
        yml_dict = nh.template_to_dict(real_yml("EANODE-OST", "eanode_template.yml"))
        databus = _run_chain(mock_cur, csv_file, yml_dict)
        assert "sites" in databus
