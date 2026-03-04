"""Tests for neotomaHelpers utilities."""
import os

import pytest

import DataBUS.neotomaHelpers as nh
from tests.conftest import real_csv, real_yml


class TestReadCsv:
    def test_read_csv_returns_list_of_dicts(self):
        path = real_csv("SISAL", "sisal_entity_13.csv")
        rows = nh.read_csv(path)
        assert isinstance(rows, list)
        assert len(rows) > 0
        assert isinstance(rows[0], dict)

    def test_read_csv_210pb(self):
        path = real_csv("210Pb", "Cayou 1993 VOYA.csv")
        rows = nh.read_csv(path)
        assert len(rows) > 0

    def test_read_csv_node(self):
        path = real_csv("NODE-OST", "NODE-R32.csv")
        rows = nh.read_csv(path)
        assert len(rows) > 0

    def test_read_csv_eanode(self):
        path = real_csv("EANODE-OST", "EA000017.csv")
        rows = nh.read_csv(path)
        assert len(rows) > 0


class TestTemplateToDist:
    def test_template_to_dict_sisal(self):
        path = real_yml("SISAL", "template.yml")
        d = nh.template_to_dict(path)
        assert isinstance(d, dict)
        assert "metadata" in d
        assert len(d["metadata"]) > 0

    def test_template_to_dict_210pb(self):
        path = real_yml("210Pb", "template.yml")
        d = nh.template_to_dict(path)
        assert isinstance(d, dict)

    def test_template_to_dict_node(self):
        path = real_yml("NODE-OST", "node_template.yml")
        d = nh.template_to_dict(path)
        assert isinstance(d, dict)

    def test_template_to_dict_eanode(self):
        path = real_yml("EANODE-OST", "eanode_template.yml")
        d = nh.template_to_dict(path)
        assert isinstance(d, dict)


class TestPullParams:
    def test_pull_params_sisal_sites(self, sisal_pair):
        csv_file, yml_dict = sisal_pair
        from DataBUS.Site import SITE_PARAMS
        result = nh.pull_params(SITE_PARAMS, yml_dict, csv_file, "ndb.sites")
        assert isinstance(result, dict)

    def test_pull_params_returns_dict(self, pb210_pair):
        csv_file, yml_dict = pb210_pair
        from DataBUS.Site import SITE_PARAMS
        result = nh.pull_params(SITE_PARAMS, yml_dict, csv_file, "ndb.sites")
        assert isinstance(result, dict)


class TestToyData:
    """Quick sanity checks that the toy data files are readable."""

    def test_toy_sisal_readable(self):
        from tests.conftest import toy_csv
        rows = nh.read_csv(toy_csv("test_sisal.csv"))
        assert len(rows) > 0

    def test_toy_210pb_readable(self):
        from tests.conftest import toy_csv
        rows = nh.read_csv(toy_csv("test_210Pb.csv"))
        assert len(rows) > 0

    def test_toy_node_readable(self):
        from tests.conftest import toy_csv
        rows = nh.read_csv(toy_csv("test_node.csv"))
        assert len(rows) > 0

    def test_toy_eanode_readable(self):
        from tests.conftest import toy_csv
        rows = nh.read_csv(toy_csv("test_eanode.csv"))
        assert len(rows) > 0

    def test_toy_sisal_site_name_modified(self):
        from tests.conftest import toy_csv
        rows = nh.read_csv(toy_csv("test_sisal.csv"))
        assert rows[0].get("site_name") == "TestSite_SISAL"

    def test_toy_210pb_site_name_modified(self):
        from tests.conftest import toy_csv
        rows = nh.read_csv(toy_csv("test_210Pb.csv"))
        assert rows[0].get("Site.name") == "TestSite_210Pb"
