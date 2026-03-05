"""Tests for neotomaHelpers utilities."""
import os

import pytest

import DataBUS.neotomaHelpers as nh
from tests.conftest import toy_csv, toy_yml


class TestReadCsv:
    def test_read_csv_returns_list_of_dicts(self):
        rows = nh.read_csv(toy_csv("test_sisal.csv"))
        assert isinstance(rows, list)
        assert len(rows) > 0
        assert isinstance(rows[0], dict)

    def test_read_csv_210pb(self):
        rows = nh.read_csv(toy_csv("test_210Pb.csv"))
        assert len(rows) > 0

    def test_read_csv_node(self):
        rows = nh.read_csv(toy_csv("test_node.csv"))
        assert len(rows) > 0

    def test_read_csv_eanode(self):
        rows = nh.read_csv(toy_csv("test_eanode.csv"))
        assert len(rows) > 0


class TestTemplateToDist:
    def test_template_to_dict_sisal(self):
        d = nh.template_to_dict(toy_yml("test_sisal_template.yml"))
        assert isinstance(d, dict)
        assert "metadata" in d
        assert len(d["metadata"]) > 0

    def test_template_to_dict_210pb(self):
        d = nh.template_to_dict(toy_yml("test_210pb_template.yml"))
        assert isinstance(d, dict)

    def test_template_to_dict_node(self):
        d = nh.template_to_dict(toy_yml("test_node_template.yml"))
        assert isinstance(d, dict)

    def test_template_to_dict_eanode(self):
        d = nh.template_to_dict(toy_yml("test_eanode_template.yml"))
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
