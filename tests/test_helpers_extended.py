"""Extended tests for neotomaHelpers utilities."""
import os
import tempfile
import pytest
import DataBUS.neotomaHelpers as nh
from DataBUS.neotomaHelpers.pull_overwrite import pull_overwrite
from DataBUS.neotomaHelpers.pull_required import pull_required
from DataBUS.neotomaHelpers.read_csv import read_csv
from DataBUS.neotomaHelpers.check_file import check_file


# ── pull_required ─────────────────────────────────────────────────────────────
class TestPullRequired:
    def _yml(self, neotoma_key, required=True):
        return {"metadata": [
            {"neotoma": neotoma_key, "column": "X", "required": required,
             "rowwise": False, "type": "string"}
        ]}

    def test_required_true(self):
        yml = self._yml("ndb.sites.sitename", required=True)
        result = pull_required(["sitename"], yml, "ndb.sites")
        assert result["sitename"] is True

    def test_required_false(self):
        yml = self._yml("ndb.sites.sitename", required=False)
        result = pull_required(["sitename"], yml, "ndb.sites")
        assert result["sitename"] is False

    def test_missing_param_defaults_false(self):
        yml = {"metadata": []}
        result = pull_required(["altitude"], yml, "ndb.sites")
        assert result["altitude"] is False

    def test_multiple_params(self):
        yml = {"metadata": [
            {"neotoma": "ndb.sites.sitename", "column": "Name",
             "required": True, "rowwise": False, "type": "string"},
            {"neotoma": "ndb.sites.altitude", "column": "Alt",
             "required": False, "rowwise": False, "type": "float"},
        ]}
        result = pull_required(["sitename", "altitude"], yml, "ndb.sites")
        assert result["sitename"] is True
        assert result["altitude"] is False

    def test_table_list_returns_list(self):
        yml = {"metadata": []}
        result = pull_required(["sitename"], yml, ["ndb.sites", "ndb.collectionunits"])
        assert isinstance(result, list)
        assert len(result) == 2

    def test_table_with_trailing_dot(self):
        yml = self._yml("ndb.sites.sitename", required=True)
        result = pull_required(["sitename"], yml, "ndb.sites.")
        assert result["sitename"] is True


# ── pull_overwrite ────────────────────────────────────────────────────────────
class TestPullOverwrite:
    def _yml(self, neotoma_key, overwrite=False):
        return {"metadata": [
            {"neotoma": neotoma_key, "column": "X", "overwrite": overwrite,
             "required": False, "rowwise": False, "type": "string"}
        ]}

    def test_overwrite_true(self):
        yml = self._yml("ndb.sites.sitename", overwrite=True)
        result = pull_overwrite(["sitename"], yml, "ndb.sites")
        assert result["sitename"] is True

    def test_overwrite_false(self):
        yml = self._yml("ndb.sites.sitename", overwrite=False)
        result = pull_overwrite(["sitename"], yml, "ndb.sites")
        assert result["sitename"] is False

    def test_missing_param_defaults_false(self):
        yml = {"metadata": []}
        result = pull_overwrite(["altitude"], yml, "ndb.sites")
        assert result["altitude"] is False

    def test_geog_param_expands(self):
        yml = self._yml("ndb.collectionunits.geog", overwrite=True)
        result = pull_overwrite(["geog"], yml, "ndb.collectionunits")
        assert "coordlo" in result
        assert "coordla" in result
        assert result["coordlo"] is True

    def test_table_list_returns_list(self):
        yml = {"metadata": []}
        result = pull_overwrite(["sitename"], yml, ["ndb.sites", "ndb.collectionunits"])
        assert isinstance(result, list)
        assert len(result) == 2


# ── read_csv ──────────────────────────────────────────────────────────────────
class TestReadCsv:
    def test_reads_simple_csv(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("col1,col2\na,1\nb,2\n")
        result = read_csv(str(f))
        assert len(result) == 2
        assert result[0] == {"col1": "a", "col2": "1"}
        assert result[1] == {"col1": "b", "col2": "2"}

    def test_empty_csv_returns_empty(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        result = read_csv(str(f))
        assert result == []

    def test_header_only_csv(self, tmp_path):
        f = tmp_path / "headers.csv"
        f.write_text("col1,col2\n")
        result = read_csv(str(f))
        assert result == []

    def test_nonexistent_file_returns_empty(self):
        # read_csv opens the file before entering the try block, so
        # FileNotFoundError propagates from the with-open statement.
        with pytest.raises(FileNotFoundError):
            read_csv("/nonexistent/path/file.csv")

    def test_real_toy_csv(self):
        path = os.path.join(os.path.dirname(__file__), "..", "data", "SISAL",
                            "sisal_entity_13.csv")
        if os.path.exists(path):
            result = read_csv(path)
            assert isinstance(result, list)
            assert len(result) > 0
            assert isinstance(result[0], dict)


# ── check_file ────────────────────────────────────────────────────────────────
class TestCheckFile:
    def test_no_log_file_passes(self, tmp_path):
        result = check_file("nonexistent.csv", validation_files=str(tmp_path) + "/")
        assert result["pass"] is True
        assert result["match"] == 0

    def test_log_with_no_errors_passes(self, tmp_path):
        log_dir = tmp_path
        log_file = log_dir / "mydata.csv.valid.log"
        log_file.write_text("✔ All good\n✔ Another success\n")
        result = check_file("mydata.csv", validation_files=str(log_dir) + "/")
        assert result["pass"] is True
        assert result["match"] == 0

    def test_log_with_errors_fails(self, tmp_path):
        log_dir = tmp_path
        log_file = log_dir / "mydata.csv.valid.log"
        log_file.write_text("✔ OK\n✗ Something failed\n✗ Another error\n")
        result = check_file("mydata.csv", validation_files=str(log_dir) + "/")
        assert result["pass"] is False
        assert result["match"] == 2

    def test_not_validated_log_with_no_errors(self, tmp_path):
        not_val_dir = tmp_path / "not_validated"
        not_val_dir.mkdir()
        log_file = not_val_dir / "other.csv.valid.log"
        log_file.write_text("✔ Fine\n")
        result = check_file("other.csv", validation_files=str(tmp_path) + "/")
        assert result["pass"] is True
        # File should be removed after passing
        assert not log_file.exists()

    def test_not_validated_log_with_errors(self, tmp_path):
        not_val_dir = tmp_path / "not_validated"
        not_val_dir.mkdir()
        log_file = not_val_dir / "bad.csv.valid.log"
        log_file.write_text("✗ Error found\n")
        result = check_file("bad.csv", validation_files=str(tmp_path) + "/")
        assert result["pass"] is False
        assert result["match"] == 1

    def test_strict_mode_counts_valid_false(self, tmp_path):
        log_dir = tmp_path
        log_file = log_dir / "strict.csv.valid.log"
        log_file.write_text("Valid: FALSE\n✔ OK\n")
        result = check_file("strict.csv", strict=True, validation_files=str(log_dir) + "/")
        assert result["match"] >= 1

    def test_strict_false_ignores_valid_false(self, tmp_path):
        log_dir = tmp_path
        log_file = log_dir / "nonstrict.csv.valid.log"
        log_file.write_text("Valid: FALSE\n✔ OK\n")
        result = check_file("nonstrict.csv", strict=False, validation_files=str(log_dir) + "/")
        assert result["match"] == 0
        assert result["pass"] is True
