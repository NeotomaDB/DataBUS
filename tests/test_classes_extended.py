"""Extended unit tests for DataBUS domain classes (no DB required)."""
import pytest

from DataBUS import (
    AnalysisUnit,
    Contact,
    DataUncertainty,
    Datum,
    Hiatus,
    LeadModel,
    Sample,
    SampleAge,
    Variable,
)


# ── Variable ──────────────────────────────────────────────────────────────────
class TestVariable:
    def test_defaults_all_none(self):
        v = Variable()
        assert v.taxonid is None
        assert v.variableelementid is None
        assert v.variableunitsid is None
        assert v.variablecontextid is None
        assert v.varid is None

    def test_with_ids(self):
        v = Variable(taxonid=10, variableunitsid=3, variableelementid=5, variablecontextid=1)
        assert v.taxonid == 10
        assert v.variableunitsid == 3
        assert v.variableelementid == 5
        assert v.variablecontextid == 1

    def test_invalid_taxonid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            Variable(taxonid="not_an_int")

    def test_str_representation(self):
        v = Variable(taxonid=42, variableunitsid=3)
        assert "42" in str(v)

    def test_insert_to_db(self, mock_cur):
        mock_cur.mock_fetchone = (99,)
        v = Variable(taxonid=1, variableunitsid=2)
        assert v.insert_to_db(mock_cur) == 99

    def test_get_id_from_db(self, mock_cur):
        mock_cur.mock_fetchone = (55,)
        v = Variable(taxonid=1, variableunitsid=2)
        assert v.get_id_from_db(mock_cur) == (55,)


# ── Datum ─────────────────────────────────────────────────────────────────────
class TestDatum:
    def test_valid_datum(self):
        d = Datum(sampleid=1, variableid=42, value=125.3)
        assert d.sampleid == 1
        assert d.variableid == 42
        assert d.value == 125.3

    def test_all_none(self):
        d = Datum()
        assert d.sampleid is None
        assert d.variableid is None
        assert d.value is None

    def test_invalid_sampleid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            Datum(sampleid="bad")

    def test_str_representation(self):
        d = Datum(sampleid=5, variableid=10, value=0.5)
        s = str(d)
        assert "5" in s and "10" in s

    def test_insert_to_db(self, mock_cur):
        mock_cur.mock_fetchone = (77,)
        d = Datum(sampleid=1, variableid=2, value=3.0)
        assert d.insert_to_db(mock_cur) == 77


# ── SampleAge ─────────────────────────────────────────────────────────────────
class TestSampleAge:
    def test_valid_sample_age(self):
        sa = SampleAge(sampleid=1, chronologyid=2, age=500, ageyounger=400, ageolder=600)
        assert sa.age == 500
        assert sa.ageyounger == 400
        assert sa.ageolder == 600

    def test_defaults_none(self):
        sa = SampleAge()
        assert sa.sampleid is None
        assert sa.chronologyid is None
        assert sa.age is None

    def test_invalid_chronologyid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            SampleAge(chronologyid="x")

    def test_str_representation(self):
        sa = SampleAge(sampleid=1, age=750)
        assert "750" in str(sa)

    def test_insert_to_db(self, mock_cur):
        mock_cur.mock_fetchone = (42,)
        sa = SampleAge(sampleid=1, chronologyid=1, age=500)
        assert sa.insert_to_db(mock_cur) == 42


# ── DataUncertainty ───────────────────────────────────────────────────────────
class TestDataUncertainty:
    def test_valid_creation(self):
        du = DataUncertainty(
            dataid=1, uncertaintyvalue=5.0,
            uncertaintyunitid=2, uncertaintybasisid=1, notes=None
        )
        assert du.uncertaintyvalue == 5.0
        assert du.uncertaintybasisid == 1
        assert du.notes is None

    def test_invalid_dataid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            DataUncertainty(dataid="bad", uncertaintyvalue=1.0,
                            uncertaintyunitid=1, uncertaintybasisid=1, notes=None)

    def test_str_representation(self):
        du = DataUncertainty(dataid=3, uncertaintyvalue=2.5,
                             uncertaintyunitid=1, uncertaintybasisid=2, notes="test")
        assert "3" in str(du)

    def test_none_units_allowed(self):
        du = DataUncertainty(dataid=1, uncertaintyvalue=1.0,
                             uncertaintyunitid=None, uncertaintybasisid=None, notes=None)
        assert du.uncertaintyunitid is None

    def test_insert_to_db_calls_cursor(self, mock_cur):
        du = DataUncertainty(dataid=1, uncertaintyvalue=2.0,
                             uncertaintyunitid=1, uncertaintybasisid=1, notes=None)
        du.insert_to_db(mock_cur)
        assert mock_cur.last_query is not None


# ── AnalysisUnit ──────────────────────────────────────────────────────────────
class TestAnalysisUnit:
    def test_defaults(self):
        au = AnalysisUnit()
        assert au.depth is None
        assert au.mixed is False

    def test_with_depth(self):
        au = AnalysisUnit(collectionunitid=1, depth=2.5, thickness=0.5)
        assert au.depth == 2.5
        assert au.thickness == 0.5

    def test_mixed_true(self):
        au = AnalysisUnit(mixed=True)
        assert au.mixed is True

    def test_mixed_none_becomes_false(self):
        au = AnalysisUnit(mixed=None)
        assert au.mixed is False

    def test_invalid_faciesid_raises(self):
        with pytest.raises(ValueError):
            AnalysisUnit(faciesid="not_int")

    def test_invalid_mixed_raises(self):
        with pytest.raises(ValueError):
            AnalysisUnit(mixed="yes")

    def test_str_representation(self):
        au = AnalysisUnit(analysisunitname="AU-1", analysisunitid=10)
        assert "AU-1" in str(au)

    def test_insert_to_db(self, mock_cur):
        mock_cur.mock_fetchone = (15,)
        au = AnalysisUnit(collectionunitid=1, depth=5.0)
        assert au.insert_to_db(mock_cur) == 15

    def test_notes_and_igsn(self):
        au = AnalysisUnit(notes="A note", igsn="IGSN12345")
        assert au.notes == "A note"
        assert au.igsn == "IGSN12345"

    def test_integer_faciesid(self):
        au = AnalysisUnit(faciesid=3)
        assert au.faciesid == 3


# ── Sample ────────────────────────────────────────────────────────────────────
class TestSample:
    def test_valid_sample(self):
        s = Sample(analysisunitid=1, datasetid=2, samplename="Pollen-2cm")
        assert s.samplename == "Pollen-2cm"
        assert s.analysisunitid == 1
        assert s.datasetid == 2

    def test_missing_analysisunitid_raises(self):
        with pytest.raises(ValueError):
            Sample(analysisunitid=None, datasetid=1)

    def test_missing_datasetid_raises(self):
        with pytest.raises(ValueError):
            Sample(analysisunitid=1, datasetid=None)

    def test_str_representation(self):
        s = Sample(analysisunitid=1, datasetid=1, samplename="S1")
        s.sampleid = None  # sampleid is only assigned after insert_to_db
        assert "S1" in str(s)

    def test_optional_fields_none(self):
        s = Sample(analysisunitid=1, datasetid=1)
        assert s.taxonid is None
        assert s.notes is None
        assert s.prepmethod is None

    def test_insert_to_db(self, mock_cur):
        mock_cur.mock_fetchone = (50,)
        s = Sample(analysisunitid=1, datasetid=1, samplename="Test")
        assert s.insert_to_db(mock_cur) == 50


# ── Hiatus ────────────────────────────────────────────────────────────────────
class TestHiatus:
    def test_valid_hiatus(self):
        h = Hiatus(analysisunitstart=10, analysisunitend=15)
        assert h.analysisunitstart == 10
        assert h.analysisunitend == 15

    def test_missing_start_raises(self):
        with pytest.raises(ValueError):
            Hiatus(analysisunitstart=None, analysisunitend=5)

    def test_missing_end_raises(self):
        with pytest.raises(ValueError):
            Hiatus(analysisunitstart=5, analysisunitend=None)

    def test_str_representation(self):
        h = Hiatus(analysisunitstart=1, analysisunitend=3)
        s = str(h)
        assert "1" in s and "3" in s

    def test_equality(self):
        h1 = Hiatus(analysisunitstart=1, analysisunitend=2, notes="gap")
        h2 = Hiatus(analysisunitstart=1, analysisunitend=2, notes="gap")
        assert h1 == h2

    def test_inequality(self):
        h1 = Hiatus(analysisunitstart=1, analysisunitend=2)
        h2 = Hiatus(analysisunitstart=1, analysisunitend=3)
        assert h1 != h2

    def test_with_notes(self):
        h = Hiatus(analysisunitstart=5, analysisunitend=10, notes="erosion")
        assert h.notes == "erosion"

    def test_insert_to_db(self, mock_cur):
        mock_cur.mock_fetchone = (88,)
        h = Hiatus(analysisunitstart=1, analysisunitend=2)
        assert h.insert_to_db(mock_cur) == 88

    def test_insert_hiatus_chron_calls_cursor(self, mock_cur):
        h = Hiatus(hiatusid=1, analysisunitstart=1, analysisunitend=2)
        h.insert_hiatus_chron_to_db(
            chronologyid=5, hiatuslength=100.0, hiatusuncertainty=10.0, cur=mock_cur
        )
        assert mock_cur.last_query is not None


# ── LeadModel ─────────────────────────────────────────────────────────────────
class TestLeadModel:
    def test_valid_lead_model(self):
        lm = LeadModel(pbbasisid=1, analysisunitid=2, cumulativeinventory=145.3)
        assert lm.cumulativeinventory == 145.3
        assert lm.pbbasisid == 1

    def test_defaults_none(self):
        lm = LeadModel()
        assert lm.pbbasisid is None
        assert lm.cumulativeinventory is None
        assert lm.datinghorizon is None

    def test_invalid_pbbasisid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            LeadModel(pbbasisid="bad")

    def test_str_representation(self):
        lm = LeadModel(pbbasisid=1, analysisunitid=2, cumulativeinventory=100.0)
        assert "100.0" in str(lm)

    def test_insert_to_db_calls_cursor(self, mock_cur):
        lm = LeadModel(pbbasisid=1, analysisunitid=1, cumulativeinventory=50.0)
        lm.insert_to_db(mock_cur)
        assert mock_cur.last_query is not None


# ── Contact (insert methods) ──────────────────────────────────────────────────
class TestContactInsertMethods:
    def test_insert_pi_calls_cursor(self, mock_cur):
        c = Contact(contactid=1, contactname="Jane Doe", order=1)
        c.insert_pi(mock_cur, datasetid=10)
        assert mock_cur.last_query is not None

    def test_insert_pi_invalid_datasetid_raises(self, mock_cur):
        c = Contact(contactid=1, order=1)
        with pytest.raises(ValueError):
            c.insert_pi(mock_cur, datasetid="bad")

    def test_insert_data_processor(self, mock_cur):
        c = Contact(contactid=2, order=1)
        c.insert_data_processor(mock_cur, datasetid=5)
        assert mock_cur.last_query is not None

    def test_insert_sample_analyst(self, mock_cur):
        c = Contact(contactid=3, order=2)
        c.insert_sample_analyst(mock_cur, sampleid=7)
        assert mock_cur.last_query is not None

    def test_insert_collector(self, mock_cur):
        c = Contact(contactid=4, order=1)
        c.insert_collector(mock_cur, collunitid=9)
        assert mock_cur.last_query is not None

    def test_insert_collector_invalid_collunitid_raises(self, mock_cur):
        c = Contact(contactid=4, order=1)
        with pytest.raises(ValueError):
            c.insert_collector(mock_cur, collunitid="bad")
