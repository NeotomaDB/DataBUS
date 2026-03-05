"""Unit tests for DataBUS domain classes (no DB required)."""

import pytest

from DataBUS import (
    ChronControl,
    Chronology,
    CollectionUnit,
    Contact,
    Dataset,
    Geochron,
    GeochronControl,
    Site,
    UThSeries,
)


# ── Contact ───────────────────────────────────────────────────────────────────
class TestContact:
    def test_valid_contact(self):
        c = Contact(contactid=1, contactname="Jane Doe", order=1)
        assert c.contactid == 1
        assert c.contactname == "Jane Doe"
        assert c.order == 1

    def test_contact_none_name(self):
        c = Contact(contactid=5)
        assert c.contactname is None

    def test_contact_invalid_id_raises(self):
        with pytest.raises((ValueError, TypeError)):
            Contact(contactid="not_an_int")

    def test_contact_str(self):
        c = Contact(contactid=2, contactname="Bob", order=1)
        assert "2" in str(c)
        assert "Bob" in str(c)


# ── Chronology ────────────────────────────────────────────────────────────────
class TestChronology:
    def test_valid_chronology(self):
        ch = Chronology(
            collectionunitid=1,
            agetypeid=2,
            chronologyname="Model A",
            ageboundyounger=-50,
            ageboundolder=12000,
        )
        assert ch.chronologyname == "Model A"
        assert ch.ageboundolder == 12000

    def test_chronology_requires_collectionunitid(self):
        with pytest.raises(ValueError):
            Chronology()

    def test_chronology_age_bounds_invalid(self):
        with pytest.raises(AssertionError):
            Chronology(collectionunitid=1, ageboundyounger=5000, ageboundolder=100)

    def test_chronology_list_contactid(self):
        ch = Chronology(collectionunitid=1, contactid=[3, 3])
        assert ch.contactid == 3

    def test_chronology_list_contactid_multiple_raises(self):
        with pytest.raises(AssertionError):
            Chronology(collectionunitid=1, contactid=[3, 4])


# ── ChronControl ─────────────────────────────────────────────────────────────
class TestChronControl:
    def test_valid_chron_control(self):
        cc = ChronControl(
            chronologyid=1, chroncontroltypeid=2, analysisunitid=3, agetypeid=1, depth=10.5, age=500
        )
        assert cc.age == 500
        assert cc.depth == 10.5

    def test_missing_required_raises(self):
        with pytest.raises(ValueError):
            ChronControl()  # all required params missing

    def test_chron_control_str(self):
        cc = ChronControl(
            chronologyid=1, chroncontroltypeid=1, analysisunitid=1, agetypeid=1, depth=5.0, age=200
        )
        assert "200" in str(cc)


# ── Geochron ─────────────────────────────────────────────────────────────────
class TestGeochron:
    def test_valid_geochron(self):
        g = Geochron(
            sampleid=1, geochrontypeid=1, agetypeid=1, age=3250, errorolder=100, erroryounger=100
        )
        assert g.age == 3250
        assert g.infinite is False

    def test_geochron_missing_required_raises(self):
        with pytest.raises(ValueError):
            Geochron(age=100)

    def test_geochron_infinite_default_false(self):
        g = Geochron(sampleid=1, geochrontypeid=1, agetypeid=1, age=100)
        assert g.infinite is False


# ── UThSeries ────────────────────────────────────────────────────────────────
class TestUThSeries:
    def test_valid_uth_series(self):
        u = UThSeries(geochronid=1, decayconstantid=2, ratio230th232th=1.265)
        assert u.ratio230th232th == 1.265

    def test_uth_missing_required_raises(self):
        with pytest.raises(ValueError):
            UThSeries(geochronid=1)  # decayconstantid missing

    def test_uth_none_geochronid_raises(self):
        with pytest.raises(ValueError):
            UThSeries(geochronid=None, decayconstantid=1)


# ── GeochronControl ───────────────────────────────────────────────────────────
class TestGeochronControl:
    def test_valid_geochrono_control(self):
        gc = GeochronControl(chroncontrolid=10, geochronid=20)
        assert gc.chroncontrolid == 10
        assert gc.geochronid == 20
        assert gc.geochroncontrolid is None

    def test_geochrono_control_str(self):
        gc = GeochronControl(chroncontrolid=1, geochronid=2)
        text = str(gc)
        assert "1" in text and "2" in text


# ── Dataset ──────────────────────────────────────────────────────────────────
class TestDataset:
    def test_valid_dataset(self):
        d = Dataset(datasettypeid=3, collectionunitid=5, datasetname="Test")
        assert d.datasettypeid == 3

    def test_dataset_missing_type_raises(self):
        with pytest.raises(ValueError):
            Dataset(datasettypeid=None, collectionunitid=1)

    def test_dataset_missing_collunit_raises(self):
        with pytest.raises(ValueError):
            Dataset(datasettypeid=1, collectionunitid=None)
