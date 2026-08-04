from DataBUS import AeDNALibrary
from DataBUS.AeDNALibrary import AEDNALIBRARY_PARAMS

from ._aedna_entity_validator import validate_aedna_entity

_LINKS = {"datasetid": "datasets", "assayid": "assays"}
_PRESENT = [p for p in AEDNALIBRARY_PARAMS if p not in _LINKS]


def valid_aednalibraries(cur, yml_dict, csv_file, databus=None):
    """Validate and insert the dataset's aeDNA library (ndb.aednalibraries).

    Reads the library ``value:`` constants (platform, instrument, kit,
    identifiers), links the dataset and the freshly inserted assay, and inserts
    one ``AeDNALibrary``.
    """
    return validate_aedna_entity(
        cur,
        yml_dict,
        AeDNALibrary,
        databus,
        links=_LINKS,
        present_fields=_PRESENT,
    )
