from .AeDNAEntity import AeDNAEntity

AEDNALIBRARY_PARAMS = [
    "datasetid",
    "assayid",
    "libid",
    "seqrunid",
    "pcrplateid",
    "barcodingpcrappr",
    "platform",
    "instrument",
    "seqkit",
    "libscreen",
]


class AeDNALibrary(AeDNAEntity):
    """An aeDNA sequencing library record (ndb.aednalibraries).

    Holds the library-preparation and sequencing metadata for a dataset's
    assay: platform, instrument, kit, and library/run identifiers.
    """

    _TABLE = "ndb.aednalibraries"
    _PARAMS = AEDNALIBRARY_PARAMS
    _PK = "libraryid"
    _INT_FIELDS = ("datasetid", "assayid")

    def __str__(self):
        return f"AeDNALibrary(libraryid={self.libraryid}, datasetid={self.datasetid}, assayid={self.assayid})"
