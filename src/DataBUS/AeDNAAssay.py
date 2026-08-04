from .AeDNAEntity import AeDNAEntity

AEDNAASSAY_PARAMS = [
    "datasetid",
    "assaytypeid",
    "assayname",
    "targettaxonomicassay",
    "targetgene",
    "subfragment",
    "ampliconsize",
    "pcrprimerforward",
    "pcrprimerreverse",
    "pcrprimernameforward",
    "pcrprimernamereverse",
    "pcrprimerreferenceforward",
    "pcrprimerreferencereverse",
    "probeseq",
    "proberef",
    "probereporter",
    "probequencher",
    "probeconc",
]


class AeDNAAssay(AeDNAEntity):
    """An aeDNA assay record (ndb.aednaassays).

    Holds the PCR/assay configuration for a dataset: assay name and type,
    target gene/taxon, primer sequences and probe details.
    """

    _TABLE = "ndb.aednaassays"
    _PARAMS = AEDNAASSAY_PARAMS
    _PK = "assayid"
    _INT_FIELDS = ("datasetid", "assaytypeid", "ampliconsize")

    def __str__(self):
        return f"AeDNAAssay(assayid={self.assayid}, datasetid={self.datasetid}, assayname={self.assayname})"
