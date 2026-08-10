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
    # FAIRe Project-section terms. Underscored to match the columns as they
    # exist in ndb.aednaassays.
    "sterilise_method",
    "neg_cont",
    "pos_cont",
]


class AeDNAAssay(AeDNAEntity):
    """An aeDNA assay record (ndb.aednaassays).

    Holds the PCR/assay configuration for a dataset: assay name and type,
    target gene/taxon, primer sequences and probe details, plus the FAIRe
    Project-section terms (``sterilise_method`` and the two control flags) that
    the checklist records once per project rather than per sample.
    """

    _TABLE = "ndb.aednaassays"
    _PARAMS = AEDNAASSAY_PARAMS
    _PK = "assayid"
    _INT_FIELDS = ("datasetid", "assaytypeid", "ampliconsize")
    _BOOL_FIELDS = ("neg_cont", "pos_cont")

    def __str__(self):
        return f"AeDNAAssay(assayid={self.assayid}, datasetid={self.datasetid}, assayname={self.assayname})"
