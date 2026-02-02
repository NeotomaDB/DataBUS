class Sample:
    """A sample in Neotoma.

    The intersection between an analysis unit and a dataset, representing
    physical material analyzed with associated metadata.
    
    See the [Neotoma Manual](https://open.neotomadb.org/manual/sample-related-tables-1.html#Samples)
    
    Attributes:
        analysisunitid (int | None): Analysis unit ID.
        datasetid (int | None): Dataset ID.
        samplename (str | None): Sample name (first element if list provided).
        sampledate (datetime | None): Collection date.
        analysisdate (datetime | None): Analysis date.
        taxonid (int | None): Taxon ID.
        labnumber (str | None): Laboratory number.
        prepmethod (str | None): Preparation method.
        notes (str | None): Additional notes.
        sampleid (int | None): Sample ID (assigned after insertion).

    Examples:
        >>> sample = Sample(analysisunitid=1, samplename="Pollen-2cm")
        >>> sample.samplename
        'Pollen-2cm'
    """

    def __init__(
        self,
        analysisunitid=None,
        datasetid=None,
        samplename=None,
        sampledate=None,
        analysisdate=None,
        taxonid=None,
        labnumber=None,
        prepmethod=None,
        notes=None,
    ):
        self.analysisunitid = analysisunitid
        self.datasetid = datasetid
        if isinstance(samplename, (tuple,list)):
            self.samplename = samplename[0]
        else:
            self.samplename = samplename
        self.sampledate = sampledate
        self.analysisdate = analysisdate
        self.taxonid = taxonid
        self.labnumber = labnumber
        self.prepmethod = prepmethod
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the sample record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The sampleid assigned by the database.
        """
        sample_q = """
        SELECT ts.insertsample(_analysisunitid := %(analysisunitid)s,
                               _datasetid := %(datasetid)s,
                               _samplename := %(samplename)s,
                               _sampledate := %(sampledate)s,
                               _analysisdate := %(analysisdate)s,
                               _taxonid := %(taxonid)s,
                               _labnumber := %(labnumber)s,
                               _prepmethod := %(prepmethod)s,
                               _notes := %(notes)s)
                        """
        inputs = {
            "analysisunitid": self.analysisunitid,
            "datasetid": self.datasetid,
            "samplename": self.samplename,
            "sampledate": self.sampledate,
            "analysisdate": self.analysisdate,
            "taxonid": self.taxonid,
            "labnumber": self.labnumber,
            "prepmethod": self.prepmethod,
            "notes": self.notes,
        }

        cur.execute(sample_q, inputs)
        self.sampleid = cur.fetchone()[0]
        return self.sampleid

    def __str__(self):
        """Return string representation of the Sample object.

        Returns:
            str: String representation.
        """
        return f"Sample(id={self.sampleid}, name='{self.samplename}', analysisunitid={self.analysisunitid})"
