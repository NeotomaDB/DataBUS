class Sample:
    def __init__(
        self,
        analysisunitid=None,
        datasetid=None,
        samplename=None,
        sampledate=None,
        analysisdate=None,
        taxonid=None,
        labnumber=None,
        preparationmethod=None,
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
        self.preparationmethod = preparationmethod
        self.notes = notes

    def insert_to_db(self, cur):
        sample_q = """
        SELECT ts.insertsample(_analysisunitid := %(analysisunitid)s,
                               _datasetid := %(datasetid)s,
                               _samplename := %(samplename)s,
                               _sampledate := %(sampledate)s,
                               _analysisdate := %(analysisdate)s,
                               _taxonid := %(taxonid)s,
                               _labnumber := %(labnumber)s,
                               _preparationmethod := %(preparationmethod)s,
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
            "preparationmethod": self.preparationmethod,
            "notes": self.notes,
        }

        cur.execute(sample_q, inputs)
        self.sampleid = cur.fetchone()[0]
        return self.sampleid

    def __str__(self):
        pass
