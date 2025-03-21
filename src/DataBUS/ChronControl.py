class ChronControl:
    def __init__(self,
                 chroncontrolid=None,
                chronologyid=None,
                chroncontroltypeid=None,
                depth=None,
                thickness=None,
                age=None,
                agelimityounger=None,
                agelimitolder=None,
                notes=None,
                analysisunitid=None,
                agetypeid=None):
        self.chroncontrolid = chroncontrolid
        self.chronologyid = chronologyid
        self.chroncontroltypeid = chroncontroltypeid
        self.depth = depth
        self.thickness = thickness
        self.age = age
        self.agelimityounger = agelimityounger
        self.agelimitolder = agelimitolder
        self.notes = notes
        self.analysisunitid = analysisunitid
        self.agetypeid = agetypeid

    def insert_to_db(self, cur):
        chroncon_query = """
        SELECT ts.insertchroncontrol(_chronologyid := %(chronologyid)s,
                                     _chroncontroltypeid := 10,
                                     _analysisunitid := %(analysisunitid)s,
                                     _depth := %(depth)s,
                                     _thickness := %(thickness)s,
                                     _agetypeid := %(agetypeid)s,
                                     _age := %(age)s,
                                     _agelimityounger := %(agelimityounger)s,
                                     _agelimitolder := %(agelimitolder)s,
                                     _notes := %(notes)s)
                        """
        inputs = {
            "chronologyid": self.chronologyid,
            "analysisunitid": self.analysisunitid,
            "chroncontroltypeid": self.chroncontroltypeid,  # placeholder
            "depth": self.depth,
            "thickness": self.thickness,
            "agetypeid": self.agetypeid,
            "age": self.age,
            "agelimityounger": self.agelimityounger,
            "agelimitolder": self.agelimitolder,
            "notes": self.notes,
        }
        cur.execute(chroncon_query, inputs)
        self.chroncontrolid = cur.fetchone()[0]
        return self.chroncontrolid

    def __str__(self):
        pass
