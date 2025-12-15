class Variable:
    def __init__(
        self,
        taxonid=None,
        variableelementid=None,
        variableunitsid=None,
        variablecontextid=None,
    ):
        self.varid = None
        self.taxonid = taxonid
        self.variableelementid = variableelementid
        self.variableunitsid = variableunitsid
        self.variablecontextid = variablecontextid

    def insert_to_db(self, cur):
        variable_q = """
                 SELECT ts.insertvariable(_taxonid := %(taxonid)s,
                                          _variableelementid := %(variableelementid)s,
                                          _variableunitsid := %(variableunitsid)s,
                                          _variablecontextid := %(variablecontextid)s)
                """
        inputs = {
            "taxonid": self.taxonid,
            "variableelementid": self.variableelementid,
            "variableunitsid": self.variableunitsid,
            "variablecontextid": self.variablecontextid,
        }

        cur.execute(variable_q, inputs)
        self.datumid = cur.fetchone()[0]
        return self.datumid

    def get_id_from_db(self, cur):
        variable_q = """
                 SELECT variableid FROM ndb.variables 
                           WHERE (variableunitsid IS NULL OR variableunitsid = %(variableunitsid)s)
                           AND taxonid = %(taxonid)s
                           AND (variableelementid IS NULL OR variableelementid = %(variableelementid)s)
                           AND (variablecontextid IS NULL OR variablecontextid = %(variablecontextid)s)
                """
        inputs = {
            "variableunitsid": self.variableunitsid,
            "taxonid": self.taxonid,
            "variableelementid": self.variableelementid,
            "variablecontextid": self.variablecontextid,
        }
        cur.execute(variable_q, inputs)
        self.varid = cur.fetchone()
        return self.varid

    def __str__(self):
        pass
