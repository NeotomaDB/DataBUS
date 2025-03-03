class Datum:
    def __init__(self, sampleid=None, variableid=None, value=None):
        self.sampleid = sampleid
        self.variableid = variableid
        self.value = value

    def insert_to_db(self, cur):
        datum_q = """
                 SELECT ts.insertdata(_sampleid := %(sampleid)s,
                                      _variableid := %(variableid)s,
                                      _value := %(value)s)
                 """
        inputs = {
            "sampleid": self.sampleid,
            "variableid": self.variableid,
            "value": self.value,
        }

        cur.execute(datum_q, inputs)
        self.datumid = cur.fetchone()[0]
        return self.datumid

    def __str__(self):
        pass