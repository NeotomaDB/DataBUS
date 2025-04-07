class GeochronControl:
    def __init__(self, chroncontrolid, geochronid):
        self.chroncontrolid = chroncontrolid
        self.geochronid = geochronid
        self.geochroncontrolid = None

    def insert_to_db(self, cur):
        geochroncontrol_q = """
                  SELECT ts.insertgeochroncontrol(%(chroncontrolid)s, 
                                                  %(geochronid)s)
                 """
        inputs = {
            'chroncontrolid': self.chroncontrolid,
            'geochronid': self.geochronid
        }

        cur.execute(geochroncontrol_q, inputs)
        self.geochroncontrolid = cur.fetchone()[0]
        return self.geochroncontrolid

    def __str__(self):
        pass