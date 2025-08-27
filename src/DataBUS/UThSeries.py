import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_uthseries.sql") as sql_file:
    insert_uthseries = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_uraniumseriesdata.sql") as sql_file:
    insert_uraniumseriesdata = sql_file.read()
    
class UThSeries:
    description = "UThSeries object in Neotoma"

    def __init__(
        self,
        geochronid=None,
        decayconstantid=None,
        ratio230th232th=None,
        ratiouncertainty230th232th=None,
        activity230th238u=None,
        activityuncertainty230th238u=None,
        activity234u238u=None,
        activityuncertainty234u238u=None,
        iniratio230th232th=None,
        iniratiouncertainty230th232th=None):
        self.geochronid = geochronid
        self.decayconstantid = decayconstantid
        self.ratio230th232th = ratio230th232th
        self.ratiouncertainty230th232th = ratiouncertainty230th232th
        self.activity230th238u = activity230th238u
        self.activityuncertainty230th238u = activityuncertainty230th238u
        self.activity234u238u = activity234u238u
        self.activityuncertainty234u238u = activityuncertainty234u238u
        self.iniratio230th232th = iniratio230th232th
        self.iniratiouncertainty230th232th = iniratiouncertainty230th232th

    def insert_to_db(self, cur):
        cur.execute(insert_uthseries) 
        uths_query = """SELECT insert_uthseries(_geochronid := %(geochronid)s,
                                                    _decayconstantid := %(decayconstantid)s,
                                                    _ratio230th232th := %(ratio230th232th)s,
                                                    _ratiouncertainty230th232th := %(ratiouncertainty230th232th)s,
                                                    _activity230th238u := %(activity230th238u)s,
                                                    _activityuncertainty230th238u := %(activityuncertainty230th238u)s,
                                                    _activity234u238u := %(activity234u238u)s,
                                                    _activityuncertainty234u238u := %(activityuncertainty234u238u)s,
                                                    _iniratio230th232th := %(iniratio230th232th)s,
                                                    _iniratiouncertainty230th232th := %(iniratiouncertainty230th232th)s);"""
        inputs = {
            'geochronid': self.geochronid,
            'decayconstantid': self.decayconstantid,
            'ratio230th232th': self.ratio230th232th,
            'ratiouncertainty230th232th': self.ratiouncertainty230th232th,
            'activity230th238u': self.activity230th238u,
            'activityuncertainty230th238u': self.activityuncertainty230th238u,
            'activity234u238u': self.activity234u238u,
            'activityuncertainty234u238u': self.activityuncertainty234u238u,
            'iniratio230th232th': self.iniratio230th232th,
            'iniratiouncertainty230th232th': self.iniratiouncertainty230th232th
        }
        cur.execute(uths_query, inputs)
        return
    
    def insert_uraniumseriesdata(self, dataid, cur):
        cur.execute(insert_uraniumseriesdata) 
        uths_query = """SELECT insert_uraniumseriesdata(_geochronid := %(geochronid)s,
                                                _dataid := %(dataid)s)"""
        inputs = {
            'geochronid': self.geochronid,
            'dataid': dataid
        }
        cur.execute(uths_query, inputs)
        return