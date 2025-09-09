import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_speleothem.sql") as sql_file:
    insert_speleothem = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_speleothem_cu.sql") as sql_file:
    insert_speleothem_cu = sql_file.read()

with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                "insert_entitygeology.sql") as sql_file:
    insert_entitygeology = sql_file.read()
    
class Speleothem:
    def __init__(
        self,
        siteid=None,
        entityname=None,
        monitoring=None,
        rockageid=None,
        entrancedistance=None,
        entrancedistanceunits=None,
        speleothemtypeid=None,
        entitystatusid=None,
        speleothemdriptypeid=None):
        self.siteid = siteid
        self.entityname = entityname
        self.monitoring = monitoring
        self.rockageid=rockageid
        self.entrancedistance=entrancedistance
        self.entrancedistanceunits=entrancedistanceunits
        self.speleothemtypeid=speleothemtypeid
        self.entitystatusid=entitystatusid
        self.speleothemdriptypeid=speleothemdriptypeid
    
    def __str__(self):
        statement = (
            f"SiteID: {self.siteid}, Entity: {self.entityid}")
        return statement

    def insert_to_db(self, cur):
        cur.execute(insert_speleothem)
        query = """
        SELECT insert_speleothem(_siteid := %(siteid)s,
                                _entityname := %(entityname)s,
                                _monitoring := %(monitoring)s,
                                _rockageid := %(rockageid)s,
                                _entrancedistance := %(entrancedistance)s,
                                _entrancedistanceunits := %(entrancedistanceunits)s,
                                _speleothemtypeid := %(speleothemtypeid)s)
                                """
        #                         _entitystatusid := %(entitystatusid)s,
        #                         _speleothemdriptypeid := %(speleothemdriptypeid)s)
        inputs = {
            "siteid": self.siteid,
            "entityname": self.entityname,
            "monitoring": self.monitoring,
            "rockageid": self.rockageid,
            "entrancedistance": self.entrancedistance,
            "entrancedistanceunits": self.entrancedistanceunits,\
            "speleothemtypeid": self.speleothemtypeid}
            # "entitystatusid": self.entitystatusid,
            # "speleothemdriptypeid": self.speleothemdriptypeid}
        cur.execute(query, inputs)
        return cur.fetchone()[0]
    
    def insert_entitygeology_to_db(self, cur, id, speleothemgeologyid, notes):
        cur.execute(insert_entitygeology)
        query = """
        SELECT insert_entitygeology(_entityid := %(entityid)s,
                                    _speleothemgeologyid := %(speleothemgeologyid)s,
                                    _notes := %(notes)s)
                                """
        inputs = {"entityid": id,
            "speleothemgeologyid": speleothemgeologyid,
            "notes": None}
        cur.execute(query, inputs)
        return
    
    def insert_cu_speleothem_to_db(self, cur, id, cuid):
        cur.execute(insert_speleothem_cu)
        query = """
        SELECT insert_speleothem_cu(_entityid := %(entityid)s,
                                   _collectionunitid := %(collectionunitid)s)
                                """
        inputs = {"entityid": id,
                  "collectionunitid": cuid}
        cur.execute(query, inputs)
        return