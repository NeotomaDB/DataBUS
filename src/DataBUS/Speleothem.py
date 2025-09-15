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
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_entityrelationship.sql") as sql_file:
    insert_entityrelationship = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_entitydripheight.sql") as sql_file:
    insert_entitydripheight = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_entitycovers.sql") as sql_file:
    insert_entitycovers = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_entitylandusecover.sql") as sql_file:
    insert_entitylandusecover = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_entityvegetationcover.sql") as sql_file:
    insert_entityvegetationcover = sql_file.read()   

class Speleothem:
    def __init__(
        self,
        siteid=None,
        entityid=None,
        entityname=None,
        monitoring=None,
        rockageid=None,
        entrancedistance=None,
        entrancedistanceunits=None,
        speleothemtypeid=None):
        self.siteid = siteid
        self.entityid = entityid
        self.entityname = entityname
        self.monitoring = monitoring
        self.rockageid=rockageid
        self.entrancedistance=entrancedistance
        self.entrancedistanceunits=entrancedistanceunits
        self.speleothemtypeid=speleothemtypeid
    
    def __str__(self):
        statement = (
            f"SiteID: {self.siteid}, Entity: {self.entityid}")
        return statement

    def insert_to_db(self, cur):
        cur.execute(insert_speleothem)
        query = """
        SELECT insert_speleothem(_siteid := %(siteid)s,
                                _entityid := %(entityid)s,
                                _entityname := %(entityname)s,
                                _monitoring := %(monitoring)s,
                                _rockageid := %(rockageid)s,
                                _entrancedistance := %(entrancedistance)s,
                                _entrancedistanceunits := %(entrancedistanceunits)s,
                                _speleothemtypeid := %(speleothemtypeid)s)
                                """
        inputs = {
            "siteid": self.siteid,
            "entityid": self.entityid,
            "entityname": self.entityname,
            "monitoring": self.monitoring,
            "rockageid": self.rockageid,
            "entrancedistance": self.entrancedistance,
            "entrancedistanceunits": self.entrancedistanceunits,\
            "speleothemtypeid": self.speleothemtypeid}
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
                  "notes": notes}
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
    
    def insert_entityrelationship_to_db(self, cur, id, entitystatusid, referenceentityid):
        cur.execute(insert_entityrelationship)
        query = """
                SELECT insert_entityrelationship(_entityid := %(entityid)s,
                                                 _entitystatusid := %(entitystatusid)s,
                                                 _referenceentityid := %(referenceentityid)s)
                """
        inputs = {"entityid": id,
                  "entitystatusid": entitystatusid,
                  "referenceentityid": referenceentityid}
        cur.execute(query, inputs)
        return
    
    def insert_entitydripheight_to_db(self, cur, id, 
                                      speleothemdriptypeid, 
                                      entitydripheight, 
                                      entitydripheightunit):
        cur.execute(insert_entitydripheight)
        query = """
                SELECT insert_entitydripheight(_entityid := %(entityid)s,
                                               _speleothemdriptypeid := %(speleothemdriptypeid)s,
                                               _entitydripheight := %(entitydripheight)s,
                                               _entitydripheightunit := %(entitydripheightunit)s)
                """
        inputs = {"entityid": id,
                  "speleothemdriptypeid": speleothemdriptypeid,
                  "entitydripheight": entitydripheight,
                  "entitydripheightunit": entitydripheightunit}
        cur.execute(query, inputs)
        return
    
    def insert_entitycovers_to_db(self, cur, id, 
                                  entitycoverid, 
                                  entitycoverthickness, 
                                  entitycoverunits):
        cur.execute(insert_entitycovers)
        query = """
                SELECT insert_entitycovers(_entityid := %(entityid)s,
                                           _entitycoverid := %(entitycoverid)s,
                                           _entitycoverthickness := %(entitycoverthickness)s,
                                           _entitycoverunits := %(entitycoverunits)s)
                """
        inputs = {"entityid": id,
                  "entitycoverid": entitycoverid,
                  "entitycoverthickness": entitycoverthickness,
                  "entitycoverunits": entitycoverunits}
        cur.execute(query, inputs)
        return
    
    def insert_entitylandusecovers_to_db(self, cur, id, 
                                        landusecovertypeid, 
                                        landusecoverpercent, 
                                        landusecovernotes):
        cur.execute(insert_entitylandusecover)
        query = """
                SELECT insert_entitylandusecover(_entityid := %(entityid)s,
                                                 _landusecovertypeid := %(landusecovertypeid)s,
                                                 _landusecoverpercent := %(landusecoverpercent)s,
                                                 _landusecovernotes := %(landusecovernotes)s)
                """
        inputs = {"entityid": id,
                  "landusecovertypeid": landusecovertypeid,
                  "landusecoverpercent": landusecoverpercent,
                  "landusecovernotes": landusecovernotes}
        cur.execute(query, inputs)
        return
    
    def insert_entityvegetationcovers_to_db(self, cur, id, 
                                        vegetationcovertypeid, 
                                        vegetationcoverpercent, 
                                        vegetationcovernotes):
        cur.execute(insert_entityvegetationcover)
        query = """
                SELECT insert_entityvegetationcover(_entityid := %(entityid)s,
                                                    _vegetationcovertypeid := %(vegetationcovertypeid)s,
                                                    _vegetationcoverpercent := %(vegetationcoverpercent)s,
                                                    _vegetationcovernotes := %(vegetationcovernotes)s)
                """
        inputs = {"entityid": id,
                  "vegetationcovertypeid": vegetationcovertypeid,
                  "vegetationcoverpercent": vegetationcoverpercent,
                  "vegetationcovernotes": vegetationcovernotes}
        cur.execute(query, inputs)
        return