import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_speleothem.sql") as sql_file:
    insert_speleothem = sql_file.read()
    
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
        speleothemtypeid=None,
        entitystatusid=None,
        speleothemgeologyid=None,
        speleothemdriptypeid=None):

        self.siteid = siteid
        self.entityid=entityid
        self.entityname=entityname
        self.monitoring=monitoring
        self.rockageid=rockageid
        self.entrancedistance=entrancedistance
        self.entrancedistanceunits=entrancedistanceunits
        self.speleothemtypeid=speleothemtypeid
        self.entitystatusid=entitystatusid
        self.speleothemgeologyid=speleothemgeologyid
        self.speleothemdriptypeid=speleothemdriptypeid
    
    def insert_to_db(self, cur):
        cur.execute(insert_speleothem)
        query = """
        SELECT insertspeleothem(_siteid := %(siteid)s,
                                _entityid := %(entityid)s,
                                _entityname := %(entityname)s,
                                _monitoring := %(monitoring)s,
                                _rockageid := %(rockageid)s,
                                _entrancedistance := %(entrancedistance)s,
                                _entrancedistanceunits := %(entrancedistanceunits)s,
                                _speleothemtypeid := %(speleothemtypeid)s,
                                _entitystatusid := %(entitystatusid)s,
                                _speleothemgeologyid := %(speleothemgeologyid)s,
                                _speleothemdriptypeid := %(speleothemdriptypeid)s)
                        """
        inputs = {
            "siteid": self.siteid,
            "entityid": self.entityid,
            "entityname": self.entityname,
            "monitoring": self.monitoring,
            "rockageid": self.rockageid,
            "entrancedistance": self.entrancedistance,
            "entrancedistanceunits": self.entrancedistanceunits,
            "speleothemtype": self.speleothemtype,
            "entitystatusid": self.entitystatusid,
            "speleothemgeologyid": self.speleothemgeologyid,
            "speleothemdriptypeid": self.speleothemdriptypeid}
        cur.execute(query, inputs)
        return

    def __str__(self):
        pass