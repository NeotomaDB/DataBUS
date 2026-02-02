import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers",
                                   "insert_speleothem.sql") as sql_file:
    insert_speleothem = sql_file.read()
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
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_externalspeleothem.sql") as sql_file:
    insert_externalspeleothem = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_entitysamples.sql") as sql_file:
    insert_entitysamples = sql_file.read()

EX_SP_PARAMS = ['externalid', 'externaldescription', 'extdatabaseid']
SPELEOTHEM_PARAMS = ['entityid', 'entityname', 'monitoring', 'rockageid', 'entrancedistance', 
              'entrancedistanceunitsid', 'speleothemtypeid', 'entitystatusid', 'speleothemgeologyid',
              'speleothemdriptypeid', 'dripheight', 'dripheightunitsid', 'covertypeid', 'coverthickness',
              'entitycoverunitsid', 'landusecovertypeid', 'landusecoverpercent', 'landusecovernotes',
              'vegetationcovertypeid', 'vegetationcoverpercent', 'vegetationcovernotes', 'ref_id',
              'organics', 'mineralogypetrologyfabric', 'clumpedisotopes', 'fluidinclusions', 
              'noblegastemperatures', 'c14', 'odl']
class Speleothem:
    """Represents a speleothem (stalactite, stalagmite, flowstone, etc.) in a cave in Neotoma.

    This class manages information about cave mineral deposits that may be sampled
    for paleoenvironmental reconstruction through isotopic and geochemical analysis.

    Attributes:
        siteid (int | None): Site ID.
        entityid (int | None): Entity ID.
        entityname (str | None): Name.
        monitoring (bool | None): Monitoring flag.
        rockageid (int | None): Rock age ID.
        entrancedistance (float | None): Distance from entrance.
        entrancedistanceunits (int | None): Distance units.
        speleothemtypeid (int | None): Speleothem type ID.

    Examples:
        >>> spel = Speleothem(siteid=1, entityname="Palace Chandelier", speleothemtypeid=1)  # Stalactite from Lehman Cave
        >>> spel.entityname
        'Palace Chandelier'
        >>> spel = Speleothem(siteid=2, entityname="Main Stalagmite", speleothemtypeid=2, entrancedistance=45.5)
        >>> spel.entrancedistance
        45.5
    """

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
        """Return string representation of the Speleothem object.

        Returns:
            str: String representation.
        """
        statement = (
            f"SiteID: {self.siteid}, Entity: {self.entityid}")
        return statement

    def insert_to_db(self, cur):
        """Insert the speleothem record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor.

        Returns:
            int: The speleothem ID assigned.
        """
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
        inputs = {
            "siteid": self.siteid,
            "entityname": self.entityname,
            "monitoring": self.monitoring,
            "rockageid": self.rockageid,
            "entrancedistance": self.entrancedistance,
            "entrancedistanceunits": self.entrancedistanceunits,
            "speleothemtypeid": self.speleothemtypeid}
        cur.execute(query, inputs)
        spid = cur.fetchone()[0]
        return spid
    
    def insert_entitygeology_to_db(self, cur, id, speleothemgeologyid, notes):
        """Insert speleothem geology information into the database.

        Args:
            cur (psycopg2.cursor): Database cursor.
            id (int): Entity identifier.
            speleothemgeologyid (int): Geology type ID.
            notes (str | None): Notes.

        Returns:
            None
        """
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
    
    def insert_entitydripheight_to_db(self, cur, id,
                                      speleothemdriptypeid,
                                      entitydripheight,
                                      entitydripheightunit):
        """Insert drip rate information for a speleothem.

        Args:
            cur (psycopg2.cursor): Database cursor.
            id (int): Entity identifier.
            speleothemdriptypeid (int): Drip type ID.
            entitydripheight (float): Drip height measurement.
            entitydripheightunit (int): Unit ID for height.

        Returns:
            None
        """
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
        """Insert cover information for a speleothem.

        Args:
            cur (psycopg2.cursor): Database cursor.
            id (int): Entity identifier.
            entitycoverid (int): Cover type ID.
            entitycoverthickness (float): Cover thickness.
            entitycoverunits (int): Unit ID for thickness.

        Returns:
            None
        """
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
        """Insert land use cover information for a speleothem.

        Args:
            cur (psycopg2.cursor): Database cursor.
            id (int): Entity identifier.
            landusecovertypeid (int): Land use type ID.
            landusecoverpercent (float): Percentage coverage.
            landusecovernotes (str | None): Notes.

        Returns:
            None
        """
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
        """Insert vegetation cover information for a speleothem.

        Args:
            cur (psycopg2.cursor): Database cursor.
            id (int): Entity identifier.
            vegetationcovertypeid (int): Vegetation type ID.
            vegetationcoverpercent (float): Percentage coverage.
            vegetationcovernotes (str | None): Notes.

        Returns:
            None
        """
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
    
    def insert_entitysamples_to_db(self, cur, id, organics, fluid_inclusions, mineralogy_petrology_fabric,
                              clumped_isotopes, noble_gas_temperatures, C14, ODL):
        """Insert sample type information for a speleothem.

        Args:
            cur (psycopg2.cursor): Database cursor.
            id (int): Entity identifier.
            organics (bool | None): Organic material present.
            fluid_inclusions (bool | None): Fluid inclusions present.
            mineralogy_petrology_fabric (bool | None): Mineralogy data available.
            clumped_isotopes (bool | None): Clumped isotope data available.
            noble_gas_temperatures (bool | None): Noble gas temperature data.
            C14 (bool | None): Radiocarbon data available.
            ODL (bool | None): Optical dating available.

        Returns:
            None
        """
        cur.execute(insert_entitysamples)
        def to_bool(x):
            if isinstance(x, str):
                return x.lower() == "yes"
            return None
        query = """
                SELECT insert_entitysamples(_entityid := %(entityid)s,
                                            _organics := %(organics)s,
                                            _fluid_inclusions := %(fluid_inclusions)s,
                                            _mineralogy_petrology_fabric := %(mineralogy_petrology_fabric)s,
                                            _noble_gas_temperatures := %(noble_gas_temperatures)s,
                                            _clumped_isotopes := %(clumped_isotopes)s,
                                            _C14 := %(C14)s,
                                            _ODL := %(ODL)s)
                """
        inputs = {"entityid": int(id),
                    "organics": to_bool(organics),
                    "fluid_inclusions": to_bool(fluid_inclusions),
                    "mineralogy_petrology_fabric": to_bool(mineralogy_petrology_fabric),
                    "noble_gas_temperatures": to_bool(noble_gas_temperatures),
                    "clumped_isotopes": to_bool(clumped_isotopes),
                    "C14": to_bool(C14),
                    "ODL": to_bool(ODL)}
        cur.execute(query, inputs)

class ExternalSpeleothem:
    """Represents an external reference to a speleothem entity.

    This class manages references to speleothem entities in external databases
    or from other research groups.

    Attributes:
        entityid (int | None): Local entity identifier.
        externalid (int | None): External entity identifier.
        extdatabaseid (int | None): External database ID.
        externaldescription (str | None): Description of external reference.
    
    Examples:
        >>> ext = ExternalSpeleothem(entityid=1, externalid="PALEODB-2847")  # Reference to external database
        >>> ext.entityid
        1
        >>> ext = ExternalSpeleothem(entityid=5, externalid="GEOMARC-156", extdatabaseid=3)
        >>> ext.externalid
        'GEOMARC-156'
    """

    def __init__(
        self,
        entityid=None,
        externalid=None,
        extdatabaseid=None,
        externaldescription=None):
        self.entityid = entityid
        self.externalid = externalid
        self.extdatabaseid = extdatabaseid
        self.externaldescription=externaldescription

    def __str__(self):
        """Return string representation of ExternalSpeleothem.

        Returns:
            str: String representation.
        """
        statement = (
            f"Entity: {self.entityid}, External Entity: {self.externalid}")
        return statement

    def insert_externalspeleothem_to_db(self, cur):
        """Insert external speleothem reference into database.

        Args:
            cur (psycopg2.cursor): Database cursor.

        Returns:
            None
        """
        cur.execute(insert_externalspeleothem)
        query = """
                SELECT insert_externalspeleothem(_entityid := %(entityid)s,
                                                    _externalid := %(externalid)s,
                                                    _extdatabaseid := %(extdatabaseid)s,
                                                    _externaldescription := %(externaldescription)s)
                """
        inputs = {"entityid": self.entityid,
                    "externalid": self.externalid,
                    "extdatabaseid": self.extdatabaseid,
                    "externaldescription": self.externaldescription}
        cur.execute(query, inputs)
        return