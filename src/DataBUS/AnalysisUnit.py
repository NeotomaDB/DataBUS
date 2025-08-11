class AnalysisUnit:
    """_A class for Neotoma Analysis Units_
    
    Neotoma defines analysis units as physical subsets of the collection unit. An analysis unit
    will often ave a position within a core, or dig site associated with depth, and may have
    one or more samples associated with it, where the sample is the intersection between the
    analysis unit and the dataset type. Analysis units are explained further in the
    [Neotoma Manual](https://open.neotomadb.org/manual/sample-related-tables-1.html#AnalysisUnits).

    Args:
        collectionunitid (int): The unique identifier assigned to the collectionunit to which the analysis unit belongs.
        analysisunitname (str, None): The name assigned to the analysis unit.
        depth (float, None): The depth of the sample (if known).
        thickness (float, None): The thickness of the physical sample. Some objects may not have thickness assigned, for example Water Chemistry samples, or bone records.
        faciesid (int, None): When a sample is obtained from a sample that is also described by exposure, outcrop or rock formation, the Neotoma identifier for that formation. 
        mixed (bool, None): Is there evidence of stratigraphic mixing within the sample.
        igsn (str, None): Support for the use of IGSN identifiers.
        notes (str, None): Notes associated with the analysis unit.
        recdatecreated (datetime, None): The time the record was created in the Neotoma Database (for records already in Neotoma).
        recdatemodified (datetime, None): Date and time of the last modification within Neotoma (for records already in Neotoma).
    Returns:
        _type_: _description_
    """    
    description = "Analysis Unit object in Neotoma"

    def __init__(
        self,
        collectionunitid=None,
        analysisunitname=None,
        depth=None,
        thickness=None,
        faciesid=None,
        mixed=None,
        igsn=None,
        notes=None,
        recdatecreated=None,
        recdatemodified=None):
        
        self.analysisunitid = []  # int
        self.collectionunitid = collectionunitid  # int
        self.analysisunitname = analysisunitname  # str
        self.depth = depth  # float
        self.thickness = thickness  # float
        self.faciesid = faciesid  # int
        self.mixed = mixed  # bool
        self.igsn = igsn  # str
        self.notes = notes  # str
        self.recdatecreated = recdatecreated  # date
        self.recdatemodified = recdatemodified  # date

    def __str__(self):
        """_Print AnalysisUnit_

        Returns:
            _str_: _A printed output summarizing the analysis unit._
        """        
        statement = f"Name: {self.analysisunitname}, " f"ID: {self.analysisunitid}, "
        return statement

    def insert_to_db(self, cur):
        """_Insert the AnalysisUnit to Neotoma._

        Args:
            cur (_psycopg2.connect_): _A valid psycopg2 connection the the Neotoma Database._

        Returns:
            _AnalysisUnit_: _The function inserts the AnalysisUnit to Neotoma and adds the new `analysisunitid` to the object._
        """        
        au_query = """SELECT ts.insertanalysisunit(_collectionunitid := %(collunitid)s,
                                                   _depth := %(depth)s,
                                                   _thickness := %(thickness)s,
                                                   _faciesid := %(faciesid)s,
                                                   _mixed := %(mixed)s,
                                                   _igsn := %(igsn)s,
                                                   _notes := %(notes)s)"""
        inputs = {
            "collunitid": self.collectionunitid,
            "depth": self.depth,
            "thickness": self.thickness,
            "faciesid": self.faciesid,
            "mixed": self.mixed,
            "igsn": self.igsn,
            "notes": self.notes,
        }
        cur.execute(au_query, inputs)
        self.analysisunitid = cur.fetchone()[0]
        return self.analysisunitid