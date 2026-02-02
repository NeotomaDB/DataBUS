ANALYSIS_UNIT_PARAMS = ["analysisunitname", "depth", "thickness",
                        "faciesid", "mixed", "igsn", "notes"]
class AnalysisUnit:
    """An analysis unit in Neotoma.

    Physical subsets of a collection unit, often with a position (depth)
    within a core or dig site. Samples are the intersection between
    analysis units and dataset types.
    
    See the [Neotoma Manual](https://open.neotomadb.org/manual/sample-related-tables-1.html#AnalysisUnits).

    Attributes:
        analysisunitid (int | None): Analysis unit ID (assigned after insertion).
        collectionunitid (int | None): Parent collection unit ID.
        analysisunitname (str | None): Name of the analysis unit.
        depth (float | None): Depth in the core/site (if known).
        thickness (float | None): Physical thickness of the sample.
        faciesid (int | None): Neotoma identifier for rock formation.
        mixed (bool | None): Evidence of stratigraphic mixing.
        igsn (str | None): IGSN identifier.
        notes (str | None): Additional notes.

    Examples:
        >>> au = AnalysisUnit(collectionunitid=1, depth=2.5)
        >>> au.depth
        2.5
    """

    def __init__(
        self,
        analysisunitid=None,
        collectionunitid=None,
        analysisunitname=None,
        depth=None,
        thickness=None,
        faciesid=None,
        mixed=None,
        igsn=None,
        notes=None):
        
        self.analysisunitid = analysisunitid
        self.collectionunitid = collectionunitid
        self.analysisunitname = analysisunitname
        self.depth = depth
        self.thickness = thickness
        self.faciesid = faciesid 
        self.mixed = mixed
        self.igsn = igsn
        self.notes = notes

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