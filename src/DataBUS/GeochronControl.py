class GeochronControl:
    """A link between a chronological control and geochronological date.

    Links a chronological control point with
    a geochronological age determination used to constrain the age model.

    Geochronologies are explained further in the
    [Neotoma Manual](https://open.neotomadb.org/manual/chronology-age-related-tables-1.html#Geochronology)
    
    Attributes:
        chroncontrolid (int): Chrono control ID.
        geochronid (int): Geochron ID.
        geochroncontrolid (int | None): Geochron control ID (assigned after insertion).

    Examples:
        >>> gc = GeochronControl(chroncontrolid=1, geochronid=2)
        >>> gc.chroncontrolid
        1
    """

    def __init__(self, chroncontrolid, geochronid):
        self.chroncontrolid = chroncontrolid
        self.geochronid = geochronid
        self.geochroncontrolid = None

    def insert_to_db(self, cur):
        """Insert the geochron-control relationship into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The geochroncontrolid assigned by the database.
        """
        # SUGGESTION: Consider adding parameter names to SQL function call for clarity
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
        """Return string representation of the GeochronControl object.

        Returns:
            str: String representation.
        """
        return f"GeochronControl(id={self.geochroncontrolid}, chroncontrolid={self.chroncontrolid}, geochronid={self.geochronid})"