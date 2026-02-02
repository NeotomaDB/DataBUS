class Variable:
    """A variable (taxon or measurement) in Neotoma.

    Defines what is being measured in paleoenvironmental data, including
    taxon, element, units, and context. Can be taxonomic (species) or physical.
    
    See the [Neotoma Manual](https://open.neotomadb.org/manual/database-design-concepts.html#taxa-and-variables)
    
    Attributes:
        varid (int | None): Variable ID (assigned after DB lookup/insert).
        taxonid (int | None): Taxon ID.
        variableelementid (int | None): Variable element ID.
        variableunitsid (int | None): Variable units ID.
        variablecontextid (int | None): Variable context ID.

    Examples:
        >>> variable = Variable(taxonid=42, variableunitsid=3)
        >>> variable.taxonid
        42
    """

    def __init__(
        self,
        taxonid=None,
        variableelementid=None,
        variableunitsid=None,
        variablecontextid=None,
    ):
        self.varid = None
        self.taxonid = taxonid
        self.variableelementid = variableelementid
        self.variableunitsid = variableunitsid
        self.variablecontextid = variablecontextid

    def insert_to_db(self, cur):
        """Insert the variable record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The variableid assigned by the database.
        """
        variable_q = """
                 SELECT ts.insertvariable(_taxonid := %(taxonid)s,
                                          _variableelementid := %(variableelementid)s,
                                          _variableunitsid := %(variableunitsid)s,
                                          _variablecontextid := %(variablecontextid)s)
                """
        inputs = {
            "taxonid": self.taxonid,
            "variableelementid": self.variableelementid,
            "variableunitsid": self.variableunitsid,
            "variablecontextid": self.variablecontextid,
        }
        cur.execute(variable_q, inputs)
        self.variableid = cur.fetchone()[0]
        return self.variableid

    def get_id_from_db(self, cur):
        """Retrieve variable ID from the database based on attributes.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int, None: The variableid or None if not found.
        """
        variable_q = """
                 SELECT variableid
                FROM ndb.variables
                WHERE taxonid = %(taxonid)s
                    AND variableunitsid IS NOT DISTINCT FROM %(variableunitsid)s
                    AND variableelementid IS NOT DISTINCT FROM %(variableelementid)s
                    AND variablecontextid IS NOT DISTINCT FROM %(variablecontextid)s;
                """
        inputs = {
            "variableunitsid": self.variableunitsid,
            "taxonid": self.taxonid,
            "variableelementid": self.variableelementid,
            "variablecontextid": self.variablecontextid,
        }
        cur.execute(variable_q, inputs)
        self.varid = cur.fetchone()
        return self.varid

    def __str__(self):
        """Return string representation of the Variable object.

        Returns:
            str: String representation.
        """
        return f"Variable(varid={self.varid}, taxonid={self.taxonid}, elementid={self.variableelementid}, unitsid={self.variableunitsid})"