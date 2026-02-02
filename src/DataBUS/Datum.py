class Datum:
    """A data point measurement in the Neotoma database.

    Encapsulates a single measurement or observation linking a sample
    to a variable with a measured value.

    Data are explained further in the
    [Neotoma Manual]https://open.neotomadb.org/manual/sample-related-tables-1.html#Data)

    Attributes:
        sampleid (int | None): Sample identifier.
        variableid (int | None): Variable identifier.
        value (float | None): The measured value.
        datumid (int | None): Database ID (assigned after insertion).

    Examples:
        >>> datum = Datum(sampleid=1, variableid=42, value=125.3)
        >>> datum.value
        125.3
    """

    def __init__(self, sampleid=None, variableid=None, value=None):
        self.sampleid = sampleid
        self.variableid = variableid
        self.value = value

    def insert_to_db(self, cur):
        """Insert the datum record into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The datumid assigned by the database.
        """
        datum_q = """
                 SELECT ts.insertdata(_sampleid := %(sampleid)s,
                                      _variableid := %(variableid)s,
                                      _value := %(value)s)
                 """
        inputs = {
            "sampleid": self.sampleid,
            "variableid": self.variableid,
            "value": self.value,
        }
        cur.execute(datum_q, inputs)
        self.datumid = cur.fetchone()[0]
        return self.datumid

    def __str__(self):
        """Return string representation of the Datum object.

        Returns:
            str: String representation.
        """
        return f"Datum(sampleid={self.sampleid}, variableid={self.variableid}, value={self.value})"