import importlib.resources

with importlib.resources.open_text(
    "DataBUS.sqlHelpers", "insert_data_uncertainty.sql"
) as sql_file:
    insert_data_uncertainty = sql_file.read()

class DataUncertainty:
    """Measurement uncertainty for a data value in Neotoma.

    Stores uncertainty metrics including magnitude, units, and basis
    of uncertainty quantification.

    Attributes:
        dataid (int): Data ID.
        uncertaintyvalue (float | None): Uncertainty magnitude.
        uncertaintyunitid (int | None): Uncertainty units ID.
        uncertaintybasisid (int | None): Uncertainty basis ID.
        notes (str | None): Notes about uncertainty.

    Examples:
        >>> uncert = DataUncertainty(dataid=1, uncertaintyvalue=5.0,
        ...                          uncertaintyunitid=2, uncertaintybasisid=1, notes=None)
        >>> uncert.uncertaintyvalue
        5.0
    """

    def __init__(
        self, dataid, uncertaintyvalue, uncertaintyunitid, uncertaintybasisid, notes
    ):
        self.dataid = dataid
        if uncertaintyvalue == "NA":
            uncertaintyvalue = None
        self.uncertaintyvalue = uncertaintyvalue
        if uncertaintyunitid == "NA":
            uncertaintyunitid = None
        self.uncertaintyunitid = (uncertaintyunitid)
        if uncertaintybasisid == "NA":
            uncertaintybasisid = None
        self.uncertaintybasisid = (uncertaintybasisid)
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the data uncertainty record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        cur.execute(insert_data_uncertainty)
        dat_un_q = """
                 SELECT insert_data_uncertainty(_dataid := %(dataid)s,
                                                   _uncertaintyvalue := %(uncertaintyvalue)s,
                                                   _uncertaintyunitid := %(uncertaintyunitid)s,
                                                   _uncertaintybasisid := %(uncertaintybasisid)s,
                                                   _notes := %(notes)s)
                 """
        inputs = {
            "dataid": self.dataid,
            "uncertaintyvalue": self.uncertaintyvalue,
            "uncertaintyunitid": self.uncertaintyunitid,
            "uncertaintybasisid": self.uncertaintybasisid,
            "notes": self.notes,
        }
        cur.execute(dat_un_q, inputs)
        return

    def __str__(self):
        """Return string representation of the DataUncertainty object.
        Returns:
            str: String representation.
        """
        return f"DataUncertainty(dataid={self.dataid}, value={self.uncertaintyvalue}, basisid={self.uncertaintybasisid})"