import importlib.resources
from .neotomaHelpers.utils import validate_int_values
DATAUNCERTAINTY_PARAMS = ["uncertaintyvalue", "uncertaintyunitid", "notes"]
insert_data_uncertainty = importlib.resources.files("DataBUS.sqlHelpers").joinpath("insert_data_uncertainty.sql").read_text(encoding="UTF-8")

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
        self.dataid = validate_int_values(dataid, "dataid")
        self.uncertaintyunitid = validate_int_values(uncertaintyunitid, "uncertaintyunitid")
        self.uncertaintybasisid = validate_int_values(uncertaintybasisid, "uncertaintybasisid")
        self.uncertaintyvalue = uncertaintyvalue
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