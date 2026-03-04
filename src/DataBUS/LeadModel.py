import importlib.resources

from .neotomaHelpers.utils import validate_int_values

LEAD_MODEL_PARAMS = ["pbbasisid", "cumulativeinventory", "datinghorizon"]

insert_pb_model = (
    importlib.resources.files("DataBUS.sqlHelpers")
    .joinpath("insert_pb_model.sql")
    .read_text(encoding="UTF-8")
)


class LeadModel:
    """A Lead-210 geochronological model in Neotoma.

    Manages lead isotope data for radiometric dating of sediment cores,
    storing basis information and cumulative inventory values.

    Attributes:
        pbbasisid (int | None): Lead isotope basis ID.
        analysisunitid (int | None): Analysis unit ID.
        cumulativeinventory (float | None): Cumulative inventory (Bq/cm²).
        datinghorizon (float | None): Depth of the dating horizon (cm).

    Examples:
        >>> lead_model = LeadModel(pbbasisid=1, analysisunitid=2, cumulativeinventory=145.3)
        >>> lead_model.cumulativeinventory
        145.3
    """

    def __init__(
        self, pbbasisid=None, analysisunitid=None, cumulativeinventory=None, datinghorizon=None
    ):
        self.pbbasisid = validate_int_values(pbbasisid, "pbbasisid")
        self.analysisunitid = validate_int_values(analysisunitid, "analysisunitid")
        self.cumulativeinventory = cumulativeinventory
        self.datinghorizon = datinghorizon

    def insert_to_db(self, cur):
        """Insert the Lead model record into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        cur.execute(insert_pb_model)
        lead_q = """SELECT insert_lead_model(_pbbasisid := %(pbbasisid)s,
                                              _analysisunitid := %(analysisunitid)s,
                                              _cumulativeinventory := %(cumulativeinventory)s)"""
        inputs = {
            "pbbasisid": self.pbbasisid,
            "analysisunitid": self.analysisunitid,
            "cumulativeinventory": self.cumulativeinventory,
        }

        cur.execute(lead_q, inputs)
        return

    def __str__(self):
        """Return string representation of the LeadModel object.

        Returns:
            str: String representation.
        """
        return f"LeadModel(pbbasisid={self.pbbasisid},analysisunitid={self.analysisunitid}, cumulativeinventory={self.cumulativeinventory})"
