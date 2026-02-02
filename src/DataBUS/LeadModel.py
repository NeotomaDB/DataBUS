# with open('./DataBUS/sqlHelpers/insert_pb_model.sql', 'r') as sql_file:
#    insert_pb_model = sql_file.read()

import importlib.resources
with importlib.resources.open_text(
    "DataBUS.sqlHelpers", "insert_pb_model.sql"
) as sql_file:
    insert_pb_model = sql_file.read()

class LeadModel:
    """A Lead-210 geochronological model in Neotoma.

    Manages lead isotope data for radiometric dating of sediment cores,
    storing basis information and cumulative inventory values.

    Attributes:
        pbbasisid (int | None): Lead isotope basis ID.
        analysisunitid (int | None): Analysis unit ID.
        cumulativeinventory (float | None): Cumulative inventory (Bq/cm²).

    Examples:
        >>> lead_model = LeadModel(pbbasisid=1, analysisunitid=2, cumulativeinventory=145.3)
        >>> lead_model.cumulativeinventory
        145.3
    """

    def __init__(self, pbbasisid=None, analysisunitid=None, cumulativeinventory=None):
        self.pbbasisid = pbbasisid
        self.analysisunitid = analysisunitid
        self.cumulativeinventory = cumulativeinventory

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
        return f"LeadModel(pbbasisid={self.pbbasisid}, analysisunitid={self.analysisunitid}, cumulativeinventory={self.cumulativeinventory})"
