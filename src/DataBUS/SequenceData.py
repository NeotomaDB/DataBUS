from .neotomaHelpers.utils import validate_int_values

SEQUENCEDATA_PARAMS = ["dataid", "sequenceid"]


class SequenceData:
    """Junction record linking a sequence to a data row in the Neotoma database.

    Maps individual data observations (dataid) to their corresponding DNA
    sequence records (sequenceid) via the ndb.sequencedata table.

    Attributes:
        dataid (int | None): Data record identifier.
        sequenceid (int | None): Sequence record identifier.

    Examples:
        >>> sd = SequenceData(dataid=1, sequenceid=5)
        >>> sd.dataid
        1
    """

    def __init__(self, dataid=None, sequenceid=None):
        self.dataid = validate_int_values(dataid, "dataid")
        self.sequenceid = validate_int_values(sequenceid, "sequenceid")

    def insert_to_db(self, cur):
        """Insert the sequence-data link into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        sd_q = """
            INSERT INTO ndb.sequencedata (dataid, sequenceid)
            VALUES (%(dataid)s, %(sequenceid)s);
        """
        inputs = {
            "dataid": self.dataid,
            "sequenceid": self.sequenceid,
        }
        cur.execute(sd_q, inputs)

    def __str__(self):
        return f"SequenceData(dataid={self.dataid}, sequenceid={self.sequenceid})"
