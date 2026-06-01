from .neotomaHelpers.utils import validate_int_values

SEQUENCE_PARAMS = ["datasetid", "sequence", "asv", "primername"]


class Sequence:
    """A DNA sequence record in the Neotoma database.

    Links a dataset (datasetid) with its DNA sequence and ASV identifier.
    Individual data rows are linked via the ndb.sequencedata junction table.

    Attributes:
        sequenceid (int | None): Database ID (assigned after insertion).
        datasetid (int | None): Dataset identifier.
        sequence (str | None): DNA nucleotide sequence.
        asv (str | None): Amplicon Sequence Variant identifier.
        primername (str | None): Primer name used for sequencing.

    Examples:
        >>> seq = Sequence(datasetid=10, sequence="ACGT", asv="ASV1")
        >>> seq.asv
        'ASV1'
    """

    def __init__(self, datasetid=None, sequence=None, asv=None, primername=None):
        self.sequenceid = None
        self.datasetid = validate_int_values(datasetid, "datasetid")
        if sequence is not None and not isinstance(sequence, str):
            raise TypeError("sequence must be a string or None.")
        self.sequence = sequence
        if asv is not None and not isinstance(asv, str):
            raise TypeError("asv must be a string or None.")
        self.asv = asv
        if primername is not None and not isinstance(primername, str):
            raise TypeError("primername must be a string or None.")
        self.primername = primername

    def insert_to_db(self, cur):
        """Insert the sequence record into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The sequenceid assigned by the database.
        """
        seq_q = """
            INSERT INTO ndb.sequences (datasetid, sequence, asv, primername)
            VALUES (%(datasetid)s, %(sequence)s, %(asv)s, %(primername)s)
            RETURNING sequenceid;
        """
        inputs = {
            "datasetid": self.datasetid,
            "sequence": self.sequence,
            "asv": self.asv,
            "primername": self.primername,
        }
        cur.execute(seq_q, inputs)
        self.sequenceid = cur.fetchone()[0]
        return self.sequenceid

    def __str__(self):
        return f"Sequence(sequenceid={self.sequenceid}, datasetid={self.datasetid}, asv={self.asv})"
