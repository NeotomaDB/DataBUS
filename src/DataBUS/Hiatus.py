import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers", "insert_hiatus.sql") as sql_file:
    insert_hiatus = sql_file.read()

with importlib.resources.open_text("DataBUS.sqlHelpers", "insert_hiatuschronology.sql") as sql_file:
    insert_hiatuschronology = sql_file.read()

class Hiatus:
    """A hiatus or stratigraphic gap in a sediment sequence.

    Marks a discontinuity in the stratigraphic record, representing missing
    time. Bounded by analysis units and can be associated with a chronology.

    Attributes:
        hiatusid (int | None): Hiatus ID.
        analysisunitstart (int): Start analysis unit ID.
        analysisunitend (int): End analysis unit ID.
        notes (str | None): Description (first element if list provided).

    Raises:
        TypeError: If hiatusid is not int/None/"NA", analysis units not int,
            or notes not str/None.

    Examples:
        >>> hiatus = Hiatus(analysisunitstart=10, analysisunitend=15)
        >>> hiatus.analysisunitstart
        10
    """
    description = "Hiatus object in Neotoma"

    def __init__(
        self,
        hiatusid=None,
        analysisunitstart=None,
        analysisunitend=None,
        notes=None):
        if not (isinstance(hiatusid, int) or hiatusid is None or hiatusid == "NA"):
            raise TypeError("✗ Hiatus ID must be an integer or None.")
        if hiatusid == ["NA"] or hiatusid == "NA":
            hiatusid = None
        self.hiatusid = hiatusid

        if not (isinstance(analysisunitstart, (int))):
            raise TypeError("analysisunitstart must be a number.")
        self.analysisunitstart = analysisunitstart

        if not (isinstance(analysisunitend, (int))):
            raise TypeError("analysisunitstart must be a number.")
        self.analysisunitend = analysisunitend

        if isinstance(notes, list):
            notes = notes[0]
        if not (isinstance(notes, str) or notes is None):
            raise TypeError("Notes must be a str or None.")
        self.notes = notes

    def __str__(self):
        """Return string representation of the Hiatus object.

        Returns:
            str: String representation.
        """
        return f"Hiatus(id={self.hiatusid}, start={self.analysisunitstart}, end={self.analysisunitend})"

    def __eq__(self, other):
        """Compare two Hiatus objects for equality.

        Args:
            other (Hiatus): Another Hiatus object.

        Returns:
            bool: True if all attributes match.
        """
        return (
            self.hiatusid == other.hiatusid
            and self.analysisunitstart == other.analysisunitstart
            and self.analysisunitend == other.analysisunitend
            and self.notes == other.notes
        )

    def insert_to_db(self, cur):
        """Insert the hiatus record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The hiatusid assigned by the database.
        """
        cur.execute(insert_hiatus)
        hiatus_query = """SELECT insert_hiatus(_analysisunitstart := %(analysisunitstart)s,
                                               _analysisunitend := %(analysisunitend)s,
                                               _notes := %(notes)s)"""

        inputs = {
            "analysisunitstart": self.analysisunitstart,
            "analysisunitend": self.analysisunitend,
            "notes": self.notes
        }
        cur.execute(hiatus_query, inputs)
        self.hiatusid = cur.fetchone()[0]
        return self.hiatusid

    def insert_hiatus_chron_to_db(self, chronologyid, hiatuslength, hiatusuncertainty, cur):
        """Insert hiatus-chronology relationship into the database.

        Args:
            chronologyid (int): Associated chronology identifier.
            hiatuslength (float): Length of hiatus.
            hiatusuncertainty (float): Uncertainty in hiatus length.
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        cur.execute(insert_hiatuschronology)
        hiatus_query = """SELECT insert_hiatuschronology(_hiatusid := %(hiatusid)s,
                                               _chronologyid := %(chronologyid)s,
                                               _hiatuslength := %(hiatuslength)s,
                                               _hiatusuncertainty := %(hiatusuncertainty)s)"""

        inputs = {
            "hiatusid": self.hiatusid,
            "chronologyid": chronologyid,
            "hiatuslength": hiatuslength,
            "hiatusuncertainty": hiatusuncertainty
        }
        cur.execute(hiatus_query, inputs)
        return