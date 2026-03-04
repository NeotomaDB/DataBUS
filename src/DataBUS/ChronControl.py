from .neotomaHelpers.utils import validate_int_values

CCONTROL_PARAMS = [
    "chronologyid",
    "chroncontroltypeid",
    "analysisunitid",
    "depth",
    "thickness",
    "agetypeid",
    "notes",
    "age",
    "agelimityounger",
    "agelimitolder",
]


class ChronControl:
    """A chronological control point in Neotoma.

    Provides dating constraints for a chronology, such as radiocarbon dates
    or other age measurements at specific depths within a stratigraphic sequence.
    See the [Neotoma Manual](https://open.neotomadb.org/manual/chronology-age-related-tables-1.html#ChronControls).

    Attributes:
        chroncontrolid (int | None): Control point ID.
        chronologyid (int | None): Chronology ID.
        chroncontroltypeid (int | None): Control type ID.
        depth (float | None): Depth value.
        thickness (float | None): Thickness value.
        age (float | None): Age value in years.
        agelimityounger (float | None): Younger age bound.
        agelimitolder (float | None): Older age bound.
        notes (str | None): Additional notes.
        analysisunitid (int | None): Analysis unit ID.
        agetypeid (int | None): Age type ID.

    Examples:
        >>> chron = ChronControl(chronologyid=1, depth=5.5, age=75)
        >>> chron.age
        75
    """

    def __init__(
        self,
        chroncontrolid=None,
        chronologyid=None,
        chroncontroltypeid=None,
        depth=None,
        thickness=None,
        age=None,
        agelimityounger=None,
        agelimitolder=None,
        notes=None,
        analysisunitid=None,
        agetypeid=None,
    ):
        for param in [chroncontroltypeid, chronologyid, analysisunitid, agetypeid, depth, age]:
            if param is None:
                raise ValueError(f"{param} is required and cannot be None.")
        self.chroncontrolid = chroncontrolid
        self.chronologyid = validate_int_values(chronologyid, "chronologyid")
        self.chroncontroltypeid = validate_int_values(chroncontroltypeid, "chroncontroltypeid")
        self.analysisunitid = validate_int_values(analysisunitid, "analysisunitid")
        self.agetypeid = validate_int_values(agetypeid, "agetypeid")
        self.depth = depth
        self.thickness = thickness
        self.age = age
        self.agelimityounger = agelimityounger
        self.agelimitolder = agelimitolder
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the chronological control point into the database.
        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.
        Returns:
            int: The chroncontrolid assigned by the database.
        """
        chroncon_query = """
        SELECT ts.insertchroncontrol(_chronologyid := %(chronologyid)s,
                                     _chroncontroltypeid := %(chroncontroltypeid)s,
                                     _analysisunitid := %(analysisunitid)s,
                                     _depth := %(depth)s,
                                     _thickness := %(thickness)s,
                                     _agetypeid := %(agetypeid)s,
                                     _age := %(age)s,
                                     _agelimityounger := %(agelimityounger)s,
                                     _agelimitolder := %(agelimitolder)s,
                                     _notes := %(notes)s)
                        """
        inputs = {
            "chronologyid": self.chronologyid,
            "analysisunitid": self.analysisunitid,
            "chroncontroltypeid": self.chroncontroltypeid,
            "depth": self.depth,
            "thickness": self.thickness,
            "agetypeid": self.agetypeid,
            "age": self.age,
            "agelimityounger": self.agelimityounger,
            "agelimitolder": self.agelimitolder,
            "notes": self.notes,
        }
        if (
            self.agelimityounger is not None
            and self.agelimitolder is not None
            and self.agelimityounger > self.agelimitolder
            and self.agetypeid != 1
        ):
            raise ValueError("Younger age limit cannot be greater than older age limit.")

        cur.execute(chroncon_query, inputs)
        self.chroncontrolid = cur.fetchone()[0]
        return self.chroncontrolid

    def __str__(self):
        """Return string representation of the ChronControl object.

        Returns:
            str: String representation.
        """
        return f"ChronControl(id={self.chroncontrolid}, chronologyid={self.chronologyid}, depth={self.depth}, age={self.age})"
