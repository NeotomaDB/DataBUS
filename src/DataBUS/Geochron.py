from .neotomaHelpers.utils import validate_int_values
GECHRON_PARAMS = ["sampleid", "geochrontypeid", "agetypeid",
                  "age", "errorolder", "erroryounger",
                  "infinite", "delta13c", "labnumber",
                  "materialdated", "notes"]

class Geochron:
    """A geochronological age determination in Neotoma.

    Stores age measurements from radiometric and other dating techniques,
    including determined age, uncertainty bounds, and dated material info.
    
    Geochronologies are explained further in the
    [Neotoma Manual](https://open.neotomadb.org/manual/chronology-age-related-tables-1.html#Geochronology)
    
    Attributes:
        sampleid (int): Sample ID.
        geochrontypeid (int): Geochron type ID.
        agetypeid (int): Age type ID.
        age (float): Age value.
        errorolder (float): Older error bound.
        erroryounger (float): Younger error bound.
        infinite (bool): Infinite age flag (defaults to False).
        delta13c (float | None): Delta 13C value (for radiocarbon).
        labnumber (str | None): Laboratory number.
        materialdated (str | None): Material dated.
        notes (str | None): Additional notes.
        geochronid (int | None): Geochron ID (assigned after insertion).

    Examples:
        >>> geo = Geochron(sampleid=1, geochrontypeid=1, agetypeid=1, age=3250,
        ...                errorolder=100, erroryounger=100, infinite=False,
        ...                delta13c=-25.5, labnumber="UCIAMS-12345",
        ...                materialdated="Charcoal", notes=None)
        >>> geo.age
        3250
    """

    def __init__(self, 
                 sampleid = None,
                 geochrontypeid = None,
                 agetypeid = None,
                 age = None,
                 errorolder = None, 
                 erroryounger = None,
                 infinite = None,
                 delta13c = None,
                 labnumber = None,
                 materialdated = None,
                 notes = None):
        missing = [name for name, val in [
            ("sampleid", sampleid),
            ("geochrontypeid", geochrontypeid),
            ("agetypeid", agetypeid),
            ("age", age)] if val is None]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
        self.sampleid = validate_int_values(sampleid, "sampleid")
        self.geochrontypeid = validate_int_values(geochrontypeid, "geochrontypeid")
        self.agetypeid = validate_int_values(agetypeid, "agetypeid")
        self.age = age
        self.errorolder = errorolder
        self.erroryounger = erroryounger
        if not infinite:
            self.infinite = False
        else:
            if not isinstance(infinite, bool):
                try:
                    self.infinite = bool(int(infinite))
                except ValueError:
                    raise TypeError("Infinite must be a boolean value.")
        self.delta13c = delta13c
        self.labnumber = labnumber
        self.materialdated = materialdated
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the geochronological date into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The geochronid assigned by the database.
        """
        geochron_query = """
        SELECT ts.insertgeochron(_sampleid := %(sampleid)s,
                                    _geochrontypeid := %(geochrontypeid)s,
                                    _agetypeid := %(agetypeid)s,
                                    _age := %(age)s,
                                    _errorolder := %(errorolder)s,
                                    _erroryounger := %(erroryounger)s,
                                    _infinite := %(infinite)s,
                                    _delta13c := %(delta13c)s,
                                    _labnumber := %(labnumber)s,
                                    _materialdated := %(materialdated)s,
                                    _notes := %(notes)s)
                                    """
        inputs = {
            "sampleid": self.sampleid,
            "geochrontypeid": self.geochrontypeid,
            "agetypeid": self.agetypeid,
            "age": self.age,
            "errorolder": self.errorolder,
            "erroryounger": self.erroryounger,
            "infinite": self.infinite,
            "delta13c": self.delta13c,
            "labnumber": self.labnumber,
            "materialdated": self.materialdated,
            "notes": self.notes}
        cur.execute(geochron_query, inputs)
        self.geochronid = cur.fetchone()[0]
        return self.geochronid

    def __str__(self):
        """Return string representation of the Geochron object.

        Returns:
            str: String representation.
        """
        return f"Geochron(id={self.geochronid}, age={self.age}, labnumber='{self.labnumber}', material='{self.materialdated}')"