class Chronology:
    """A chronology (age model) for a collection unit in Neotoma.

    Defines the dating framework for samples within a collection unit,
    including age model type, bounds, and preparation metadata.
    
    See the [Neotoma Manual](https://open.neotomadb.org/manual/chronology-age-related-tables-1.html#Chronologies).

    Attributes:
        chronologyid (int | None): Chronology ID.
        collectionunitid (int | None): Collection unit ID.
        agetypeid (int | None): Age type ID.
        contactid (int | None): Contact ID (first element if list provided).
        isdefault (bool | None): Whether this is the default chronology.
        chronologyname (str | None): Chronology name.
        dateprepared (str | None): Preparation date.
        agemodel (str | None): Age model description.
        ageboundyounger (float | None): Younger age bound.
        ageboundolder (float | None): Older age bound.
        notes (str | None): Additional notes.

    Examples:
        >>> chron = Chronology(collectionunitid=1, chronologyname="Model 2023")
        >>> chron.chronologyname
        'Model 2023'
    """

    def __init__(
        self,
        chronologyid=None,
        collectionunitid=None,
        agetypeid=None,
        contactid=None,
        isdefault=None,
        chronologyname=None,
        dateprepared=None,
        agemodel=None,
        ageboundyounger=None,
        ageboundolder=None,
        notes=None):
        self.chronologyid = chronologyid
        self.collectionunitid = collectionunitid
        self.agetypeid = agetypeid
        if isinstance(contactid, list):
            self.contactid = contactid[0]
        else:
            self.contactid = contactid
        self.isdefault = isdefault
        self.chronologyname = chronologyname
        self.dateprepared = dateprepared
        if isinstance(agemodel, list):
            self.agemodel = agemodel[0]
        else:
            self.agemodel = agemodel
        self.ageboundyounger = ageboundyounger
        self.ageboundolder = ageboundolder
        if isinstance(notes, list):
            self.notes = notes[0]
        else:
            self.notes = notes

    def insert_to_db(self, cur):
        """Insert the chronology record into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The chronologyid assigned by the database.
        """
        chron_query = """
        SELECT ts.insertchronology(_collectionunitid := %(collunitid)s,
                               _agetypeid := %(agetypeid)s,
                               _contactid := %(contactid)s,
                               _isdefault := %(isdefault)s,
                               _chronologyname := %(chronologyname)s,
                               _dateprepared := %(dateprepared)s,
                               _agemodel := %(agemodel)s,
                               _ageboundyounger := %(ageboundyounger)s,
                               _ageboundolder := %(ageboundolder)s)
                               """
        inputs = {
            "collunitid": self.collectionunitid,
            "contactid": self.contactid,
            "isdefault": self.isdefault if self.isdefault is not None else True,
            "chronologyname": self.chronologyname,
            "agetypeid": self.agetypeid,
            "dateprepared": self.dateprepared,
            "agemodel": self.agemodel,
            "ageboundyounger": self.ageboundyounger,
            "ageboundolder": self.ageboundolder,
        }
        if (self.ageboundyounger is not None and self.ageboundolder is not None):
            if self.ageboundyounger > self.ageboundolder:
                raise ValueError("Younger age bound cannot be greater than older age bound.")
        cur.execute(chron_query, inputs)
        self.chronologyid = cur.fetchone()[0]
        return self.chronologyid

    def __str__(self):
        """Return string representation of the Chronology object.

        Returns:
            str: String representation.
        """
        return f"Chronology(id={self.chronologyid}, name='{self.chronologyname}', agemodel='{self.agemodel}')"