CHRONOLOGY_PARAMS = ['ageboundolder', 'ageboundyounger', 'agemodel', 'chronologyname',
                     'agetypeid', 'contactid', 'dateprepared', 'notes']
from .neotomaHelpers.utils import validate_int_values,validate_date_values

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
        chronologyname (str | None): Chronology name.
        dateprepared (str | None): Preparation date.
        agemodel (str | None): Age model description.
        ageboundyounger (float | None): Younger age bound.
        ageboundolder (float | None): Older age bound.
        isdefault (bool | None): Whether this is the default chronology for the collection unit.
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
        chronologyname=None,
        dateprepared=None,
        agemodel=None,
        ageboundyounger=None,
        ageboundolder=None,
        isdefault = None,
        notes=None):
        self.chronologyid = validate_int_values(chronologyid, "chronologyid")
        if collectionunitid is None:
            raise ValueError("Collection Unit ID is required for Chronology.")
        self.collectionunitid = validate_int_values(collectionunitid, "collectionunitid")
        self.dateprepared = validate_date_values(dateprepared, "dateprepared")
        self.agetypeid = validate_int_values(agetypeid, "agetypeid")
        self.chronologyname = chronologyname
        self.notes = notes
        if isinstance(agemodel, list):
            assert len(agemodel) == 1, "agemodel should only have one element"
            self.agemodel = agemodel[0]
        else:
            self.agemodel = agemodel
        for attr, param in [("ageboundyounger", ageboundyounger), 
                            ("ageboundolder", ageboundolder)]:
            if isinstance(param, list):
                valid = [x for x in param if x is not None]
                if valid:
                    value = int(min(valid)) if attr == 'ageboundyounger' else int(max(valid))
                else:
                    value = None
            else:
                value = int(param) if param is not None else None
            setattr(self, attr, value)
        if (self.ageboundyounger is not None and self.ageboundolder is not None):
            assert self.ageboundyounger <= self.ageboundolder, (f"Younger age bound "
                                                              f"cannot be greater than older age bound.")
        if isinstance(contactid, list):
            assert len(list(set(contactid))) == 1, "Contact ID list should only contain one unique value."
            contactid = contactid[0]
        self.contactid = validate_int_values(contactid, "contactid")
        if isinstance(isdefault, int):
            if isdefault not in [0, 1]:
                raise ValueError("isdefault should be 0 or 1 if provided as an integer.")
            self.isdefault = bool(isdefault)
        elif isdefault is None:
            self.isdefault = False
        else:
            self.isdefault = bool(isdefault)

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
                               _chronologyname := %(chronologyname)s,
                               _dateprepared := %(dateprepared)s,
                               _agemodel := %(agemodel)s,
                               _ageboundyounger := %(ageboundyounger)s,
                               _ageboundolder := %(ageboundolder)s,
                               _isdefault := %(isdefault)s,
                               _notes := %(notes)s)
                               """
        
        inputs = {
            "collunitid": self.collectionunitid,
            "contactid": self.contactid,
            "chronologyname": self.chronologyname,
            "agetypeid": self.agetypeid,
            "dateprepared": self.dateprepared,
            "agemodel": self.agemodel,
            "ageboundyounger": self.ageboundyounger,
            "ageboundolder": self.ageboundolder,
            "isdefault": self.isdefault,
            "notes": self.notes
        }
        cur.execute(chron_query, inputs)
        self.chronologyid = cur.fetchone()[0]
        return self.chronologyid

    def __str__(self):
        """Return string representation of the Chronology object.

        Returns:
            str: String representation.
        """
        return f"Chronology(id={self.chronologyid}, name='{self.chronologyname}', agemodel='{self.agemodel}')"