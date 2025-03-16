class Chronology:
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
        notes=None,
        recdatecreated=None,
        recdatemodified=None,
    ):
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
        self.recdatecreated = recdatecreated
        self.recdatemodified = recdatemodified

    def insert_to_db(self, cur):
        chron_query = """
        SELECT ts.insertchronology(_collectionunitid := %(collunitid)s,
                               _agetypeid := %(agetypeid)s,
                               _contactid := %(contactid)s,
                               _isdefault := TRUE,
                               _chronologyname := %(chronologyname)s,
                               _dateprepared := %(dateprepared)s,
                               _agemodel := %(agemodel)s,
                               _ageboundyounger := %(ageboundyounger)s,
                               _ageboundolder := %(ageboundolder)s)
                               """
        inputs = {
            "collunitid": self.collectionunitid,
            "contactid": self.contactid,
            "chronologyname": self.chronologyname,
            "agetypeid": self.agetypeid,  # Comming from column X210Pb.Date.Units which should be linked to params3
            "dateprepared": self.dateprepared,
            "agemodel": self.agemodel,
            "ageboundyounger": self.ageboundyounger,
            "ageboundolder": self.ageboundolder,
        }
        print(inputs)
        cur.execute(chron_query, inputs)
        self.chronologyid = cur.fetchone()[0]
        return self.chronologyid

    def __str__(self):
        pass