class Geochron:
    def __init__(self, sampleid,
                 geochrontypeid, 
                 agetypeid, age, 
                 errorolder, erroryounger, 
                 infinite, delta13c, 
                 labnumber, materialdated, notes):
        self.sampleid = sampleid
        self.geochrontypeid = geochrontypeid
        self.agetypeid = agetypeid
        self.age = age
        self.errorolder = errorolder
        self.erroryounger = erroryounger
        if infinite is None:
            self.infinite = False
        else:
            self.infinite = infinite
        self.delta13c = delta13c
        self.labnumber = labnumber
        self.materialdated = materialdated
        self.notes = notes
        
    def insert_to_db(self, cur):
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
        pass