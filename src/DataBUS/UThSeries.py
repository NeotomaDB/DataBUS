import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers",
                                   "insert_uthseries.sql") as sql_file:
    insert_uthseries = sql_file.read()
with importlib.resources.open_text("DataBUS.sqlHelpers",
                                   "insert_uraniumseriesdata.sql") as sql_file:
    insert_uraniumseriesquery = sql_file.read()
UTH_PARAMS = ['geochronid', 'decayconstantid',
              'ratio230th232th', 'ratiouncertainty230th232th',
              'activity230th238u', 'activityuncertainty230th238u',
              'activity234u238u', 'activityuncertainty234u238u',
              'iniratio230th232th', 'iniratiouncertainty230th232th']

class UThSeries:
    """Uranium-thorium radiometric dating data in Neotoma.

    Stores U/Th isotope ratios and activities measured for radiometric dating,
    including initial ratios and associated uncertainties.

    Attributes:
        geochronid (int | None): Geochronology ID.
        decayconstantid (int | None): Decay constant ID.
        ratio230th232th (float | None): ²³⁰Th/²³²Th ratio.
        ratiouncertainty230th232th (float | None): Ratio uncertainty.
        activity230th238u (float | None): ²³⁰Th/²³⁸U activity.
        activityuncertainty230th238u (float | None): Activity uncertainty.
        activity234u238u (float | None): ²³⁴U/²³⁸U activity.
        activityuncertainty234u238u (float | None): Activity uncertainty.
        iniratio230th232th (float | None): Initial ratio.
        iniratiouncertainty230th232th (float | None): Initial ratio uncertainty.

    Examples:
        >>> uth = UThSeries(geochronid=1, ratio230th232th=1.265)
        >>> uth.ratio230th232th
        1.265
    """
    description = "UThSeries object in Neotoma"

    def __init__(
        self,
        geochronid=None,
        decayconstantid=None,
        ratio230th232th=None,
        ratiouncertainty230th232th=None,
        activity230th238u=None,
        activityuncertainty230th238u=None,
        activity234u238u=None,
        activityuncertainty234u238u=None,
        iniratio230th232th=None,
        iniratiouncertainty230th232th=None):
        self.geochronid = geochronid
        self.decayconstantid = decayconstantid
        self.ratio230th232th = ratio230th232th
        self.ratiouncertainty230th232th = ratiouncertainty230th232th
        self.activity230th238u = activity230th238u
        self.activityuncertainty230th238u = activityuncertainty230th238u
        self.activity234u238u = activity234u238u
        self.activityuncertainty234u238u = activityuncertainty234u238u
        self.iniratio230th232th = iniratio230th232th
        self.iniratiouncertainty230th232th = iniratiouncertainty230th232th

    def insert_to_db(self, cur):
        """Insert U/Th series data into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        cur.execute(insert_uthseries)
        uths_query = """SELECT insert_uthseries(_geochronid := %(geochronid)s,
                                                    _decayconstantid := %(decayconstantid)s,
                                                    _ratio230th232th := %(ratio230th232th)s,
                                                    _ratiouncertainty230th232th := %(ratiouncertainty230th232th)s,
                                                    _activity230th238u := %(activity230th238u)s,
                                                    _activityuncertainty230th238u := %(activityuncertainty230th238u)s,
                                                    _activity234u238u := %(activity234u238u)s,
                                                    _activityuncertainty234u238u := %(activityuncertainty234u238u)s,
                                                    _iniratio230th232th := %(iniratio230th232th)s,
                                                    _iniratiouncertainty230th232th := %(iniratiouncertainty230th232th)s);"""
        inputs = {
            'geochronid': self.geochronid,
            'decayconstantid': self.decayconstantid,
            'ratio230th232th': self.ratio230th232th,
            'ratiouncertainty230th232th': self.ratiouncertainty230th232th,
            'activity230th238u': self.activity230th238u,
            'activityuncertainty230th238u': self.activityuncertainty230th238u,
            'activity234u238u': self.activity234u238u,
            'activityuncertainty234u238u': self.activityuncertainty234u238u,
            'iniratio230th232th': self.iniratio230th232th,
            'iniratiouncertainty230th232th': self.iniratiouncertainty230th232th
        }
        cur.execute(uths_query, inputs)
        return

    def __str__(self):
        """Return string representation of the UThSeries object.

        Returns:
            str: String representation.
        """
        return f"UThSeries(geochronid={self.geochronid}, ratio230th232th={self.ratio230th232th}, activity234u238u={self.activity234u238u})"


def insert_uraniumseriesdata(cur, dataid, geochronid):
    """Insert uranium series data linking data to geochronology.

    Args:
        cur (psycopg2.cursor): Database cursor for executing queries.
        dataid (int): Data value identifier.
        geochronid (int): Geochronological data identifier.

    Returns:
        None
    """
    cur.execute(insert_uraniumseriesquery)
    uths_query = """SELECT insert_uraniumseriesdata(_geochronid := %(geochronid)s,
                                                    _dataid := %(dataid)s)"""
    inputs = {
        'geochronid': geochronid,
        'dataid': dataid
    }
    cur.execute(uths_query, inputs)
    return