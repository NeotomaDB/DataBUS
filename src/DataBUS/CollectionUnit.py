from warnings import warn

from .Geog import Geog
from .neotomaHelpers.utils import validate_date_values, validate_int_values

CU_PARAMS = [
    "handle",
    "depenvtid",
    "collunitname",
    "colldate",
    "colldevice",
    "gpsaltitude",
    "gpserror",
    "waterdepth",
    "substrateid",
    "slopeaspect",
    "slopeangle",
    "location",
    "notes",
    "geog",
    "colltypeid",
]


class CollectionUnit:
    """Represents a sediment core or excavation collection in Neotoma.

    A collection unit is a physical collection (e.g., a sediment core, excavation)
    at a specific site. It contains geographic, temporal, and physical information
    about the collected material.

    Collection units are explained further in the
    [Neotoma Manual](https://open.neotomadb.org/manual/database-design-concepts.html#sitedesign).

    Attributes:
        collectionunitid (int | None): Collection unit identifier.
        handle (str): Unique handle/identifier.
        siteid (int): Associated site ID.
        colltypeid (int | None): Collection type ID.
        depenvtid (int | None): Depositional environment ID.
        collunitname (str | None): Collection unit name.
        colldate (datetime | None): Collection date.
        colldevice (str | None): Collection device used.
        gpsaltitude (float | None): GPS altitude in meters.
        gpserror (float | None): GPS error in meters.
        waterdepth (float | None): Water depth in meters.
        substrateid (int | None): Substrate type ID.
        slopeaspect (int | None): Slope aspect in degrees.
        slopeangle (int | None): Slope angle in degrees.
        location (str | None): Location description.
        notes (str | None): Additional notes.
        geog (Geog | None): Geographic coordinates.
        distance (float | None): Distance from reference (computed when `cu.find_close_collunits()` is executed).

    Examples:
        >>> cu = CollectionUnit(siteid=1, handle="MCL-01")  # Mirror Lake core collection
        >>> cu.handle
        'MCL-01'
        >>> cu = CollectionUnit(siteid=2, handle="LC-Core-1", waterdepth=25.5, collunitname="Main core")  # Lake cave site
        >>> cu.waterdepth
        25.5
    """

    def __init__(
        self,
        collectionunitid=None,
        handle=None,
        siteid=None,
        colltypeid=None,
        depenvtid=None,
        collunitname=None,
        colldate=None,
        colldevice=None,
        gpsaltitude=None,
        gpserror=None,
        waterdepth=None,
        substrateid=None,
        slopeaspect=None,
        slopeangle=None,
        location=None,
        notes=None,
        geog=None,
    ):
        self.collectionunitid = validate_int_values(collectionunitid, "collectionunitid")
        self.siteid = validate_int_values(siteid, "siteid")
        self.colltypeid = validate_int_values(colltypeid, "colltypeid")
        self.depenvtid = validate_int_values(depenvtid, "depenvtid")
        self.colldate = validate_date_values(colldate, "colldate")
        self.substrateid = validate_int_values(substrateid, "substrateid")
        self.slopeaspect = validate_int_values(slopeaspect, "slopeaspect")
        self.slopeangle = validate_int_values(slopeangle, "slopeangle")
        self.notes = notes
        self.colldevice = colldevice
        self.notes = notes
        self.distance = None

        if handle is None:
            raise ValueError("✗ A collection unit handle must be provided.")
        if isinstance(handle, list) and len(list(set(handle))) > 1:
            raise ValueError("✗ There can only be a single collection unit handle defined.")
        elif isinstance(handle, list):
            handle = list(set(handle))[0]
        elif isinstance(handle, str) and len(handle) > 10:
            self.handle = handle[:10]  # Truncate to 10 characters
            warning_msg = f"\n ⚠  Handle '{handle}' exceeds 10 characters and has been truncated to '{self.handle}'."
            warn(warning_msg, stacklevel=2)
        else:
            self.handle = handle
        if collunitname is None:
            self.collunitname = handle
        else:
            self.collunitname = collunitname
        self.gpsaltitude = gpsaltitude
        self.gpserror = gpserror
        self.waterdepth = waterdepth
        if isinstance(location, list) and len(list(set(location))) > 1:
            raise ValueError("✗ There can only be a single location defined.")
        elif isinstance(location, list):
            self.location = location[0]
        else:
            self.location = location
        if not (isinstance(geog, Geog) or geog is None):
            raise TypeError("geog must be Geog or None.")
        self.geog = geog

    def __str__(self):
        statement = f"Name: {self.collunitname}, Handle: {self.handle}, Geog: {self.geog}"
        if self.distance is None:
            return statement
        else:
            return statement + f", Distance: {self.distance:<10}"

    def find_close_collunits(self, cur, distance=10000, limit=10):
        """Find geographically close collection units.

        Args:
            cur (psycopg2.cursor): Database cursor.
            distance (float): Distance threshold in meters.
            limit (int): Maximum number to return.

        Returns:
            list: Collection units within specified distance.
        """
        close_handles = """
                SELECT st.siteid, cu.handle, cu.collectionunitid,
                    ST_SetSRID(ST_Centroid(st.geog::geometry), 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography AS dist
                FROM   ndb.sites AS st
                INNER JOIN ndb.collectionunits AS cu ON cu.siteid = st.siteid
                WHERE ST_SetSRID(ST_Centroid(st.geog::geometry), 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography < %(distance)s
                ORDER BY dist
                LIMIT %(lim)s;"""
        if self.geog is None or self.geog.latitudeN is None or self.geog.longitudeE is None:
            return []
        else:
            cur.execute(
                close_handles,
                {
                    "long": self.geog.longitudeE,
                    "lat": self.geog.latitudeN,
                    "distance": distance,
                    "lim": limit,
                },
            )
            close_handles = cur.fetchall()
        return close_handles

    def __eq__(self, other):
        attributes = CU_PARAMS
        validate = []
        prs = [attr for attr in attributes if attr != "handle"]
        for attr in prs:
            self_val = getattr(self, attr)
            other_val = getattr(other, attr)
            if self_val is None and other_val is None:
                continue
            else:
                validate.append(self_val == other_val)
        return all(validate)

    def compare_cu(self, other):
        attributes = CU_PARAMS
        differences = []
        for attr in attributes:
            self_val = getattr(self, attr)
            other_val = getattr(other, attr)
            if (
                attr == "geog"
                and self_val is None
                and other_val.latitudeN is None
                and other_val.longitudeE is None
            ):
                continue
            if self_val is None and other_val is None:
                continue
            elif self_val != other_val:
                differences.append(f"CSV {attr}: {self_val} != Neotoma {attr}: {other_val}")
        return differences

    def update_collunit(self, other, overwrite, cu_response=None):
        if cu_response is None:
            cu_response = type("response", (), {})()
            cu_response.match = {}
            cu_response.message = []
        attributes = CU_PARAMS
        prs = [attr for attr in attributes if attr != "handle"]
        updated_attributes = []
        for attr in prs:
            if getattr(self, attr) != getattr(other, attr):
                cu_response.matched[attr] = False
                cu_response.message.append(
                    f"? {attr} does not match. Update set to {overwrite[attr]}\n"
                    f"CSV File value: {getattr(self, attr)}.\n"
                    f"Neotoma value: {getattr(other, attr)}"
                )
            else:
                cu_response.valid.append(True)
                cu_response.message.append(f"✔  {attr} match.")
            if attr in overwrite and overwrite[attr]:
                setattr(self, attr, getattr(other, attr))
                updated_attributes.append(attr)
        return self

    def upsert_to_db(self, cur):
        cu_query = """SELECT upsert_collunit(_collectionunitid := %(collectionunitid)s,
                                             _handle := %(handle)s,
                                             _siteid := %(siteid)s,
                                             _colltypeid := %(colltypeid)s,
                                             _depenvtid := %(depenvtid)s,
                                             _collunitname := %(collunitname)s,
                                             _colldate := %(colldate)s,
                                             _colldevice := %(colldevice)s,
                                             _gpslatitude := %(ns)s,
                                             _gpslongitude := %(ew)s,
                                             _gpsaltitude := %(gpsaltitude)s,
                                             _gpserror := %(gpserror)s,
                                             _waterdepth := %(waterdepth)s,
                                             _substrateid := %(substrateid)s,
                                             _slopeaspect := %(slopeaspect)s,
                                             _slopeangle := %(slopeangle)s,
                                             _location := %(location)s,
                                             _notes := %(notes)s)
                                             """
        if not isinstance(self.geog, Geog):
            latitude = None
            longitude = None
        else:
            latitude = self.geog.latitudeN
            longitude = self.geog.longitudeE

        inputs = {
            "siteid": self.siteid,
            "collectionunitid": self.collectionunitid,
            "handle": self.handle,
            "colltypeid": self.colltypeid,
            "depenvtid": self.depenvtid,
            "collunitname": self.collunitname,
            "colldate": self.colldate,
            "colldevice": self.colldevice,
            "ns": latitude,
            "ew": longitude,
            "gpsaltitude": self.gpsaltitude,
            "gpserror": self.gpserror,
            "waterdepth": self.waterdepth,
            "substrateid": self.substrateid,
            "slopeaspect": self.slopeaspect,
            "slopeangle": self.slopeangle,
            "location": self.location,
            "notes": self.notes,
        }
        cur.execute(cu_query, inputs)
        self.collunitid = cur.fetchone()[0]
        return self.collunitid

    def insert_to_db(self, cur):
        """Insert the collection unit into the database.

        Args:
            cur (psycopg2.cursor): Database cursor.

        Returns:
            int: The collectionunitid assigned.
        """
        cu_query = """SELECT ts.insertcollectionunit(
                               _handle := %(handle)s,
                               _siteid := %(siteid)s,
                               _colltypeid := %(colltypeid)s,
                               _depenvtid := %(depenvtid)s,
                               _collunitname := %(collunitname)s,
                               _colldate := %(colldate)s,
                               _colldevice := %(colldevice)s,
                               _gpslatitude := %(gpslatitude)s,
                               _gpslongitude := %(gpslongitude)s,
                               _gpserror := %(gpserror)s,
                               _waterdepth := %(waterdepth)s,
                               _substrateid := %(substrateid)s,
                               _slopeaspect := %(slopeaspect)s,
                               _slopeangle := %(slopeangle)s,
                               _location := %(location)s,
                               _notes := %(notes)s)"""
        inputs = {
            "handle": self.handle,
            "siteid": self.siteid,
            "colltypeid": self.colltypeid,
            "depenvtid": self.depenvtid,
            "collunitname": self.collunitname,
            "colldate": self.colldate,
            "colldevice": self.colldevice,
            "gpslatitude": self.geog.latitudeN if self.geog is not None else None,
            "gpslongitude": self.geog.longitudeE if self.geog is not None else None,
            "gpserror": self.gpserror,
            "waterdepth": self.waterdepth,
            "substrateid": self.substrateid,
            "slopeaspect": self.slopeaspect,
            "slopeangle": self.slopeangle,
            "location": self.location,
            "notes": self.notes,
        }
        cur.execute(cu_query, inputs)
        self.collunitid = cur.fetchone()[0]
        return self.collunitid
