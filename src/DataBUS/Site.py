from .neotomaHelpers.eq import _eq
from .Geog import Geog
SITE_PARAMS = ["siteid", "sitename", "altitude",
               "area", "sitedescription", "notes", "geog"]

class Site:
    """Represents a geographic site location in Neotoma.

    A site is a geographic location where paleoenvironmental data has been collected.
    It contains geographic coordinates and descriptive information.

    See the [Neotoma Manual](https://open.neotomadb.org/manual/site-related-tables-1.html#Sites).

    Attributes:
        description (str): Class description.
        siteid (int | None): Site identifier.
        sitename (str): Site name (required).
        altitude (int | None): Elevation in meters.
        area (float | None): Site area in square kilometers.
        sitedescription (str | None): Detailed description.
        notes (str | None): Additional notes.
        geog (Geog | None): Geographic coordinates.
        distance (float | None): Distance from reference (computed).
    
    Examples:
        >>> site = Site(sitename="Mirror Lake", geog=Geog([43.3734, -71.5316]))  # Mirror Lake, NH
        >>> site.sitename
        'Mirror Lake'
        >>> site = Site(sitename="Crater Lake", altitude=1949, geog=Geog([42.9453, -122.1103]))
        >>> site.altitude
        1949
    """
    def __init__(
        self,
        siteid=None,
        sitename=None,
        altitude=None,
        area=None,
        sitedescription=None,
        notes=None,
        geog=None):
        if not (isinstance(siteid, int) or siteid is None):
            raise TypeError("✗ Site ID must be an integer or None.")
        self.siteid = siteid

        if sitename is None:
            raise ValueError(f"✗ Sitename must be given.")
        if not isinstance(sitename, str):
            raise TypeError(f"✗ Sitename must be a string.")
        self.sitename = sitename.strip().strip(',').lower().title()

        if not (isinstance(altitude, (int, float)) or altitude is None):
            raise TypeError("Altitude must be a number or None.")
        self.altitude = altitude

        if not (isinstance(area, (int, float)) or area is None):
            raise TypeError("Area must be a number or None.")
        self.area = area

        if not (isinstance(sitedescription, str) or sitedescription is None):
            raise TypeError("Site Description must be a string or None.")
        self.sitedescription = sitedescription
        if isinstance(self.sitedescription, str) and self.sitedescription.strip() == '':
            self.sitedescription = None
        if not (isinstance(notes, str) or notes is None):
            raise TypeError("Site Description must be a string or None.")
        if isinstance(self.sitedescription, str) and self.sitedescription.strip() == '':
            self.sitedescription = None
        if not (isinstance(notes, str) or notes is None):
            raise TypeError("Notes must be a str or None.")
        self.notes = notes
        if not (isinstance(geog, Geog) or geog is None):
            raise TypeError("geog must be Geog or None.")
        self.geog = geog
        self.distance = None

    def __str__(self):
        """Return string representation of the Site object.

        Returns:
            str: String representation including site name, ID, and geography.
        """
        id = f" ID: {self.siteid}, " if self.siteid is not None else ""
        statement = f"Name: {self.sitename},{id} Geog: {self.geog}"
        return statement

    def __eq__(self, other):
        """Compare two Site objects for equality.
        Returns:
            bool: True if all attributes match.
        """
        return (self.siteid == other.siteid
                and self.sitename == other.sitename
                and self.altitude == other.altitude
                and self.area == other.area
                and self.sitedescription == other.sitedescription
                and self.notes == other.notes
                and self.geog == other.geog)

    def insert_to_db(self, cur):
        """Insert the site into the Neotoma database.
        Args:
            cur (psycopg2.cursor): Database cursor.
        Returns:
            int: The siteid assigned by the database.
        """
        site_query = """SELECT ts.insertsite(_sitename := %(sitename)s, 
                        _altitude := %(altitude)s,
                        _area := %(area)s,
                        _descript := %(sitedescription)s,
                        _notes := %(notes)s,
                        _east := %(e)s,
                        _west := %(w)s,
                        _north := %(n)s,
                        _south := %(s)s)"""
        if not self.geog:
            latitudeN = None
            longitudeE = None
            latitudeS = None
            longitudeW = None
        else:
            latitudeN = self.geog.latitudeN
            longitudeE = self.geog.longitudeE
            latitudeS = self.geog.latitudeS
            longitudeW = self.geog.longitudeW
        inputs = {
            "sitename": self.sitename,
            "altitude": self.altitude,
            "area": self.area,
            "sitedescription": self.sitedescription,
            "notes": self.notes,
            "n": latitudeN,
            "s": latitudeS,
            "e": longitudeE,
            "w": longitudeW,
        }
        cur.execute(site_query, inputs)
        self.siteid = cur.fetchone()[0]
        return self.siteid

    def upsert_to_db(self, cur):
        """Updates a site that already exists in the database.
        Args:
            cur (psycopg2.cursor): Database cursor.
        Returns:
            int: The siteid.
        """
        site_query = """SELECT upsert_site(_siteid := %(siteid)s,
                                    _sitename := %(sitename)s,
                                    _altitude := %(altitude)s,
                                    _area := %(area)s,
                                    _descript := %(sitedescription)s,
                                    _notes := %(notes)s,
                                    _east := %(ew)s,
                                    _north:= %(ns)s)
                                    """
        if not self.geog:
            latitudeN = None
            longitudeE = None
            latitudeS = None
            longitudeW = None
        else:
            latitudeN = self.geog.latitudeN
            longitudeE = self.geog.longitudeE
            latitudeS = self.geog.latitudeS
            longitudeW = self.geog.longitudeW
        inputs = {
            "sitename": self.sitename,
            "altitude": self.altitude,
            "area": self.area,
            "sitedescription": self.sitedescription,
            "notes": self.notes,
            "ns": latitudeN,
            #"s": latitudeS,
            "ew": longitudeE,
            #"w": longitudeW,
        }
        cur.execute(site_query, inputs)
        self.siteid = cur.fetchone()[0]
        return self.siteid

    def find_close_sites(self, cur, dist=10000, limit=5):
        """Find geographically close sites using PostGIS distance.
        Args:
            cur (psycopg2.cursor): Database cursor.
            dist (float): Distance threshold in meters (default 10km).
            limit (int): Maximum number of sites to return (default 5).
        Returns:
            list: Tuples of site records ordered by distance.
        """
        close_site = """SELECT st.*,
                        ST_SetSRID(st.geog::geometry, 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography AS dist
                        FROM   ndb.sites AS st
                        WHERE ST_SetSRID(st.geog::geometry, 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography < %(dist)s
                        ORDER BY dist
                        LIMIT %(lim)s;"""
        try:
            params = {"long": self.geog.longitudeE,
                      "lat": self.geog.latitudeN,
                      "dist": dist,
                      "lim": limit}
            cur.execute(close_site, params)
            close_sites = cur.fetchall()
            return close_sites
        except Exception as e:
            print(f"Site is not formatted correctly. {e}")
            return None

    def update_site(self, other, overwrite, siteresponse=None):
        """Update site attributes from another site object.
        Args:
            other (Site): Source site for updating.
            overwrite (dict): Dictionary specifying which attributes to overwrite.
            siteresponse (SiteResponse | None): Response object for tracking changes.
        Returns:
            Site: Updated site object.
        """
        if siteresponse is None:
            siteresponse = type("Response", (), {})()  # Create a simple object
            siteresponse.match = {}
            siteresponse.message = []
        attributes = ["sitename",
                      "altitude",
                      "area",
                      "sitedescription",
                      "notes",
                      "geog"]
        updated_attributes = []
        for attr in attributes:
            if getattr(self, attr) != getattr(other, attr):
                siteresponse.matched[attr] = False
                siteresponse.message.append(f"? {attr} does not match."
                                            f" Update set to {overwrite[attr]}.")
            else:
                siteresponse.valid.append(True)
                siteresponse.message.append(f"✔  {attr} match.")
            if overwrite[attr]:
                setattr(self, attr, getattr(other, attr))
                updated_attributes.append(attr)
        return self

    def compare_site(self, other):
        """Compare site attributes with another site object.
        Args:
            other (Site): Site object to compare against.
        Returns:
            list: List of differences found between sites.
        """
        differences = []
        for attr in SITE_PARAMS:
            if _eq(getattr(self, attr), getattr(other, attr)) is False:
                differences.append(f"CSV {attr}: {getattr(self, attr)} != Neotoma {attr}: "
                                   f"{getattr(other, attr)}")
        return differences