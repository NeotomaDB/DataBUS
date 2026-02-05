import warnings
from .neotomaHelpers.eq import _eq

class WrongCoordinates(Exception):
    """Custom exception raised when coordinates are outside valid geographic ranges."""
    pass

class Geog:
    """Geographic coordinates with validation and hemisphere determination.

    Stores latitude and longitude with validation to ensure values are within
    valid geographic ranges. Automatically determines hemisphere from coordinates.

    Attributes:
        longe (float | None): Longitude in decimal degrees (-180 to 180).
        latn (float | None): Latitude in decimal degrees (-90 to 90).
        longw (float | None): Longitude in decimal degrees (-180 to 180).
        lats (float | None): Latitude in decimal degrees (-90 to 90).
        hemisphere (str | None): Cardinal directions ('NE', 'NW', 'SE', 'SW').

    Raises:
        TypeError: If coords is not list/tuple/None, or lat/long not numbers.
        ValueError: If coords length is not 2.
        WrongCoordinates: If coordinates outside valid ranges.

    Examples:
        >>> geog = Geog([43.3734, -71.5316, 43.3734, -71.5316])
        >>> geog.hemisphere
        'NW'
    """

    def __init__(self, coords):
        if not (isinstance(coords, (list, tuple)) or coords is None):
            raise TypeError("✗ Coordinates must be a list or a tuple")
         # check if list where all values are None
        if coords == None or all(v is None for v in coords):
            self.latitudeN = None
            self.longitudeE = None
            self.latitudeS = None
            self.longitudeW = None
            self.hemisphere = None
        else:
            if len(coords) != 2 and len(coords) != 4:
                raise ValueError("✗ Coordinates must have a length of 2 or 4.")
        try:
            lat, lon, lat2, lon2 = coords
        except ValueError:
            lat, lon = coords
            lat2, lon2 = lat, lon
        if lat is None or lon is None:
            warnings.warn("? No coordinates given.")
        for val, name, lo, hi in [
            (lat, "LatN", -90, 90),
            (lon, "LongE", -180, 180),
            (lat2, "LatS", -90, 90),
            (lon2, "LongW", -180, 180)]:
            if val is not None:
                if not isinstance(val, (int, float)):
                    raise TypeError(f"✗ {name} must be a number or None.")
                if not (lo <= val <= hi):
                    raise WrongCoordinates(f"✗ {name} must be between {lo} and {hi}.")
        self.latitudeN = lat
        self.longitudeE = lon
        self.latitudeS = lat2
        self.longitudeW = lon2
        if self.latitudeN is not None and self.longitudeE is not None:
            self.hemisphere = ("N" if self.latitudeN >= 0 else "S") + (
                "E" if self.longitudeE >= 0 else "W")

    def __eq__(self, other):
        """Compare two Geog objects for equality based on coordinates.
        Args:
            other (Geog): Another Geog object to compare.
        Returns:
            bool: True if both objects have identical latitude and longitude.
        """
        if not isinstance(other, Geog):
            return False
        else:
            return (_eq(self.latitudeN, other.latitudeN) and
                    _eq(self.longitudeE, other.longitudeE) and
                    _eq(self.latitudeS, other.latitudeS) and
                    _eq(self.longitudeW, other.longitudeW))

    def __str__(self):
        """Return string representation of geographic coordinates.

        Returns:
            str: Formatted string showing latitude and longitude.
        """
        return f"(Lat: {self.latitudeN}, Long: {self.longitudeE})"