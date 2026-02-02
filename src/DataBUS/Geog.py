class WrongCoordinates(Exception):
    """Custom exception raised when coordinates are outside valid geographic ranges."""
    pass

class Geog:
    """Geographic coordinates with validation and hemisphere determination.

    Stores latitude and longitude with validation to ensure values are within
    valid geographic ranges. Automatically determines hemisphere from coordinates.

    Attributes:
        latitude (float | None): Latitude value (-90 to 90).
        longitude (float | None): Longitude value (-180 to 180).
        hemisphere (str | None): Cardinal directions ('NE', 'NW', 'SE', 'SW').

    Raises:
        TypeError: If coords is not list/tuple/None, or lat/long not numbers.
        ValueError: If coords length is not 2.
        WrongCoordinates: If coordinates outside valid ranges.

    Examples:
        >>> geog = Geog([43.3734, -71.5316])
        >>> geog.hemisphere
        'NW'
    """

    def __init__(self, coords):
        if not (isinstance(coords, (list, tuple)) or coords is None):
            raise TypeError("✗ Coordinates must be a list or a tuple")
        if coords is None or coords == [None, None]:
            self.latitude = None
            self.longitude = None
        else:
            if len(coords) != 2:
                raise ValueError("✗ Coordinates must have a length of 2.")
            if not (isinstance(coords[0], (int, float)) or coords[0] is None):
                raise TypeError("✗ Latitude must be a number or None.")
            if not (isinstance(coords[1], (int, float)) or coords[1] is None):
                raise TypeError("✗ Longitude must be a number or None.")
            if isinstance(coords[0], (int, float)) and not (-90 <= coords[0] <= 90):
                raise WrongCoordinates("✗ Latitude must be between -90 and 90.")
            if isinstance(coords[1], (int, float)) and not (-180 <= coords[1] <= 180):
                raise WrongCoordinates("✗ Longitude must be between -180 and 180.")
            self.latitude = coords[0]
            self.longitude = coords[1]
        if self.latitude is not None and self.longitude is not None:
            self.hemisphere = ("N" if self.latitude >= 0 else "S") + (
                "E" if self.longitude >= 0 else "W"
            )
        else:
            self.hemisphere = None

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
            return self.latitude == other.latitude and self.longitude == other.longitude

    def __str__(self):
        """Return string representation of geographic coordinates.

        Returns:
            str: Formatted string showing latitude and longitude.
        """
        return f"(Lat:{self.latitude}, Long: {self.longitude})"