from .neotomaHelpers.utils import validate_int_values, validate_str_values


class AeDNAEntity:
    """Base for the flat aeDNA tables (ndb.aednaassays, ndb.aednalibraries).

    Subclasses declare four class attributes:

    * ``_TABLE`` — fully-qualified table name.
    * ``_PARAMS`` — ordered list of insert columns.
    * ``_PK`` — primary-key column name (returned by the insert).
    * ``_INT_FIELDS`` — the ``_PARAMS`` that are integer FKs/sizes; every other
      param is validated as a string.

    Construction validates each field and stores it as an attribute; ``_PK`` is
    initialised to ``None`` and set after :meth:`insert_to_db`.
    """

    _TABLE: str = ""
    _PARAMS: list[str] = []
    _PK: str = ""
    _INT_FIELDS: tuple[str, ...] = ()

    def __init__(self, **fields):
        setattr(self, self._PK, None)
        for name in self._PARAMS:
            value = fields.get(name)
            if name in self._INT_FIELDS:
                value = validate_int_values(value, name)
            else:
                value = validate_str_values(value, name)
            setattr(self, name, value)

    def insert_to_db(self, cur):
        """Insert the record into ``_TABLE`` and return the new ``_PK`` value."""
        cols = self._PARAMS
        query = (
            f"INSERT INTO {self._TABLE} ({', '.join(cols)}) "
            f"VALUES ({', '.join('%(' + c + ')s' for c in cols)}) "
            f"RETURNING {self._PK};"
        )
        cur.execute(query, {c: getattr(self, c) for c in cols})
        pk = cur.fetchone()[0]
        setattr(self, self._PK, pk)
        return pk
