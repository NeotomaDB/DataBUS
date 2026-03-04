from .neotomaHelpers.utils import validate_int_values


class DatasetDatabase:
    """A link between a dataset and a constituent database in Neotoma.

    Associates a dataset with a constituent database,
    enabling tracking of dataset provenance.

    Attributes:
        databaseid (int): Constituent database ID.
        datasetid (int | None): Dataset ID.

    Raises:
        ValueError: If databaseid or datasetid is not int or None.

    Examples:
        >>> ds_db = DatasetDatabase(databaseid=1, datasetid=2)
        >>> ds_db.databaseid
        1
    """

    def __init__(self, databaseid, datasetid=None):
        if databaseid is None or datasetid is None:
            raise ValueError(
                "Both databaseid and datasetid are required to create a DatasetDatabase."
            )
        self.datasetid = validate_int_values(datasetid, "datasetid")
        self.databaseid = validate_int_values(databaseid, "databaseid")

    def insert_to_db(self, cur):
        """Insert the dataset-database relationship into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        db_query = """
               SELECT ts.insertdatasetdatabase(_datasetid := %(datasetid)s,
                                               _databaseid := %(databaseid)s)
               """
        inputs = {"datasetid": self.datasetid, "databaseid": self.databaseid}
        cur.execute(db_query, inputs)
        return

    def __str__(self):
        """Return string representation of the DatasetDatabase object.

        Returns:
            str: String representation.
        """
        return f"DatasetDatabase(datasetid={self.datasetid}, databaseid={self.databaseid})"
