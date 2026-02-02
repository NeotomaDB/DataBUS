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
        if isinstance(datasetid, int) or datasetid is None:
            self.datasetid = datasetid
        else:
            raise ValueError("Dataset ID must be an integer.")
        self.datasetid = datasetid
        if isinstance(databaseid, int) or databaseid is None:
            self.databaseid = databaseid
        else:
            raise ValueError("DatabaseID must be an integer.")

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