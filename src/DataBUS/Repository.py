class Repository:
    """A physical specimens repository for a dataset in Neotoma.

    Associates a dataset with a repository where physical specimens
    (e.g., cores, samples) are archived and curated.

    Attributes:
        datasetid (int): Dataset ID (required).
        repositoryid (int | None): Repository ID.
        notes (str | None): Notes about specimen storage.

    Raises:
        ValueError: If datasetid is not int, or repositoryid not int or None.

    Examples:
        >>> repo = Repository(datasetid=1, repositoryid=2)
        >>> repo.repositoryid
        2
    """

    def __init__(self, datasetid, repositoryid, notes=None):
        if isinstance(datasetid, int):
            self.datasetid = datasetid
        else:
            raise ValueError("Dataset ID must be an integer.")
        self.datasetid = datasetid
        if isinstance(repositoryid, int) or repositoryid is None:
            self.repositoryid = repositoryid
        else:
            raise ValueError("Specimens Repository ID must be an integer or None.")
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the dataset-repository relationship into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            None
        """
        repo_query = """
            SELECT ts.insertdatasetrepository(_datasetid := %(datasetid)s,
                                               _repositoryid := %(repositoryid)s,
                                               _notes := %(notes)s)
                                            """
        inputs = {
            "datasetid": self.datasetid,
            "repositoryid": self.repositoryid,
            "notes": self.notes,
        }
        cur.execute(repo_query, inputs)
        return

    def __str__(self):
        """Return string representation of the Repository object.

        Returns:
            str: String representation.
        """
        return f"Repository(datasetid={self.datasetid}, repositoryid={self.repositoryid})"