from .neotomaHelpers.utils import validate_int_values
DATASET_PARAMS = [("datasetname", "ndb.datasets.datasetname"),
                  ("datasettypeid", "ndb.datasettypes.datasettypeid")]

class Dataset:
    """A dataset in Neotoma.

    A collection of data (e.g., pollen counts, isotope values) of a specific
    type associated with a collection unit.

    Datasets are explained further in the
    [Neotoma Manual](https://open.neotomadb.org/manual/dataset-collection-related-tables-1.html#Datasets).

    Attributes:
        datasetid (int | None): Dataset ID (assigned after insertion).
        collectionunitid (int | None): Collection unit ID.
        datasettypeid (int): Dataset type ID (required).
        datasetname (str | None): Dataset name.
        notes (str | None): Additional notes.

    Raises:
        ValueError: If datasettypeid is not an integer.

    Examples:
        >>> dataset = Dataset(datasettypeid=1, datasetname="Pollen Core")
        >>> dataset.datasettypeid
        1
    """

    def __init__(
        self,
        datasettypeid,
        datasetid=None,
        collectionunitid=None,
        datasetname=None,
        notes=None):
        #if collunit is none or datasettypeid is none, fail creation
        if datasettypeid is None:
            raise ValueError("Datasettype ID is required to create a Dataset.")
        if collectionunitid is None:
            raise ValueError("Collection Unit ID is required to create a Dataset.")
        self.datasetid = validate_int_values(datasetid, "datasetid")
        self.collectionunitid = validate_int_values(collectionunitid, "collectionunitid")
        self.datasettypeid = validate_int_values(datasettypeid, "datasettypeid")
        self.datasetname = datasetname
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the dataset record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The datasetid assigned by the database.
        """
        dataset_query = """
        SELECT ts.insertdataset(_collectionunitid:= %(collunitid)s,
                                _datasettypeid := %(datasettypeid)s,
                                _datasetname := %(datasetname)s,
                                _notes := %(notes)s);
                        """
        inputs = {
            "collunitid": self.collectionunitid,
            "datasettypeid": self.datasettypeid,
            "datasetname": self.datasetname,
            "notes": self.notes,
        }
        cur.execute(dataset_query, inputs)
        self.datasetid = cur.fetchone()[0]
        return self.datasetid

    def __str__(self):
        """Return string representation of the Dataset object.

        Returns:
            str: String representation.
        """
        return f"Dataset(id={self.datasetid}, name='{self.datasetname}', typeid={self.datasettypeid})"