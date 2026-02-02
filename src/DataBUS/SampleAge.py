SAMPLE_AGE_PARAMS = ["age", "ageyounger", "ageolder", "agetype", "agemodel"]

class SampleAge:
    """Age information for a sample in the Neotoma database.

    Stores age estimates for a sample within a specific chronology,
    including age bounds.
    
    See the [Neotoma Manual](https://open.neotomadb.org/manual/sample-related-tables-1.html#SampleAges)
    
    Attributes:
        sampleid (int | None): Sample identifier.
        chronologyid (int | None): Chronology identifier.
        age (float | None): Age estimate.
        ageyounger (float | None): Younger age bound.
        ageolder (float | None): Older age bound.
        sampleage (int | None): Database ID (assigned after insertion).

    Examples:
        >>> sample_age = SampleAge(sampleid=1, chronologyid=2, age=75)
        >>> sample_age.age
        75
    """

    def __init__(
        self, sampleid=None, chronologyid=None, age=None, ageyounger=None, ageolder=None
    ):
        self.sampleid = sampleid
        self.chronologyid = chronologyid
        self.age = age
        self.ageyounger = ageyounger
        self.ageolder = ageolder

    def insert_to_db(self, cur):
        """Insert the sample age record into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The sampleage ID assigned by the database.
        """
        # SUGGESTION: Add validation to ensure age bounds are logically consistent (younger < age < older)
        # SUGGESTION: Consider logging age values for debugging purposes
        sample_q = """
        SELECT ts.insertsampleage(_sampleid := %(sampleid)s,
                                  _chronologyid := %(chronologyid)s,
                                  _age := %(age)s,
                                  _ageyounger := %(ageyounger)s,
                                  _ageolder := %(ageolder)s)
                        """
        inputs = {
            "sampleid": self.sampleid,
            "chronologyid": self.chronologyid,
            "age": self.age,
            "ageyounger": self.ageyounger,
            "ageolder": self.ageolder,
        }

        cur.execute(sample_q, inputs)
        self.sampleage = cur.fetchone()[0]
        return self.sampleage

    def __str__(self):
        """Return string representation of the SampleAge object.

        Returns:
            str: String representation.
        """
        return f"SampleAge(sampleid={self.sampleid}, age={self.age}, range=[{self.ageyounger}-{self.ageolder}])"
