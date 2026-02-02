class Contact:
    """A person who participated in data collection or processing in Neotoma.

    Manages contact information and roles in paleoenvironmental research
    including data processing, sample analysis, and field collection.

    Attributes:
        contactid (int): Contact ID.
        contactname (str | None): Contact name.
        order (int | None): Order/sequence in list of contacts.

    Raises:
        ValueError: If contactid is not int or None, or order is not int or None.

    Examples:
        >>> contact = Contact(contactid=1, contactname="Simon Goring", order=1)
        >>> contact.contactname
        'Simon Goring'
    """

    def __init__(self, contactid, contactname=None, order=None):
        if isinstance(contactid, int) or contactid is None:
            self.contactid = contactid
        else:
            raise ValueError("ContactID must be an integer.")
        self.contactname = contactname
        if isinstance(order, int) or order is None:
            self.order = order
        else:
            raise ValueError("Order must be an integer or None.")

    def insert_pi(self, cur, datasetid):
        """Insert contact as principal investigator for a dataset.

        Args:
            cur (psycopg2.cursor): Database cursor.
            datasetid (int): Dataset identifier.

        Returns:
            None
        """
        if not isinstance(datasetid, int):
            raise ValueError("DatasetID must be an integer")

        pi_query = """
        SELECT ts.insertdatasetpi(_datasetid := %(datasetid)s,
                                  _contactid := %(contactid)s,
                                  _piorder := %(piorder)s);
                        """
        inputs = {
            "datasetid": datasetid,
            "contactid": self.contactid,
            "piorder": self.order,
        }
        cur.execute(pi_query, inputs)
        return

    def insert_data_processor(self, cur, datasetid):
        """Insert contact as data processor for a dataset.

        Args:
            cur (psycopg2.cursor): Database cursor.
            datasetid (int): Dataset identifier.

        Returns:
            None
        """
        processor = """
                SELECT ts.insertdataprocessor(_datasetid := %(datasetid)s,
                                              _contactid := %(contactid)s)
                    """
        inputs = {"datasetid": datasetid, "contactid": self.contactid}
        cur.execute(processor, inputs)
        return

    def insert_sample_analyst(self, cur, sampleid):
        """Insert contact as sample analyst.

        Args:
            cur (psycopg2.cursor): Database cursor.
            sampleid (int): Sample identifier.

        Returns:
            None
        """
        sa_query = """
                   SELECT ts.insertsampleanalyst(_sampleid := %(sampleid)s,
                                                 _contactid := %(contactid)s,
                                                 _analystorder := %(analystorder)s)
                    """
        inputs = {
            "sampleid": sampleid,
            "contactid": self.contactid,
            "analystorder": self.order,
        }
        cur.execute(sa_query, inputs)
        return None

    def insert_collector(self, cur, collunitid):
        """Insert contact as field collector for a collection unit.

        Args:
            cur (psycopg2.cursor): Database cursor.
            collunitid (int): Collection unit identifier.

        Returns:
            None
        """
        if not isinstance(collunitid, int):
            raise ValueError("CollectionUnitID must be an integer")

        collector_query = """
        SELECT ts.insertcollector(_collunitid := %(collunitid)s,
                           _contactid := %(contactid)s,
                           _collectororder := %(collectororder)s);
                        """
        inputs = {
            "collunitid": collunitid,
            "contactid": self.contactid,
            "collectororder": self.order,
        }
        cur.execute(collector_query, inputs)
        return

    def __str__(self):
        """Return string representation of the Contact object.

        Returns:
            str: String representation.
        """
        return f"Contact(id={self.contactid}, name='{self.contactname}', order={self.order})"