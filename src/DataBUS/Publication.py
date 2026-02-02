class Publication:
    """A bibliographic publication reference in Neotoma.

    Stores publication information including journal articles, books,
    and other academic references.

    See the [Neotoma Manual](https://open.neotomadb.org/manual/publication-related-tables-1.html#Publications)

    Attributes:
        publicationid (int | None): Publication ID (assigned after insertion).
        pubtypeid (int | None): Publication type ID.
        year (int | None): Publication year.
        citation (str | None): Citation string.
        title (str | None): Publication title.
        journal (str | None): Journal name.
        vol (str | None): Volume number.
        issue (str | None): Issue number.
        pages (str | None): Page range.
        citnumber (int | None): Citation number.
        doi (str | None): Digital Object Identifier.
        booktitle (str | None): Book title (for chapters).
        numvol (int | None): Number of volumes.
        edition (str | None): Edition information.
        voltitle (str | None): Volume title.
        sertitle (str | None): Series title.
        servol (str | None): Series volume.
        publisher (str | None): Publisher name.
        url (str | None): URL to publication.
        city (str | None): Publication city.
        state (str | None): Publication state.
        country (str | None): Publication country.
        origlang (str | None): Original language.
        notes (str | None): Additional notes.

    Examples:
        >>> pub = Publication(year=2022, title="Holocene pollen", journal="QSR")
        >>> pub.year
        2022
    """
    description = "Publication object in Neotoma"

    def __init__(
        self,
        pubtypeid=None,
        year=None,
        citation=None,
        title=None,
        journal=None,
        vol=None,
        issue=None,
        pages=None,
        citnumber=None,
        doi=None,
        booktitle=None,
        numvol=None,
        edition=None,
        voltitle=None,
        sertitle=None,
        servol=None,
        publisher=None,
        url=None,
        city=None,
        state=None,
        country=None,
        origlang=None,
        notes=None):
        self.publicationid = None
        self.pubtypeid = pubtypeid
        self.year = year
        self.citation = citation
        self.title = title
        self.journal = journal
        self.vol = vol
        self.issue = issue
        self.pages = pages
        self.citnumber = citnumber
        self.doi = doi
        self.booktitle = booktitle
        self.numvol = numvol
        self.edition = edition
        self.voltitle = voltitle
        self.sertitle = sertitle
        self.servol = servol
        self.publisher = publisher
        self.url = url
        self.city = city
        self.state = state
        self.country = country
        self.origlang = origlang
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the publication record into the database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The publicationid assigned by the database.
        """
        # SUGGESTION: Add validation for required fields (year, title, etc.)
        # SUGGESTION: Consider separating journal vs. book publication logic
        publication_query = """SELECT ts.insertpublication(
                        _pubtypeid := %(pubtypeid)s,
                        _year := %(year)s,
                        _citation := %(citation)s,
                        _title := %(title)s,
                        _journal := %(journal)s,
                        _vol := %(vol)s,
                        _issue := %(issue)s,
                        _pages := %(pages)s,
                        _citnumber := %(citnumber)s,
                        _doi := %(doi)s,
                        _booktitle := %(booktitle)s,
                        _numvol := %(numvol)s,
                        _edition := %(edition)s,
                        _voltitle := %(voltitle)s,
                        _sertitle := %(sertitle)s,
                        _servol := %(servol)s,
                        _publisher := %(publisher)s,
                        _url := %(url)s,
                        _city := %(city)s,
                        _state := %(state)s,
                        _country := %(country)s,
                        _origlang := %(origlang)s,
                        _notes := %(notes)s)"""
        inputs = {
            "pubtypeid": self.pubtypeid,
            "year": self.year,
            "citation": self.citation,
            "title": self.title,
            "journal": self.journal,
            "vol": self.vol,
            "issue": self.issue,
            "pages": self.pages,
            "citnumber": self.citnumber,
            "doi": self.doi,
            "booktitle": self.booktitle,
            "numvol": self.numvol,
            "edition": self.edition,
            "voltitle": self.voltitle,
            "sertitle": self.sertitle,
            "servol": self.servol,
            "publisher": self.publisher,
            "url": self.url,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "origlang": self.origlang,
            "notes": self.notes
        }
        cur.execute(publication_query, inputs)
        self.publicationid = cur.fetchone()[0]
        return self.publicationid

    def __str__(self):
        """Return string representation of the Publication object.

        Returns:
            str: String representation.
        """
        return f"Publication(id={self.publicationid}, year={self.year}, title='{self.title}', journal='{self.journal}')"