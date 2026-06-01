from .neotomaHelpers.utils import validate_int_values

PROJECT_PARAMS = [
    "projectname",
    "projectdescription",
    "projectstartdate",
    "projectenddate",
]


class Project:
    """A research project record in the Neotoma database.

    Attributes:
        projectid (int | None): Database ID (assigned after insertion).
        parentprojectid (int | None): Parent project ID for sub-projects.
        projectname (str | None): Project name.
        projectdescription (str | None): Project description.
        projectstartdate (str | None): Start date.
        projectenddate (str | None): End date.

    Examples:
        >>> p = Project(projectname="FAIRe eDNA Survey")
        >>> p.projectname
        'FAIRe eDNA Survey'
    """

    def __init__(
        self,
        projectname=None,
        projectdescription=None,
        projectstartdate=None,
        projectenddate=None,
        parentprojectid=None,
    ):
        self.projectid = None
        self.parentprojectid = validate_int_values(parentprojectid, "parentprojectid")
        if projectname is not None and not isinstance(projectname, str):
            raise TypeError("projectname must be a string or None.")
        self.projectname = projectname
        if projectdescription is not None and not isinstance(projectdescription, str):
            raise TypeError("projectdescription must be a string or None.")
        self.projectdescription = projectdescription
        self.projectstartdate = projectstartdate
        self.projectenddate = projectenddate

    def insert_to_db(self, cur):
        """Insert the project record into the Neotoma database.

        Returns:
            int: The projectid assigned by the database.
        """
        q = """
            INSERT INTO ndb.projects
                (parentprojectid, projectname, projectdescription,
                 projectstartdate, projectenddate)
            VALUES
                (%(parentprojectid)s, %(projectname)s, %(projectdescription)s,
                 %(projectstartdate)s, %(projectenddate)s)
            RETURNING projectid;
        """
        inputs = {
            "parentprojectid": self.parentprojectid,
            "projectname": self.projectname,
            "projectdescription": self.projectdescription,
            "projectstartdate": self.projectstartdate,
            "projectenddate": self.projectenddate,
        }
        cur.execute(q, inputs)
        self.projectid = cur.fetchone()[0]
        return self.projectid

    def __str__(self):
        return f"Project(projectid={self.projectid}, projectname={self.projectname})"


class Grant:
    """A research grant record in the Neotoma database.

    Attributes:
        grantid (int | None): Database ID (assigned after insertion).
        grantname (str | None): Grant name.
        grantnumber (str | None): Grant number / identifier.
        dateawarded (str | None): Date the grant was awarded.
        dateended (str | None): Date the grant ended.

    Examples:
        >>> g = Grant(grantname="NSF BIO", grantnumber="2055632")
        >>> g.grantnumber
        '2055632'
    """

    def __init__(
        self,
        grantname=None,
        grantnumber=None,
        dateawarded=None,
        dateended=None,
    ):
        self.grantid = None
        if grantname is not None and not isinstance(grantname, str):
            raise TypeError("grantname must be a string or None.")
        self.grantname = grantname
        if grantnumber is not None and not isinstance(grantnumber, str):
            raise TypeError("grantnumber must be a string or None.")
        self.grantnumber = grantnumber
        self.dateawarded = dateawarded
        self.dateended = dateended

    def insert_to_db(self, cur):
        """Insert the grant record into the Neotoma database.

        Returns:
            int: The grantid assigned by the database.
        """
        q = """
            INSERT INTO ndb.grants (grantname, grantnumber, dateawarded, dateended)
            VALUES (%(grantname)s, %(grantnumber)s, %(dateawarded)s, %(dateended)s)
            RETURNING grantid;
        """
        inputs = {
            "grantname": self.grantname,
            "grantnumber": self.grantnumber,
            "dateawarded": self.dateawarded,
            "dateended": self.dateended,
        }
        cur.execute(q, inputs)
        self.grantid = cur.fetchone()[0]
        return self.grantid

    def __str__(self):
        return f"Grant(grantid={self.grantid}, grantname={self.grantname})"


class Institution:
    """An institution record in the Neotoma database.

    The institutionid is the RoR (Research Organization Registry) identifier,
    which is a text field rather than an auto-generated integer.

    Attributes:
        institutionid (str | None): RoR identifier (PK).
        institutionname (str | None): Institution name.
        institutionlocation (str | None): Institution location.

    Examples:
        >>> i = Institution(institutionid="https://ror.org/01y2jtd41",
        ...                 institutionname="University of Wisconsin-Madison")
        >>> i.institutionname
        'University of Wisconsin-Madison'
    """

    def __init__(
        self,
        institutionid=None,
        institutionname=None,
        institutionlocation=None,
    ):
        if institutionid is not None and not isinstance(institutionid, str):
            raise TypeError("institutionid must be a string or None.")
        self.institutionid = institutionid
        if institutionname is not None and not isinstance(institutionname, str):
            raise TypeError("institutionname must be a string or None.")
        self.institutionname = institutionname
        if institutionlocation is not None and not isinstance(institutionlocation, str):
            raise TypeError("institutionlocation must be a string or None.")
        self.institutionlocation = institutionlocation

    def insert_to_db(self, cur):
        """Insert the institution record into the Neotoma database.

        Uses ON CONFLICT to handle upserts since institutionid (RoR) may
        already exist.

        Returns:
            str: The institutionid.
        """
        q = """
            INSERT INTO ndb.institutions (institutionid, institutionname, institutionlocation)
            VALUES (%(institutionid)s, %(institutionname)s, %(institutionlocation)s)
            ON CONFLICT (institutionid) DO UPDATE
                SET institutionname = EXCLUDED.institutionname,
                    institutionlocation = EXCLUDED.institutionlocation
            RETURNING institutionid;
        """
        inputs = {
            "institutionid": self.institutionid,
            "institutionname": self.institutionname,
            "institutionlocation": self.institutionlocation,
        }
        cur.execute(q, inputs)
        self.institutionid = cur.fetchone()[0]
        return self.institutionid

    def __str__(self):
        return f"Institution(institutionid={self.institutionid}, name={self.institutionname})"


# ── Junction table helpers ────────────────────────────────────────────────────


def insert_project_dataset(cur, projectid, datasetid):
    """Link a project to a dataset via ndb.projectdatasets."""
    q = """
        INSERT INTO ndb.projectdatasets (projectid, datasetid)
        VALUES (%(projectid)s, %(datasetid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"projectid": projectid, "datasetid": datasetid})


def insert_project_grant(cur, projectid, grantid):
    """Link a project to a grant via ndb.projectgrants."""
    q = """
        INSERT INTO ndb.projectgrants (projectid, grantid)
        VALUES (%(projectid)s, %(grantid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"projectid": projectid, "grantid": grantid})


def insert_project_participant(cur, projectid, contactid):
    """Link a project to a contact via ndb.projectparticipants."""
    q = """
        INSERT INTO ndb.projectparticipants (projectid, contactid)
        VALUES (%(projectid)s, %(contactid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"projectid": projectid, "contactid": contactid})


def insert_project_keyword(cur, projectid, keywordid):
    """Link a project to a keyword via ndb.projectkeywords."""
    q = """
        INSERT INTO ndb.projectkeywords (projectid, keywordid)
        VALUES (%(projectid)s, %(keywordid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"projectid": projectid, "keywordid": keywordid})


def insert_funding_institution(cur, grantid, institutionid):
    """Link a grant to a funding institution via ndb.fundinginstitutions."""
    q = """
        INSERT INTO ndb.fundinginstitutions (institutionid, grantid)
        VALUES (%(institutionid)s, %(grantid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"institutionid": institutionid, "grantid": grantid})


def insert_contact_institution(cur, contactid, institutionid):
    """Link a contact to an institution via ndb.contactinstitutions."""
    q = """
        INSERT INTO ndb.contactinstitutions (institutionid, contactid)
        VALUES (%(institutionid)s, %(contactid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"institutionid": institutionid, "contactid": contactid})


def insert_grant_awardee(cur, grantid, contactid):
    """Link a grant to an awardee contact via ndb.grantawardees."""
    q = """
        INSERT INTO ndb.grantawardees (grantid, contactid)
        VALUES (%(grantid)s, %(contactid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(q, {"grantid": grantid, "contactid": contactid})


def insert_external_grant(cur, grantid, identifier, extdatabaseid):
    """Link a grant to an external database via ndb.externalgrants."""
    q = """
        INSERT INTO ndb.externalgrants (grantid, identifier, extdatabaseid)
        VALUES (%(grantid)s, %(identifier)s, %(extdatabaseid)s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(
        q,
        {"grantid": grantid, "identifier": identifier, "extdatabaseid": extdatabaseid},
    )
