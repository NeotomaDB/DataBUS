def get_contacts(cur, name):
    """Retrieve contact information from database using various name matching strategies.

    Queries the Neotoma database for contacts using multiple matching strategies:
    first tries abbreviated names (with leading initials), then full given names,
    then contact name, and finally surname matching.

    Examples:
        >>> get_contacts(cursor, ['Doe, John'])  # doctest: +SKIP
        [{'name': 'doe, john', 'id': 123, 'order': 1}]
        >>> get_contacts(cursor, ['Doe, Jane'])  # doctest: +SKIP
        [{'name': 'doe, jane', 'id': 456, 'order': 2}]

    Args:
        cur: Database cursor object for executing SQL queries.
        contacts_list (list): List of contact names in format 'Surname, Firstname' or similar.

    Returns:
        list: List of dictionaries with keys:
              'name' (str): Contact name as found in database.
              'id' (int): Contact ID from database, or None if not found.
              'order' (int): Original position in contacts_list, or None if not found.
    """
    name = name.strip()
    get_contact = """SELECT contactid, familyname || ', ' || givennames AS fullname
                     FROM ndb.contacts
                     WHERE LOWER(contactname) = %(contactname)s;"""
    cur.execute(get_contact, {"contactname": name.lower()})
    data = cur.fetchone()
    contact = {}
    if data:
        contact["name"] = data[1]
        contact["id"] = data[0]
        return contact
    familyname = name.split(",")[0].strip()
    firstname = name.split(",")[1].strip() if len(name.split(",")) > 1 else ""
    if "." in firstname:
        get_contact = """SELECT contactid, familyname || ', ' || leadinginitials AS fullname
                         FROM ndb.contacts
                         WHERE LOWER(familyname) = %(familyname)s AND
                         LOWER(leadinginitials) ILIKE %(leadinginitials)s;"""
        cur.execute(
            get_contact,
            {"familyname": familyname.lower(), "leadinginitials": str(firstname.lower() + "%")},
        )
    else:
        get_contact = """SELECT contactid, familyname || ', ' || givennames AS fullname
                         FROM ndb.contacts
                         WHERE LOWER(familyname) = %(familyname)s AND
                         LOWER(givennames) ILIKE %(givennames)s;"""
        cur.execute(
            get_contact,
            {"familyname": familyname.lower(), "givennames": str(firstname.lower() + "%")},
        )
    data = cur.fetchone()
    contact = {}
    if data:
        contact["name"] = data[1]
        contact["id"] = data[0]
    else:
        contact["name"] = name
        contact["id"] = None
    return contact
