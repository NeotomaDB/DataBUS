import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response
from DataBUS.Contact import CONTACT_PARAMS, CONTACT_TABLES

def valid_contact(cur, yml_dict, csv_file):
    """Validates contact information against the Neotoma Paleoecology Database.

    Validates contact data (contact IDs or names) against the Neotoma database
    to ensure they exist and are valid. Matches contact names with database records
    and creates Contact objects with validated parameters.

    Examples:
        >>> valid_contact(cursor, "data.csv", config_dict)  # doctest: +SKIP
        Response(valid=[True, True], message=['Contact John Doe validated', 'Principal Investigator verified'], validAll=True)

    Args:
        cur (psycopg2.extensions.cursor): Cursor pointing to Neotoma database.
        csv_file (str): Path to CSV file containing contact data or user name.
        yml_dict (dict): Dictionary object from template configuration.

    Returns:
        Response: Response object containing validation results and messages.
    """
    response = Response()
    params = CONTACT_PARAMS
    table = CONTACT_TABLES
    inputs = nh.pull_params(params, yml_dict, csv_file, table)

    for i, id in enumerate(inputs):
        if not id['contactid']:
            if isinstance(id.get('contactname', None), list):
                id['contactname'] = list(set(id["contactname"]))
                id['contactname'] = [value for item in id['contactname'] for value in item.split("|")]
                id['contactname'] = list(set(id["contactname"]))
            elif isinstance(id.get('contactname', None), str):
                id['contactname'] = id['contactname'].split("|")
        else:
            id["contactid"] = list(set(id["contactid"]))
        id["table"] = table[i]

    for element in inputs:
        if element.get('contactid') or element.get('contactname'):
            response.message.append(f"  === Checking Against Database - Table: {element['table']}.contactid ===")
        agentname = agentname = element.get("contactname") or element.get("contactid")
        if agentname:
            namematch = []
            for name in agentname:
                familyname = name.split(",")[0].strip()
                firstname = name.split(",")[1].strip() if len(name.split(",")) > 1 else ""
                if '.' in firstname:
                    parts = firstname.strip().split()
                    if len(parts) >= 2 and len(parts[1].replace(".", "")) == 1:
                        firstname = parts[0][0].upper() + "." + parts[1][0].upper() + "."
                    nameQuery = """SELECT contactid, familyname || ', ' || leadinginitials AS fullname
                                   FROM ndb.contacts
                                   WHERE LOWER(familyname) = %(familyname)s AND
                                         LOWER(leadinginitials) ILIKE %(leadinginitials)s;"""
                    cur.execute(nameQuery, {"familyname": familyname.lower(),
                                            "leadinginitials": str(firstname.lower()+'%')})
                else:
                    nameQuery = """SELECT contactid, familyname || ', ' || givennames AS fullname
                               FROM ndb.contacts
                               WHERE LOWER(familyname) = %(familyname)s AND
                                     LOWER(givennames) ILIKE %(givennames)s;"""
                    cur.execute(nameQuery, {"familyname": familyname.lower(),
                                            "givennames": str(firstname.lower()+'%')})
                result = {"name": name.strip(), "match": cur.fetchall()}
                namematch.append(result)
            
            for person in namematch:
                if len(person["match"]) == 0:
                    response.message.append(f"  ✗ No approximate matches found for "
                                            f"{person['name']}. Have they been added to Neotoma?")
                    response.valid.append(False)
                elif any([person["name"] == i[1] for i in person["match"]]):
                    id = next(
                        (number for number, name in person["match"]
                        if name == person["name"]),None)
                    response.message.append(f"  ✔ Exact match found for {person['name']}")
                    response.valid.append("True")
                    try:
                        Contact(contactid=id, contactname=person["name"], order=None)
                        response.valid.append(True)
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"  ✗ Cannot create Contact object: {e}")
                else:
                    response.message.append(
                        f"  ? No exact match found for {person['name']}, but several potential matches follow:"
                    )
                    for i in person["match"]:
                        response.message.append(f"   * {i[1]}")
    return response