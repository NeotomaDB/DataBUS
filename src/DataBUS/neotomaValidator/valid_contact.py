import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response
from DataBUS.Contact import CONTACT_PARAMS, CONTACT_TABLES

def valid_contact(cur, yml_dict, csv_file):
    """Validates contact information against the Neotoma Paleoecology Database.

    Validates contact data (contact IDs or names) against the Neotoma database
    to ensure they exist and are valid. Matches contact names with database records
    and creates Contact objects with validated parameters.

    Args:
        cur (psycopg2.extensions.cursor): Cursor pointing to Neotoma database.
        yml_dict (dict): Dictionary object from template configuration.
        csv_file (str): Path to CSV file containing contact data or user name.

    Returns:
        Response: Response object containing validation results and messages.

    Examples:
        >>> valid_contact(cursor, yml_dict, "data.csv")
        Response(valid=[True, True], message=[...], validAll=True)
    """
    response = Response()
    inputs = nh.pull_params(CONTACT_PARAMS, yml_dict, csv_file, CONTACT_TABLES)

    for i, entry in enumerate(inputs):
        if not entry['contactid']:
            if isinstance(entry.get('contactname'), list):
                entry['contactname'] = list(set(entry["contactname"]))
                entry['contactname'] = [value for item in entry['contactname']
                                        for value in item.split("|")]
                entry['contactname'] = list(set(entry["contactname"]))
            elif isinstance(entry.get('contactname'), str):
                entry['contactname'] = entry['contactname'].split("|")
        else:
            entry["contactid"] = list(set(entry["contactid"]))
        entry["table"] = table[i]

    for element in inputs:
        if element.get('contactid') or element.get('contactname'):
            response.message.append(
                f"  === Checking Against Database - Table: {element['table']}.contactid ==="
            )
        agentname = element.get("contactname") or element.get("contactid")
        if agentname:
            namematch = []
            for name in agentname:
                familyname = name.split(",")[0].strip()
                firstname = name.split(",")[1].strip() if len(name.split(",")) > 1 else ""
                if '.' in firstname:
                    parts = firstname.strip().split()
                    if len(parts) >= 2 and len(parts[1].replace(".", "")) == 1:
                        firstname = parts[0][0].upper() + "." + parts[1][0].upper() + "."
                    name_query = """SELECT contactid, familyname || ', ' || leadinginitials AS fullname
                                    FROM ndb.contacts
                                    WHERE LOWER(familyname) = %(familyname)s AND
                                          LOWER(leadinginitials) ILIKE %(leadinginitials)s;"""
                    cur.execute(name_query, {"familyname": familyname.lower(),
                                             "leadinginitials": str(firstname.lower() + '%')})
                else:
                    name_query = """SELECT contactid, familyname || ', ' || givennames AS fullname
                                    FROM ndb.contacts
                                    WHERE LOWER(familyname) = %(familyname)s AND
                                          LOWER(givennames) ILIKE %(givennames)s;"""
                    cur.execute(name_query, {"familyname": familyname.lower(),
                                             "givennames": str(firstname.lower() + '%')})
                result = {"name": name.strip(), "match": cur.fetchall()}
                namematch.append(result)

            for person in namematch:
                if len(person["match"]) == 0:
                    response.message.append(f"  ✗ No approximate matches found for "
                                            f"{person['name']}. Have they been added to Neotoma?")
                    response.valid.append(False)
                elif any(person["name"] == match[1] for match in person["match"]):
                    contact_id = next(
                        (number for number, name in person["match"]
                         if name == person["name"]), None)
                    response.message.append(f"  ✔ Exact match found for {person['name']}")
                    response.valid.append(True)
                    try:
                        Contact(contactid=contact_id, contactname=person["name"], order=None)
                        response.valid.append(True)
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"  ✗ Cannot create Contact object: {e}")
                else:
                    response.message.append(
                        f"  ? No exact match found for {person['name']}, "
                        f"but several potential matches follow:"
                    )
                    for match in person["match"]:
                        response.message.append(f"   * {match[1]}")
    return response
