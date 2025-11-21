import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response

def valid_contact2(cur, csv_file, yml_dict):
    """
    Validate contact information against the Neotoma Paleoecology Database.

    This function checks the provided contact information (either contact IDs or contact names) 
    against the Neotoma Paleoecology Database to ensure they exist and are valid. It processes 
    the input data, queries the database for matches, and returns a response object containing 
    validation results and messages.
    
    Parameters:
        cur (psycopg2.extensions.cursor): A cursor pointing to the Neotoma Paleoecology Database.
        csv_file (str): A user name or individual.
        yml_dict (dict): The dictionary object passed by template_to_dict.

    Returns:
        Response: An object containing validation results and messages.
    """
    response = Response()

    params = ["contactid", "contactname"] 
    table = ["ndb.datasetpis", "ndb.collectors",
             "ndb.sampleanalysts", "ndb.datasetprocessor",
              "ndb.chronologies"]
    inputs = nh.pull_params(params, yml_dict, csv_file, table)
    for i, entry in enumerate(inputs):
        cid = entry.get("contactid")
        if not cid:
            # Handle contactname if contactid is missing
            if isinstance(entry.get('contactname'), list):
                entry['contactname'] = list(set(entry['contactname']))
                entry['contactname'] = [value for item in entry['contactname'] for value in item.split("|")]
                entry['contactname'] = list(set(entry['contactname']))
            elif isinstance(entry.get('contactname'), str):
                entry['contactname'] = entry['contactname'].split("|")
        else:
            # Normalize contactid into list of unique strings
            if isinstance(cid, int):
                cid_list = [str(cid)]
            elif isinstance(cid, str):
                cid_list = [c.strip() for c in cid.split(",") if c.strip()]
            elif isinstance(cid, list):
                cid_list = [str(c) for c in cid]
            else:
                cid_list = []
            entry["contactid"] = list(set(cid_list))
        entry["table"] = table[i]

    for element in inputs:
        if element.get('contactid'):
            response.message.append(f"  === Checking Against Database - Table: {element['table']}.contactid ===")
            agentid = element.get('contactid')
            idmatch =[]
            for id in agentid:
                idQuery = """SELECT contactid, familyname || ', ' || givennames AS fullname
                             FROM ndb.contacts
                             WHERE contactid = %(contactid)s;"""
                cur.execute(idQuery, {"contactid": int(id)})
                result = {"id": id, "match": cur.fetchall()}
                idmatch.append(result)
            for person in idmatch:
                if len(person["match"]) == 0:
                    response.message.append(f"  ✗ No approximate matches found for "
                                            f"{person['name']}. Have they been added to Neotoma?")
                    response.valid.append(False)
                elif any([int(person["id"]) == i[0] for i in person["match"]]):
                    id = next(
                        (number for number, name in person["match"]
                        if number == int(person["id"])),None)
                    response.message.append(f"  ✔ Exact match found for {person['match'][0][1]}")
                    response.valid.append("True")
                    try:
                        Contact(contactid=int(person["id"]), order=None)
                        response.valid.append(True)
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"  ✗ Cannot create Contact object: {e}")
                else:
                    response.message.append(
                        f"  ? No exact match found for {person['id']}, but several potential matches follow:"
                    )
                    for i in person["id"]:
                        response.message.append(f"   * {i[1]}")

        elif element.get('contactname'):
            response.message.append(f"  === Checking Against Database - Table: {element['table']}.contactid ===")
            agentname = element.get("contactname")
            if agentname:
                namematch = []
                for name in agentname:
                    familyname = name.split(",")[0].strip()
                    firstname = name.split(",")[1].strip() if len(name.split(",")) > 1 else ""
                    if '.' in firstname:
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
    response.validAll = all(response.valid)
    return response 