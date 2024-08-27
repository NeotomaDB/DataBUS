import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response


def valid_contact(cur, csv_template, yml_dict):
    """_Get user agent or contact from Neotoma_

    Args:
        cur (_psycopg2.extensions.cursor_): _A cursor pointing to the Neotoma Paleoecology Database._
        csv_template (_string_): _A user name or individual._
        yml_dict (_dict_): _The dictionary object passed by template_to_dict._
    """
    response = Response()

    params = ["contactid", "contactname"]
    table = ["ndb.datasetpis", "ndb.sampleanalysts", "ndb.chronologies"]
    inputs = nh.pull_params(params, yml_dict, csv_template, table)

    for i, id in enumerate(inputs):
        id["contactid"] = list(set(id["contactid"]))
        id["contactname"] = list(set(id["contactname"]))
        id["table"] = table[i]
    for element in inputs:
        response.message.append(
            f"  === Checking Against Database - Table: {element['table']} ==="
        )
        agentname = element["contactid"]
        namematch = []
        for name in agentname:
            response.message.append(f"  *** Named Contact: {name} ***")
            nameQuery = """
                    SELECT contactid, contactname
                    FROM ndb.contacts AS ct
                    WHERE to_tsvector(ct.contactname) @@ plainto_tsquery(%(name)s);"""
            cur.execute(nameQuery, {"name": name})

            result = {"name": name, "match": cur.fetchall()}
            namematch.append(result)

        for person in namematch:
            if len(person["match"]) == 0:
                response.message.append(
                    f"  ✗ No approximate matches found for {person['name']}. Have they been added to Neotoma?"
                )
                response.valid.append(False)
            elif any([person["name"] == i[1] for i in person["match"]]):
                id = next(
                    (
                        number
                        for number, name in person["match"]
                        if name == person["name"]
                    ),
                    None,
                )
                response.message.append(f"  ✔ Exact match found for {person['name']}.")
                response.valid.append("True")
                try:
                    Contact(contactid=id, contactname=person["name"], order=None)
                    response.valid.append(True)
                    response.message.append("  ✔ Contact object created")
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"  ✗ Cannot create Contact object: {e}")
            else:
                response.message.append(
                    f"  ? No exact match found for {person['name']}, several potential matches follow:"
                )
                response.valid.append(False)
                for i in person["match"]:
                    response.message.append(f"   * {i[1]}")
    response.validAll = all(response.valid)
    return response
