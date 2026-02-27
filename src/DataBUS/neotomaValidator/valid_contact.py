import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response
from DataBUS.Contact import CONTACT_PARAMS, CONTACT_TABLES

def valid_contact(cur, yml_dict, csv_file, tables=CONTACT_TABLES):
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
    inputs = {}
    for table in tables:
        try:
            data = nh.pull_params(CONTACT_PARAMS, yml_dict, csv_file, table)
            if all(data.get(param) is None for param in CONTACT_PARAMS):
                response.message.append(f"?  No contact information provided for {table}.")
                response.valid.append(True)
                continue
            inputs[table] = data
        except Exception as e:
            response.message.append(f"✗ Error pulling parameters for {table}: {e}")
            response.valid.append(False)
            continue
    
    for key in inputs:
        response.message.append(f"=== Validating Contacts for Table: {key} ===")
        if not (isinstance(inputs[key].get("contactid"), list) and
                all(isinstance(i, int) for i in inputs[key]["contactid"])):
            if not (isinstance(inputs[key].get("contactname"), str) or
                    (isinstance(inputs[key]["contactname"], list) and
                    all(isinstance(i, str) for i in inputs[key]["contactname"]))):
                response.message.append(f"✗ Invalid contact information for {key}: {inputs[key]['contactname']}.")
                response.valid.append(False)
                continue
            else:
                counter = 0
                if isinstance(inputs[key]["contactname"], str):
                    inputs[key]["contactname"] = [inputs[key]["contactname"]]
                for name in inputs[key]["contactname"]:
                    counter += 1
                    get_name = nh.get_contacts(cur, name)
                    get_name["order"] = counter
                    if get_name["id"] is None:
                        response.message.append(f"✗ Contact not found in database: {name}.")
                        response.valid.append(False)
                    else:
                        try:
                            Contact(contactid = get_name["id"],
                                    contactname = get_name["name"],
                                    order = get_name["order"])
                            response.valid.append(True)
                            response.message.append(f"✓ Valid contact: {get_name['name']} (ID: {get_name['id']}) for {key}.")
                        except Exception as e:
                            response.message.append(f"✗ Cannot create Contact object for: {name}. {e}")
                            response.valid.append(False)
        else:
            counter = 0
            for contact_id in inputs[key]["contactid"]:
                counter += 1
                try:
                    Contact(contactid = contact_id, order = counter)
                    response.message.append(f"✓ Valid contact ID: {contact_id} for {key}.")
                    response.valid.append(True)
                except Exception as e:
                    response.message.append(f"✗ Invalid contact ID: {contact_id}. {e}")
                    response.valid.append(False)
    return response