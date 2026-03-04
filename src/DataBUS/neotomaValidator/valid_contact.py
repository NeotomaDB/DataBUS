import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response
from DataBUS.Contact import CONTACT_PARAMS, CONTACT_TABLES

def valid_contact(cur, yml_dict, csv_file, tables=CONTACT_TABLES, databus=None):
    """Validates contact information against the Neotoma Paleoecology Database.

    Validates contact data (contact IDs or names) against the Neotoma database
    to ensure they exist and are valid. Matches contact names with database records
    and creates Contact objects with validated parameters. When databus is provided,
    inserts contacts into the appropriate relational tables:
      - ndb.collectors      → insert_collector(cur, collunitid)
      - ndb.datasetpis      → insert_pi(cur, datasetid)
      - ndb.datasetprocessor → insert_data_processor(cur, datasetid)
      - ndb.sampleanalysts  → insert_sample_analyst(cur, sampleid) for each sample
      - ndb.chronologies    → contact is stored on the Chronology object (no separate insert)

    Args:
        cur (psycopg2.extensions.cursor): Cursor pointing to Neotoma database.
        yml_dict (dict): Dictionary object from template configuration.
        csv_file (str): Path to CSV file containing contact data or user name.
        tables (list): List of table names to validate contacts for.
        databus (dict | None): Dictionary of prior validation results used for insert IDs.

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
                            contact = Contact(contactid=get_name["id"],
                                              contactname=get_name["name"],
                                              order=get_name["order"])
                            response.valid.append(True)
                            response.message.append(
                                f"✓ Valid contact: {get_name['name']} (ID: {get_name['id']}) for {key}.")
                            response.id_list.append(get_name["id"])
                            try:
                                _insert_contact(cur, contact, key, databus, response)
                                response.valid.append(True)
                            except Exception as e:
                                response.message.append(f"✗ Could not insert contact: {e}")
                                response.valid.append(False)
                        except Exception as e:
                            response.message.append(
                                f"✗ Cannot create Contact object for: {name}. {e}")
                            response.valid.append(False)
        else:
            counter = 0
            for contact_id in inputs[key]["contactid"]:
                counter += 1
                try:
                    contact = Contact(contactid=contact_id, order=counter)
                    response.message.append(f"✓ Valid contact ID: {contact_id} for {key}.")
                    response.valid.append(True)
                    response.id_list.append(contact_id)
                    _insert_contact(cur, contact, key, databus, response)
                except Exception as e:
                    response.message.append(f"✗ Invalid contact ID: {contact_id}. {e}")
                    response.valid.append(False)
    return response

def _insert_contact(cur, contact, table_key, databus, response):
    """Dispatch the correct insert method based on the contact table.

    Args:
        cur: Database cursor.
        contact (Contact): Validated Contact object.
        table_key (str): The CONTACT_TABLE key (e.g. 'ndb.collectors').
        databus (dict | None): Prior validation results for IDs.
        response (Response): Response object to append messages to.
    """
    try:
        collunitid = databus.get('collunits').id_int
        datasetid = databus.get('datasets').id_int
        sample_ids = databus.get('samples').id_list
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Error retrieving ID from databus: {e}")
        response.valid.append(False)
        collunitid = 2 #Placeholder
        datasetid = 1 # Placeholder
        sample_ids = [1, 2, 3] # Placeholder

    if table_key == "ndb.collectors": 
        try:
            contact.insert_collector(cur, collunitid=collunitid)
            response.message.append(
                f"✔  Inserted collector (contactid={contact.contactid}) "
                f"for collunit {collunitid}.")
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Could not insert collector: {e}")
            response.valid.append(False)
    elif table_key == "ndb.datasetpis":
        try:
            contact.insert_pi(cur, datasetid=datasetid)
            response.message.append(
                f"✔  Inserted dataset PI (contactid={contact.contactid}) "
                f"for dataset {datasetid}.")
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Could not insert dataset PI: {e}")
            response.valid.append(False)
    elif table_key == "ndb.datasetprocessor":
        try:
            contact.insert_data_processor(cur, datasetid=datasetid)
            response.message.append(
                f"✔  Inserted data processor (contactid={contact.contactid}) "
                f"for dataset {datasetid}.")
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Could not insert data processor: {e}")
            response.valid.append(False)
    elif table_key == "ndb.sampleanalysts":
        inserted = 0
        for sampleid in sample_ids:
            try:
                contact.insert_sample_analyst(cur, sampleid=sampleid)
                inserted += 1
            except Exception as e:
                response.message.append(
                    f"✗ Could not insert sample analyst for sample {sampleid}: {e}")
                response.valid.append(False)
        if inserted:
            response.message.append(
                f"✔  Inserted sample analyst (contactid={contact.contactid}) "
                f"for {inserted} sample(s).")
    elif table_key == "ndb.chronologies":
        response.message.append(
            f"✔  Contact (contactid={contact.contactid}) will be associated with chronology record.")
        response.valid.append(True)