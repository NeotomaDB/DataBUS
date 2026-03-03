import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, Response
from DataBUS.Chronology import CHRONOLOGY_PARAMS

def valid_chronologies(cur, yml_dict, csv_file, databus=None):
    """Validates and inserts chronologies for geochronological data.

    Validates chronology parameters including age type, contact ID, date prepared,
    and age bounds. Handles age model conversions (e.g., collection date to years BP)
    and creates Chronology objects with validated parameters.  When databus is provided
    and all parameters are valid, inserts each chronology into the database using
    the real collection unit ID from databus['collunits'].id_int and stores the
    resulting chronology ID in response.id_list

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
        databus (dict | None): Prior validation results supplying collectionunitid
            and (optionally) contactid overrides.

    Returns:
        Response: Response object containing validation messages, validity list,
            chronology IDs, and overall status.

    Examples:
        >>> valid_chronologies(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...])
    """
    response = Response()
    try:
        inputs = nh.pull_params(CHRONOLOGY_PARAMS, yml_dict, csv_file, "ndb.chronologies")
        agetype = inputs.get('agetypeid')
        if "chronologies" in inputs:
            inputs = inputs.get("chronologies")
        else:
            response.valid.append(True)
            response.message.append("? No chronology parameters provided.")
            return response
    except Exception as e:
        response.valid.append(False)
        response.message.append(
            f"✗  Chronology parameters cannot be properly extracted: {e}.")
        return response

    if len(inputs) > 1:
        response.message.append("✔ File with multiple chronologies.")
        response.message.append(f"{list(inputs.keys())}")

    agetype_query = """SELECT agetypeid FROM ndb.agetypes
                       WHERE LOWER(agetype) = %(agetype)s"""
    contact_query = """SELECT contactid FROM ndb.contacts
                       WHERE LOWER(contactname) = %(contactname)s"""
    
    if databus.get('collunits') is not None:
        cu_id = databus['collunits'].id_int
        if isinstance(cu_id, int):
            collunitid = cu_id
            response.valid.append(True)
    else:
        collunitid = 1 # placeholder
        response.valid.append(False)
        response.message.append(f"✗ No collection unit found in databus. Using placeholder value for collectionunitid.")

    for chron_key in inputs:
        ch = inputs[chron_key]
        ch['agetypeid'] = agetype
        # Use the YAML chronologyname grouping key as the DB chronologyname if not
        # explicitly mapped (no template entry for ndb.chronologies.chronologyname).
        if ch.get("chronologyname") is None:
            ch["chronologyname"] = chron_key
        
        if ch.get("agetypeid") is not None:
            if isinstance(ch["agetypeid"], str):
                cur.execute(agetype_query,
                            {'agetype': ch["agetypeid"].lower().strip()})
                result = cur.fetchone()
                if result:
                    ch['agetypeid'] = result[0]
                    response.message.append(
                        f"✔ The provided age type is correct: {result[0]}")
                    response.valid.append(True)
                else:
                    response.message.append(
                        "✗ The provided age type does not exist in Neotoma DB.")
                    response.valid.append(False)
                    continue
        if ch.get("contactid") is not None:
            if isinstance(ch["contactid"], str):
                cur.execute(contact_query,
                            {'contactname': ch["contactid"].lower().strip()})
                result = cur.fetchone()
                if result:
                    ch['contactid'] = result[0]
                    response.message.append(
                        f"✔ The provided contact name is correct: {result[0]}")
                    response.valid.append(True)
                else:
                    response.message.append(
                        "✗ The provided contact name does not exist in Neotoma DB.")
                    response.valid.append(False)
                    continue
        try:
            if ch.get('agemodel') == "collection date":
                ch['ageboundolder'] = nh.convert_to_bp(ch.get('ageboundolder'))
                ch['ageboundyounger'] = nh.convert_to_bp(ch.get('ageboundyounger'))
        except Exception as e:
            response.valid.append(False)
            response.message.append(
                f"✗ Age bounds could not be converted from collection date to "
                f"years BP: {e}")
            continue
        try:
            ch["collectionunitid"] = collunitid
            chron = Chronology(**ch)
            response.valid.append(True)
            response.message.append("✔  Chronology can be created.")
            try:
                chron_id = chron.insert_to_db(cur)
                response.message.append(
                    f"✔  Chronology inserted with ID {chron_id} "
                    f"('{chron_key}').")
                response.valid.append(True)
                response.id_list.append(chron_id)
                response.name[chron_key] = chron_id
            except Exception as e:
                response.valid.append(False)
                response.message.append(
                    f"✗  Chronology could not be inserted: {e}")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Chronology cannot be created: {e}")
            continue
    return response