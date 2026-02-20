import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, Response
from DataBUS.Chronology import CHRONOLOGY_PARAMS

def valid_chronologies(cur, yml_dict, csv_file):
    """Validates chronologies for geochronological data.

    Validates chronology parameters including age type, contact ID, date prepared,
    and age bounds. Handles age model conversions (e.g., collection date to years BP)
    and creates Chronology objects with validated parameters.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_chronologies(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...])
    """
    response = Response()
    try:
        inputs = nh.pull_params(CHRONOLOGY_PARAMS, yml_dict, csv_file, "ndb.chronologies", values=False)
        if "chronologies" in inputs:
            inputs=inputs.get("chronologies")
        else:
            response.valid.append(True)
            response.message.append("? No chronology parameters provided.")
            return response
    except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Chronology parameters cannot be properly extracted: {e}.")
            return response
    if len(inputs) > 1:
        response.message.append("✔ File with multiple chronologies.")
        response.message.append(f"{list(inputs.keys())}")
    agetype_query = """SELECT agetypeid FROM ndb.agetypes
                       WHERE LOWER(agetype) = %(agetype)s"""
    contact_query = """SELECT contactid FROM ndb.contacts
                       WHERE LOWER(contactname) = %(contactname)s"""
    for chron in inputs:
        ch = inputs[chron]
        if ch.get("agetypeid") is not None:
            if isinstance(ch["agetypeid"], str):
                cur.execute(agetype_query,
                            {'agetype': ch["agetypeid"].lower().strip()})
                result = cur.fetchone()
                if result:
                    ch['agetypeid'] = result[0]
                    response.message.append(f"✔ The provided age type is correct: {result[0]}")
                    response.valid.append(True)
                else:
                    response.message.append("✗ The provided age type does not exist in Neotoma DB.")
                    response.valid.append(False)
                    return response
        if ch.get("contactid") is not None:
            if isinstance(ch["contactid"], str):
                cur.execute(contact_query,
                            {'contactname': ch["contactid"].lower().strip()})
                result = cur.fetchone()
                if result:
                    ch['contactid'] = result[0]
                    response.message.append(f"✔ The provided contact name is correct: {result[0]}")
                    response.valid.append(True)
                else:
                    response.message.append("✗ The provided contact name does not exist in Neotoma DB.")
                    response.valid.append(False)
                    return response
        try:
            if ch.get('agemodel') == "collection date":
                ch['ageboundolder'] = nh.convert_to_bp(ch.get('ageboundolder'))
                ch['ageboundyounger'] = nh.convert_to_bp(ch.get('ageboundyounger'))
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Age bounds for could not be converted from collection date to years BP: {e}")
            return response
        try:
            ch["collectionunitid"] = 1 # Placeholder
            Chronology(**ch)
            response.valid.append(True)
            response.message.append("✔  Chronology can be created.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Chronology cannot be created: {e}")
    return response