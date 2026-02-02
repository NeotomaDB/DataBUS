import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def valid_geopolitical_units(cur, yml_dict, csv_file):
    """Validates geopolitical unit assignments against Neotoma database.

    Validates provided geopolitical units (country, state, county, etc.) by
    querying the database for matching geopolitical IDs. Returns the most
    specific (lowest level) valid geopolitical unit found.
    
    Args:
        cur (_psycopg2.extensions.connection_): Database connection to Neotoma database.
        yml_dict (dict): Dictionary containing parameters from YAML configuration.
        csv_file (str): Path to CSV file containing additional parameters.

    Returns:
        Response: Response object containing validation results and messages.
    
    Examples:
        >>> valid_geopolitical_units(cursor, config_dict, "geo_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()

    params = ["geopoliticalunit1", "geopoliticalunit2",
              "geopoliticalunit3", "geopoliticalunit4"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sitegeopolitical")
    query = """SELECT geopoliticalid FROM ndb.geopoliticalunits
                WHERE LOWER(geopoliticalname) = %(geopoliticalname)s"""
    geo_units = {}
    for unit in inputs:
        if inputs[unit]:
            cur.execute(query, {'geopoliticalname' : inputs[unit].lower()})
            answer = cur.fetchone()
            if not answer:
                answer = None
            else:
                answer = answer[0]
        else:
            answer = None
        geo_units[unit] = answer
    key_params = list(inputs.keys())
    result = next((geo_units[key] for key in key_params[::-1] if geo_units[key] is not None), None)

    if any(value is not None for value in geo_units.values()):
        response.message.append(f"✔ Site GPUID in {result}.")
        response.valid.append(True)
    else:
        response.message.append(f"? Site GPUID not available in Neotoma.")
        response.valid.append(True)

    return response