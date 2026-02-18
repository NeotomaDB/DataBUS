import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def valid_geopolitical_units(cur, yml_dict, csv_file, uploader=None, insert=False):
    """Validates geopolitical unit assignments against Neotoma database.

    Validates provided geopolitical units (national_unit, state, county, etc.) by
    querying the database for matching geopolitical IDs. Returns the most
    specific (lowest level) valid geopolitical unit found.

    Args:
        cur (psycopg2.extensions.connection): Database connection to Neotoma database.
        yml_dict (dict): Dictionary containing parameters from YAML configuration.
        csv_file (str): Path to CSV file containing additional parameters.
        uploader (dict, optional): Dictionary containing uploaded site data. Defaults to None.
        insert (bool, optional): Whether to insert geopolitical data. Defaults to False.

    Returns:
        Response: Response object containing validation results and messages.

    Examples:
        >>> valid_geopolitical_units(cursor, config_dict, "geo_data.csv", insert=False)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    params = ["national_unit", "subnational_unit_lv1",
              "subnational_unit_lv2", "subnational_unit_lv3"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sitegeopolitical")

    query = """SELECT geopoliticalid FROM ndb.geopoliticalunits
               WHERE LOWER(geopoliticalname) = %(geopoliticalname)s"""
    query_with_parent = """SELECT geopoliticalid FROM ndb.geopoliticalunits
                           WHERE LOWER(geopoliticalname) = %(geopoliticalname)s
                           AND highergeopoliticalid = %(highergeopoliticalid)s"""
    insert_q = """SELECT ts.insertsitegeopol(_siteid:= %(siteid)s,
                                             _geopoliticalid := %(geopolid)s)"""

    if inputs.get('national_unit') is None:
        response.message.append("? No Geopolitical Units given.")
        response.valid.append(True)
        return response

    cur.execute(query, {'geopoliticalname': inputs.get('national_unit').lower()})
    gpid1 = cur.fetchone()
    country = inputs.pop('national_unit', None)

    if not gpid1:
        response.message.append(f"✗  National Unit {country} not found.")
        response.valid.append(False)
        return response

    gpid1 = gpid1[0]
    current_id = gpid1
    response.message.append(f"✔ National Unit {country} found.")
    response.valid.append(True)

    for unit in inputs:
        cur.execute(query_with_parent, {'geopoliticalname': inputs[unit].lower(),
                                        'highergeopoliticalid': current_id})
        gpid2 = cur.fetchone()
        if not gpid2:
            response.message.append(f"? Subregional Unit {inputs[unit]} not found.")
            break
        gpid2 = gpid2[0]
        current_id = gpid2
        cur.execute(insert_q, {'siteid': uploader["sites"].siteid,
                                'geopolid': gpid2})
        response.message.append(f"✔ Subregional Unit {inputs[unit]} found.")
    return response
