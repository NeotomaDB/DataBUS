import DataBUS.neotomaHelpers as nh
from DataBUS import Geochron, Response
from DataBUS.Geochron import GECHRON_PARAMS

def valid_geochron(cur, yml_dict, csv_file):
    """Validates geochronological dating data.

    Validates geochronology parameters including dating type, age, error bounds,
    and material dated.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing geochronology data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_geochron(cursor, config_dict, "dating_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(GECHRON_PARAMS, yml_dict, csv_file, "ndb.geochronology")
        if not inputs.get('age'):
            response.valid.append(True)
            response.message.append("✔  No age values provided.")
            return response
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Geochronology parameters cannot be properly extracted. "
                                f"Verify the CSV file.: {e}")
        return response
    indices = [i for i, value in enumerate(inputs.get('age')) if value is not None]
    if not indices:
        response.valid.append(True)
        response.message.append("✔  No age values provided.")
        return response
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices]
              if isinstance(inputs[k], list) else inputs[k] for k in inputs}
    for key in ("geochrontypeid", "agetypeid"):
        val = inputs.get(key)
        inputs[key] = val if isinstance(val, list) else [val] * len(indices)
    inputs["sampleid"] = [i+1 for i in range(len(indices))] # placeholder for sampleid
    inputs = {k: v for k, v in inputs.items() if v is not None}

    geochron_query = """SELECT geochrontypeid FROM ndb.geochrontypes
                    WHERE LOWER(geochrontype) = LOWER(%(geochrontype)s)"""
    agetype_query = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    par = {"agetypeid": [agetype_query, "agetype"],
           "geochrontypeid": [geochron_query, "geochrontype"]}
    
    for row in zip(*inputs.values()):
        geochron = dict(zip(inputs.keys(), row))
        for param, (query, key) in par.items():
            if geochron.get(param):
                if isinstance(geochron[param], str):
                    cur.execute(query, {key: geochron[param].lower().strip()})
                    result = cur.fetchone()
                    if result:
                        geochron[param] = result[0]
                        if f"✔ The provided {param} is correct: {result[0]}" not in response.message:
                            response.message.append(f"✔ The provided {param} is correct: {result[0]}")
                        response.valid.append(True)
                    else:
                        if f"✗ The provided {param} with value {geochron[param]} does not exist in Neotoma DB." not in response.message:
                            response.message.append(f"✗ The provided {param} with value {geochron[param]} does not exist in Neotoma DB.")
                        response.valid.append(False)
        try:
            Geochron(**geochron)
            response.valid.append(True)
            if f"✔ Geochronology created successfully." not in response.message:
                response.message.append(f"✔ Geochronology created successfully.")
        except Exception as e:
            if f"✗  Geochronology cannot be created: {e}" not in response.message:
                response.message.append(f"✗  Geochronology cannot be created: {e}")
            response.valid.append(False)
    return response