import DataBUS.neotomaHelpers as nh
from DataBUS import ChronControl, Response
from DataBUS.ChronControl import CCONTROL_PARAMS

def valid_chroncontrols(yml_dict, csv_file, cur):
    """Validates chronological control points for age models.

    Validates chronology control parameters including depth, age, thickness,
    and control type. Maps SISAL database control types to Neotoma types,
    verifies consistency of data dimensions, and creates ChronControl objects
    with validated parameters.

    Args:
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
        cur (cursor): Database cursor for executing SQL queries.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_chroncontrols(config_dict, csv_data, cursor)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(CCONTROL_PARAMS, yml_dict, csv_file, "ndb.chroncontrols")
        if all(value is None for value in inputs.values()):
            response.valid.append(True)
            response.message.append("✔ No chronology control parameters provided, skipping validation.")
            return response
        indices = [i for i, value in enumerate(inputs['age']) if value is not None]
        inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices]
                  if isinstance(inputs[k], list) else inputs[k]
                  for k in inputs}
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Chronology parameters cannot be properly extracted. "
                                f"Verify the CSV file.: {e}")
        return response
    agetype_query = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    chroncontrol_q = """SELECT chroncontroltypeid FROM ndb.chroncontroltypes
                        WHERE LOWER(chroncontroltype) = LOWER(%(chroncontroltype)s)"""
    
    par = {'agetypeid': [agetype_query, 'agetype'],
           'chroncontroltypeid': [chroncontrol_q, 'chroncontroltype']}
    chronos = inputs.pop('chronologyid') 
    chronos = [1] # placeholder for chronologyid
    inputs['analysisunitid'] = [i+1 for i in range(len(inputs['age']))] # placeholder for AU
    for chron in chronos:
        for row in zip(*inputs.values()):
            control = dict(zip(inputs.keys(), row))
            control['chronologyid'] = chron
            for param, (query, key) in par.items():
                if isinstance(control.get(param), str):
                    cur.execute(query, {key: control[param].lower().strip()})
                    result = cur.fetchone()
                    if result:
                        control[param] = result[0]
                        if f"✔ The provided {param} is correct: {result[0]}" not in response.message:
                            response.message.append(f"✔ The provided {param} is correct: {result[0]}")
                        response.valid.append(True)
                    else:
                        if f"✗ The provided {param} with value {control[param]} does not exist in Neotoma DB." not in response.message:
                            response.message.append(f"✗ The provided {param} with value {control[param]} does not exist in Neotoma DB.")
                        response.valid.append(False)
            try:
                ChronControl(**control)
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"✗  Could not create chron control with provided parameters: {e}")
                response.valid.append(False)
    if response.validAll:
        response.message.append("✔  Chron controls can be created.")
    return response