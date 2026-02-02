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
        >>> valid_chroncontrols(config_dict, csv_data, cursor, validator)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    params = CCONTROL_PARAMS
    
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.chroncontrols")
        indices = [i for i, value in enumerate(inputs['age']) if value is not None]
        inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    except Exception as e:
        response.valid.append(False) 
        response.message.append("✗ Chronology parameters cannot be properly extracted. Verify the CSV file.")
        response.message.append(e)
        return response
    
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    chroncontrol_q = """SELECT chroncontroltypeid FROM ndb.chroncontroltypes
                WHERE LOWER(chroncontroltype) = LOWER(%(chroncontroltype)s)"""
    try:
        if inputs.get("agetype"):
            cur.execute(agetype_q , {"agetype": inputs["agetype"]})
            agetype = cur.fetchone()
            if agetype:
                inputs["agetypeid"] = agetype[0]
                response.message.append("✔ The provided age type is correct.")
                response.valid.append(True)
            else:
                response.message.append("✗ The provided age type is incorrect..")
                response.valid.append(False)
                inputs["agetypeid"] = None
            del inputs["agetype"]
        else:
            response.message.append("? No age type provided.")
            response.valid.append(True)
            inputs["agetypeid"] = None
    except KeyError as e:
        inputs["agetype"] = None
        response.message.append("? No age type provided.")
        response.valid.append(True)
        inputs["agetypeid"] = None
    try:
        if len(inputs["depth"]) == len(inputs["age"]) == len(inputs["thickness"]):
            response.message.append(f"✔ The number of depths (analysis units), ages, and thicknesses are the same.")
            response.valid.append(True)
        else:
            response.message.append(f"✗ The number of depths (analysis units), ages, "
                                    f"and thicknesses is not the same. Please check.")
            response.valid.append(False)
    except Exception as e:
        response.message.append("? Depth, Age, or Thickness are not given.")

    for k in inputs:
        if not inputs[k]:
            response.message.append(f"? {k} has no values.")
        else:
            response.message.append(f"✔ {k} looks valid.")
            response.valid.append(True)

    if inputs.get('chroncontroltypeid'):
        elements = set(inputs['chroncontroltypeid'])
        chroncontroltypes = {}
        for e in elements:
            cur.execute(chroncontrol_q, {"chroncontroltype": e})
            chroncontrol = cur.fetchone()
            if chroncontrol:
                chroncontroltypes[e] = chroncontrol[0]
            else:
                response.message.append(f"✗  Chron control type {e} not found in database")
                response.valid.append(False)
                chroncontroltypes[e] = None

    if inputs.get('chroncontroltypeid') is not None:
        inputs['chroncontroltypeid'] = [chroncontroltypes.get(item, item) for item in inputs.get('chroncontroltypeid',[])]

    iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
    static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
    for values in zip(*iterable_params.values()):
        try:
            kwargs = dict(zip(iterable_params.keys(), values))
            kwargs.update(static_params) 
            ChronControl(**kwargs)
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗  Could not create chron control {e}")
            response.valid.append(False)

    if response.validAll:
        response.message.append(f"✔  Chron control can be created")
    response.message = list(set(response.message))
    return response