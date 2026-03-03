import DataBUS.neotomaHelpers as nh
from DataBUS import ChronControl, Response
from DataBUS.ChronControl import CCONTROL_PARAMS

def valid_chroncontrols(cur, yml_dict, csv_file, databus=None):
    """Validates and inserts chronological control points for age models.

    Validates chronology control parameters including depth, age, thickness,
    and control type. Maps string control types to Neotoma integer IDs,
    verifies consistency of data dimensions, and creates ChronControl objects.

    When databus is provided and all parameters are valid, inserts each control
    point into the database using:
      - chronologyid from databus['chronologies'].id_list
      - analysisunitid values from databus['analysisunits'].id_list
    The resulting chroncontrol IDs are appended to response.id_list.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
        databus (dict | None): Prior validation results supplying chronology and
            analysis unit IDs.

    Returns:
        Response: Response object containing validation messages, validity list,
            chroncontrol IDs, and overall status.

    Examples:
        >>> valid_chroncontrols(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(CCONTROL_PARAMS, yml_dict, csv_file, "ndb.chroncontrols")
        if all(value is None for value in inputs.values()):
            response.valid.append(True)
            response.message.append(
                "✔ No chronology control parameters provided, skipping validation.")
            return response
        indices = [i for i, value in enumerate(inputs['age']) if value is not None]
        inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices]
                  if isinstance(inputs[k], list) else inputs[k]
                  for k in inputs}
    except Exception as e:
        response.valid.append(False)
        response.message.append(
            f"✗ Chronology parameters cannot be properly extracted. "
            f"Verify the CSV file.: {e}")
        return response

    agetype_query = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    chroncontrol_q = """SELECT chroncontroltypeid FROM ndb.chroncontroltypes
                        WHERE LOWER(chroncontroltype) = LOWER(%(chroncontroltype)s)"""
    par = {'agetypeid': [agetype_query, 'agetype'],
           'chroncontroltypeid': [chroncontrol_q, 'chroncontroltype']}

    # Pull chronologyid – prefer real ID from databus
    chronos_raw = inputs.pop('chronologyid', None)
    if databus.get('chronologies') is not None:
        chronos = databus.get('chronologies').id_list
        response.valid.append(True)
    else:
        if chronos_raw and isinstance(chronos_raw, list) and chronos_raw[0] is not None:
            chronos = list(dict.fromkeys(chronos_raw))  # assuming given chronologyid(s) are valid if provided
            response.valid.append(True)
        else:
            chronos = [1]
            response.valid.append(False)
            response.message.append(f"✗ No chronology found in databus. Using placeholder value for chronologyid.")

    # Resolve analysisunitids – prefer real IDs from databus
    if databus.get('analysisunits'):
        au_ids = databus['analysisunits'].id_list
        # take only the AUs for the indices we have
        au_ids = [au_ids[i] for i in indices if i < len(au_ids)]
        inputs['analysisunitid'] = au_ids
    else:
        response.valid.append(False)
        response.message.append(f"✗ No analysis units found in databus. Using placeholder values for analysisunitid.")
        inputs['analysisunitid'] = [i + 1 for i in range(len(inputs['age']))]

    for chron in chronos:
        for row in zip(*inputs.values()):
            control = dict(zip(inputs.keys(), row))
            control['chronologyid'] = chron
            # Resolve string lookup fields
            for param, (query, key) in par.items():
                if isinstance(control.get(param), str):
                    cur.execute(query, {key: control[param].lower().strip()})
                    result = cur.fetchone()
                    if result:
                        control[param] = result[0]
                        if (f"✔ The provided {param} is correct: {result[0]}"
                                not in response.message):
                            response.message.append(
                                f"✔ The provided {param} is correct: {result[0]}")
                        response.valid.append(True)
                    else:
                        if (f"✗ The provided {param} with value "
                                f"{control[param]} does not exist in Neotoma DB."
                                not in response.message):
                            response.message.append(
                                f"✗ The provided {param} with value "
                                f"{control[param]} does not exist in Neotoma DB.")
                        response.valid.append(False)
            try:
                cc = ChronControl(**control)
                if "✔  Chron controls can be created." not in response.message:
                   response.message.append("✔  Chron controls can be created.")
                response.valid.append(True)
                try:
                    cc_id = cc.insert_to_db(cur)
                    response.id_list.append(cc_id)
                    response.message.append(
                        f"✔  ChronControl inserted with ID {cc_id}.")
                    response.valid.append(True)
                except Exception as e:
                    if (f"✗  ChronControl could not be inserted: {e}"
                            not in response.message):
                        response.message.append(
                            f"✗  ChronControl could not be inserted: {e}")
                    response.valid.append(False)
                    continue
            except Exception as e:
                response.message.append(
                    f"✗  Could not create chron control with provided "
                    f"parameters: {e}")
                response.valid.append(False)
                continue        
    return response