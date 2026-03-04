import DataBUS.neotomaHelpers as nh
from DataBUS import LeadModel, Response
from DataBUS.LeadModel import LEAD_MODEL_PARAMS


def valid_pbmodel(cur, yml_dict, csv_file, databus):
    """Validates lead-210 dating model parameters.

    Validates lead model parameters including basis (dating assumption) and
    cumulative inventory. Creates LeadModel objects for each analysis unit
    with validated basis ID.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing lead model data.
        databus (dict): Dictionary containing validation parameters from prior steps.

    Returns:
        Response: Response object containing validation messages and overall validity status.

    Examples:
        >>> valid_pbmodel(cursor, config_dict, "pb_data.csv", databus)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(LEAD_MODEL_PARAMS, yml_dict, csv_file, "ndb.leadmodels")
        if all(value is None for value in inputs.values()):
            response.valid.append(True)
            response.message.append("?  No lead model parameters provided.\n")
            return response
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  Lead model parameters cannot be properly extracted. {e}\n")
        return response

    query = """SELECT pbbasisid FROM ndb.leadmodelbasis
                     WHERE pbbasis = %(pbbasisid)s"""
    cur.execute(query, {"pbbasisid": inputs.get("pbbasisid")})

    inputs["pbbasisid"] = cur.fetchone()
    if inputs.get("pbbasisid") is not None:
        inputs["pbbasisid"] = inputs["pbbasisid"][0]

    if databus.get("analysisunits") and databus["analysisunits"].id_list:
        au_ids = databus["analysisunits"].id_list
    else:
        au_ids = range(1, 10)  # placeholder
        response.valid.append(False)
        response.message.append("✗ Analysis unit IDs not available; using placeholder range.")
    for j in au_ids:
        try:
            pb_model = LeadModel(
                pbbasisid=inputs.get("pbbasisid"),
                analysisunitid=j,
                cumulativeinventory=inputs.get("cumulativeinventory"),
                datinghorizon=inputs.get("datinghorizon"),
            )
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            if f"✗  Lead model cannot be created: {e}" not in response.message:
                response.message.append(f"✗  Lead model cannot be created: {e}")
                continue
        try:
            pb_model.insert_to_db(cur)
            response.valid.append(True)
            if "✔  Lead model can be inserted." not in response.message:
                response.message.append("✔  Lead model can be inserted.")
        except Exception as e:
            response.valid.append(False)
            if f"✗  Lead model cannot be inserted: {e}" not in response.message:
                response.message.append(f"✗  Lead model cannot be inserted: {e}")
    return response
