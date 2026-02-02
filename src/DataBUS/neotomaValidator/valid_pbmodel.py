import DataBUS.neotomaHelpers as nh
from DataBUS import LeadModel, Response

def valid_pbmodel(cur, yml_dict, csv_file, validator):
    """Validates lead-210 dating model parameters.

    Validates lead model parameters including basis (dating assumption) and
    cumulative inventory. Creates LeadModel objects for each analysis unit
    with validated basis ID.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing lead model data.
        validator (dict): Dictionary containing validation parameters from prior steps.

    Returns:
        Response: Response object containing validation messages and overall validity status.

    Examples:
        >>> valid_pbmodel(cursor, config_dict, "pb_data.csv", validator)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()

    params = ["basis", "cumulativeinventory"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.leadmodels")

    for j in range(1, validator["analysisunit"].counter + 1):  
        # verify if class can be created with a fake analysisunitID
        pbbasisid_q = """SELECT pbbasisid from ndb.leadmodelbasis
                         WHERE pbbasis = %(pbbasis)s"""
        cur.execute(pbbasisid_q, {"pbbasis": inputs["basis"][0]})
        inputs["pbbasisid"] = cur.fetchone()
        if inputs["pbbasisid"]:
            inputs["pbbasisid"] = inputs["pbbasisid"][0]
        try:
            LeadModel(
                pbbasisid=inputs["pbbasisid"],
                analysisunitid=j,
                cumulativeinventory=inputs["cumulativeinventory"][0],
            )
            response.valid.append(True)    
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Lead model cannot be created: {e}")
    if response.validAll:
        response.message.append(f"✔  Lead Model can be created.")

    return response