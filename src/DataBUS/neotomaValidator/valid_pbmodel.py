import DataBUS.neotomaHelpers as nh
from DataBUS import LeadModel, Response


def valid_pbmodel(cur, yml_dict, csv_file, validator):
    """_Validating lead Model"""
    response = Response()

    params = ["basis", "cumulativeinventory"]
    inputs = nh.clean_inputs(
        nh.pull_params(params, yml_dict, csv_file, "ndb.leadmodels")
    )

    for j in range(
        1, validator["analysisunit"].aucounter + 1
    ):  # verify if class can be created with a fake analysisunitID
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
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append("✔  Lead Model can be created.")

    return response
