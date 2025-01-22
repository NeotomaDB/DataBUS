import DataBUS.neotomaHelpers as nh
from DataBUS import LeadModel, Response


def insert_pbmodel(cur, yml_dict, csv_file, uploader):
    """Inserting lead Model"""
    response = Response()

    params = ["basis", "cumulativeinventory"]
    inputs = nh.clean_inputs(
        nh.pull_params(params, yml_dict, csv_file, "ndb.leadmodels")
    )

    pbbasisid_q = """SELECT pbbasisid from ndb.leadmodelbasis
                        WHERE pbbasis = %(pbbasis)s"""
    cur.execute(pbbasisid_q, {"pbbasis": inputs["basis"][0]})
    inputs["pbbasisid"] = cur.fetchone()

    if inputs["pbbasisid"]:
        inputs["pbbasisid"] = inputs["pbbasisid"][0]

    for j in uploader[
        "anunits"
    ].auid:  # verify if class can be created with a fake analysisunitID

        try:
            pb_model = LeadModel(
                pbbasisid=inputs["pbbasisid"],
                analysisunitid=j,
                cumulativeinventory=inputs["cumulativeinventory"][0],
            )
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Lead model cannot be built: {e}")
            pb_model = LeadModel(
                pbbasisid=None, analysisunitid=None, cumulativeinventory=None
            )
        finally:
            try:
                pb_model.insert_to_db(cur)
                response.valid.append(True)
                response.message.append("✔  Added Lead Model.")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Lead Model could not be inserted {e}.")

    response.validAll = all(response.valid)
    return response
