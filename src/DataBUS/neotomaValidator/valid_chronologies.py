import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, ChronResponse


def valid_chronologies(cur, yml_dict, csv_file):
    """_Validating chronologies"""
    response = ChronResponse()

    params = [
        "chronologyid",
        "collectionunitid",
        "contactid",
        "isdefault",
        "chronologyname",
        "dateprepared",
        "agemodel",
        "ageboundyounger",
        "ageboundolder",
        "notes",
        "recdatecreated",
        "recdatemodified",
    ]
    try:
        inputs = nh.clean_inputs(
            nh.pull_params(params, yml_dict, csv_file, "ndb.chronologies")
        )
        inputs_age = nh.clean_inputs(
            nh.pull_params(["age"], yml_dict, csv_file, "ndb.sampleages")
        )
    except Exception as e:
        response.validAll = False 
        response.message.append("Chronology parameters cannot be properly extracted. Verify the CSV file.")
        response.message.append(e)
        return response

    try:
        agetype = list(set(inputs_age["unitcolumn"]))
        inputs["agetype"] = agetype[0]
    except KeyError:
        inputs["agetype"] = None

    if inputs["agetype"]:
        if inputs["agetype"] == "cal yr BP":
            inputs["agetypeid"] = 2
            response.message.append("✔ The provided age type is correct.")
            response.valid.append(True)
        elif inputs["agetype"] == "CE/BCE":
            inputs["agetypeid"] = 1
            response.message.append("✔ The provided age type is correct.")
            response.valid.append(True)
        else:
            response.message.append("✗ The provided age type is incorrect..")
            response.valid.append(False)
            inputs["agetypeid"] = None
    else:
        response.message.append("? No age type provided.")
        response.valid.append(True)
        inputs["agetypeid"] = None

    for k in inputs:
        if not inputs[k]:
            response.message.append(f"? {k} has no values.")
        else:
            response.message.append(f"✔ {k} looks valid.")
            response.valid.append(True)

    if isinstance(inputs["contactid"], str):
        get_contact = """SELECT contactid FROM ndb.contacts WHERE LOWER(%(contactname)s) = contactname;"""
        cur.execute(get_contact, {"contactname": inputs["contactid"]})
        inputs["contactid"] = cur.fetchone()[0]
    try:
        Chronology(
            chronologyid=inputs["chronologyid"],
            agetypeid=inputs["agetypeid"],
            contactid=inputs["contactid"],
            isdefault=inputs["isdefault"],
            chronologyname=inputs["chronologyname"],
            dateprepared=inputs["dateprepared"],
            agemodel=inputs["agemodel"],
            ageboundyounger=inputs["ageboundyounger"],
            ageboundolder=inputs["ageboundolder"],
            notes=inputs["notes"],
            recdatecreated=inputs["recdatecreated"],
            recdatemodified=inputs["recdatemodified"],
        )
        response.valid.append(True)
        response.message.append("✔  Chronology can be created")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  Chronology cannot be created: {e}")
    response.validAll = all(response.valid)
    return response
