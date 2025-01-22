import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, ChronResponse


def insert_chronology(cur, yml_dict, csv_file, uploader):
    """
    Inserts chronology data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): Dictionary containing information about the inserted chronology.
        Contains keys:
            'chronology': ID of the inserted chronology.
            'valid': Boolean indicating if the insertion was successful.
    """
    response = ChronResponse()

    params = [
        "chronologyid", "collectionunitid", "contactid", "isdefault", "chronologyname",
        "dateprepared", "agemodel", "ageboundyounger", "ageboundolder", "notes",
        "recdatecreated", "recdatemodified",
    ]
    inputs = nh.clean_inputs(
        nh.pull_params(params, yml_dict, csv_file, "ndb.chronologies")
    )
    inputs_age = nh.clean_inputs(
        nh.pull_params(["age"], yml_dict, csv_file, "ndb.sampleages"))
    
    agetype = list(set(inputs_age["unitcolumn"]))
    inputs["agetype"] = agetype[0]
    inputs["agemodel"] = "X210Lead"

    if inputs["agetype"] == "cal yr BP":
        inputs["agetypeid"] = 2
    elif inputs["agetype"] == "CE/BCE":
        inputs["agetypeid"] = 1
    else:
        inputs["agetypeid"] = None
    if isinstance(inputs["contactid"], list):
        get_contact = """SELECT contactid FROM ndb.contacts WHERE LOWER(%(contactname)s) = contactname;"""
        cur.execute(get_contact, {"contactname": inputs["contactid"][0]})
        inputs["contactid"] = cur.fetchone()
    try:
        chron = Chronology(
            collectionunitid=uploader["collunitid"].cuid,
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
    except Exception:
        response.valid.append(False)
        response.message.append("✗  Chronology cannot be created {e}")
        chron = Chronology(collectionunitid=uploader["collunitid"].cuid)

    if isinstance(inputs_age["age"], (int, float)):
        chron.maxage = int(max(inputs_age["age"])) 
        chron.minage = int(min(inputs_age["age"]))
    else:
        response.message.append("? Age is set to None. Minage/maxage will be None.")

    try:
        chronid = chron.insert_to_db(cur)
        response.chronid = chronid
        response.valid.append(True)
        response.message.append(f"✔ Added Chronology {chronid}.")

    except Exception as e:
        response.message.append(
            f"✗  Chronology Data is not correct. Error message: {e}"
        )
        chron = Chronology(collectionunitid=uploader["collunitid"].cuid, agetypeid=1)
        chronid = chron.insert_to_db(cur)
        response.valid.append(False)
    response.validAll = all(response.valid)
    return response