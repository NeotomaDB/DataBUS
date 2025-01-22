import DataBUS.neotomaHelpers as nh
from DataBUS import AnalysisUnit, AUResponse

def insert_analysisunit(cur, yml_dict, csv_file, uploader):
    """_Inserting analysis units_
    Args:
        cur (_psycopg2.extensions.cursor_): _A cursor pointing to the Neotoma
            Paleoecology Database._
        yml_dict (_dict_): _A `dict` returned by the YAML template._
        csv_file (_dict_): _The csv file with the required data to be uploaded._
        uploader (_dict_): A `dict` object that contains critical information about the
          object uploaded so far.
    Returns:
        _int_: _The integer value of the newly created siteid from the Neotoma Database._
    """
    params = [
        "analysisunitname",
        "depth",
        "thickness",
        "faciesid",
        "mixed",
        "igsn",
        "notes",
        "recdatecreated",
        "recdatemodified",
    ]
    try:
        inputs = nh.clean_inputs(
            nh.pull_params(params, yml_dict, csv_file, "ndb.analysisunits")
        )
    except Exception:
        response.validAll = False
        response.valid.append(False)
        response.message.append("AU Elements in the CSV file are not properly inserted. Please verify the CSV file")

    inputs["database"] = nh.retrieve_dict(yml_dict, "ndb.datasetdatabases.databasename")
    if inputs["database"][0]['value'].lower() == "East Asian Nonmarine Ostracod Database".lower():
            inputs["analysisunitname"] = f"EANOD/{uploader['collunitid'].handle}/OST"

    ## Placeholder
    if not inputs['mixed']:
        inputs['mixed'] = False
    ## Placeholder

    response = AUResponse()

    kv = {
        "mixed": False,
        "faciesid": None,
        "igsn": None,
        "notes": None,
        "recdatecreated": None,
        "recdatemodified": None,
    }

    for k in kv:
        if (inputs[k] is None or isinstance(inputs[k], bool) or len(inputs[k]) == 1) and inputs['depth']:
            inputs[k] = [inputs[k]] * len(inputs["depth"]) if inputs[k] is not None else [kv[k]] * len(inputs["depth"])

    if not uploader['collunitid'].cuid:
        response.message.append("✗ CU ID needed to create Analysis Unit"
                                " Placeholder `1` will be used to create log.")
        uploader['collunitid'].cuid = 1

    if inputs['depth']:
        for i in range(0, len(inputs["depth"])):
            try:
                au = AnalysisUnit(
                collectionunitid=uploader["collunitid"].cuid,
                analysisunitname=inputs["analysisunitname"],
                depth=inputs["depth"][i],
                thickness=inputs["thickness"][i],
                faciesid=inputs["faciesid"][i],
                mixed=inputs["mixed"][i],
                igsn=inputs["igsn"][i],
                notes=inputs["notes"][i],
                recdatecreated=inputs["recdatecreated"][i],
                recdatemodified=inputs["recdatemodified"][i],
            )
  
            except Exception as e:
                response.message.append(
                    f"✗ Could not create Analysis Unit, " f"verify entries: \n {e}"
                )
                au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid, mixed=False)
            try:
                auid = au.insert_to_db(cur)
                if all(response.valid) == True:
                    response.message.append(f"✔ Added Analysis Unit {auid}.")
                    response.valid.append(True)
                else:
                    response.message.append("✗ Analysis Unit can be created but CU has errors.")
            except Exception as e:
                response.message.append(
                    f"✗ Analysis Unit Data is not correct. Error message: {e}"
                )
                au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid)
                auid = au.insert_to_db(cur)
                response.message.append(
                    f"✗ Adding temporary Analysis Unit {auid} to continue process."
                    f"\nSite will be removed from upload."
                )
                response.valid.append(False)
            response.auid.append(auid)
    else:
        try:
            au = AnalysisUnit(
                    collectionunitid=uploader["collunitid"].cuid,
                    analysisunitname=inputs["analysisunitname"],
                    depth=inputs["depth"],
                    thickness=inputs["thickness"],
                    faciesid=inputs["faciesid"],
                    mixed=inputs["mixed"],
                    igsn=inputs["igsn"],
                    notes=inputs["notes"],
                    recdatecreated=inputs["recdatecreated"],
                    recdatemodified=inputs["recdatemodified"],
                )
        except Exception as e:
            response.message.append(
                f"✗ Could not create Analysis Unit, " f"verify entries: \n {e}"
            )
            au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid, mixed=False)
        try:
            auid = au.insert_to_db(cur)
            response.message.append(f"✔ Added Analysis Unit {auid}.")
            response.valid.append(True)
        except Exception as e:
            response.message.append(
                f"✗ Analysis Unit Data is not correct. Error message: {e}"
            )
            au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid)
            auid = au.insert_to_db(cur)
            response.message.append(
                f"✗ Adding temporary Analysis Unit {auid} to continue process."
                f"\nSite will be removed from upload."
            )
            response.valid.append(False)
        response.auid.append(auid)
    response.validAll = all(response.valid)
    return response
