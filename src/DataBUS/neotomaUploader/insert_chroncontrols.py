import DataBUS.neotomaHelpers as nh
from DataBUS import ChronControl, ChronResponse

def insert_chroncontrols(cur, yml_dict, csv_file, uploader):
    """
    Inserts chronological control data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted chronological control units.
            'chron_control_units' (list): List of IDs for the inserted chronological control units.
            'valid' (bool): Indicates if all insertions were successful.

    """
    response = ChronResponse()
    params = ['chronologyid', 'chroncontroltypeid', 
              'depth', 'thickness', 'age', 
              'agelimityounger', 'agelimitolder', 
              'notes', 'analysisunitid', 'agetype']
    
    sisal_t = {'MC-ICP-MS U/Th':'Uranium-series',
       'ICP-MS U/Th':'Other Uranium-series', 
       'Alpha U/Th':'Uranium-series', 
       'U/Th':'unspecified Uranium-series', 
       'Event; actively forming':'Active deposition surface', 
       'Event; start of laminations':'Annual laminations (varves)', 
       'Event; end of laminations':'Annual laminations (varves)', 
       'C14': 'Radiocarbon, calibrated', 
       'Multiple methods':'Complex (mixture of types)', 
       'other (see notes)':'Other dating methods'}
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.chroncontrols")
        indices = [i for i, value in enumerate(inputs['age']) if value is not None]
        inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    except Exception as e:
        response.validAll = False 
        response.message.append("Chronology parameters cannot be properly extracted. Verify the CSV file.")
        response.message.append(e)
        return response
    
    inputs['chronologyid'] = uploader['chronology'].chronid
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    chroncontrol_q = """SELECT chroncontroltypeid FROM ndb.chroncontroltypes
                WHERE LOWER(chroncontroltype) = LOWER(%(chroncontroltype)s)"""
    try:
        if inputs["agetype"]:
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
        else:
            response.message.append("? No age type provided.")
            response.valid.append(True)
            inputs["agetypeid"] = None
    except (KeyError, Exception) as e:
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
    if inputs['depth']:
        for i in range(0, len(inputs["depth"])):
            if inputs['chroncontroltypeid'][i] in sisal_t:
                inputs['chroncontroltypeid'][i] = sisal_t[inputs['chroncontroltypeid'][i]]
            cur.execute(chroncontrol_q, {"chroncontroltype": inputs['chroncontroltypeid'][i]})
            chroncontrol = cur.fetchone()
            if chroncontrol:
                inputs["chroncontrolid"] = chroncontrol[0]
                response.valid.append(True)
            else:
                response.message.append(f"✗  Chron control type {inputs['chroncontroltypeid'][i]} not found in database")
                response.valid.append(False)
                inputs["chroncontrolid"] = None
            try:
                cc = ChronControl(
                    analysisunitid=uploader['anunits'].auid[i],
                    chronologyid = inputs['chronologyid'],
                    chroncontroltypeid=inputs["chroncontrolid"],
                    depth=inputs["depth"][i],
                    thickness=inputs["thickness"][i],
                    age=inputs["age"][i],
                    agelimityounger=inputs['agelimityounger'][i],
                    agelimitolder=inputs['agelimitolder'][i],
                    notes=inputs["notes"],
                    agetypeid=inputs["agetypeid"],
                )

                ccid = cc.insert_to_db(cur)
                response.ccid.append(ccid)
                response.message.append(f"✔ Added Chron Control {ccid}.")
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"✗  Could not create chron control {e}")
                response.valid.append(False)
    else:
        if inputs.get('chroncontroltypeid') and inputs['chroncontroltypeid'] in sisal_t:
            inputs['chroncontroltypeid']  = sisal_t[inputs['chroncontroltypeid'][0]]
        try:
            cur.execute(chroncontrol_q, {"chroncontroltype": inputs["chroncontroltypeid"]})
            chroncontrol = cur.fetchone()
            if chroncontrol:
                inputs["chroncontrolid"] = chroncontrol[0]
                response.valid.append(True)
            else:
                response.message.append(f"✗  Chron control type {inputs['chroncontroltypeid']} not found in database")
                response.valid.append(False)
                inputs["chroncontrolid"] = None
            cc = ChronControl(
                        analysisunitid=uploader['anunits'].auid[0],
                        chronologyid=inputs['chronologyid'],
                        chroncontroltypeid=inputs["chroncontrolid"],
                        depth=inputs["depth"],
                        thickness=inputs["thickness"],
                        age=inputs["age"],
                        agelimityounger=inputs['agelimityounger'],
                        agelimitolder=inputs['agelimitolder'],
                        notes=inputs["notes"],
                        agetypeid=inputs["agetypeid"],
                    )
            ccid = cc.insert_to_db(cur)
            response.ccid.append(ccid)
            response.message.append(f"✔ Added Chron Control {ccid}.")
            response.valid.append(True)
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗  Could not create 1 chron control {e}")
            response.valid.append(False)

    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔  Chron control can be created")
    response.message = list(set(response.message))
    return response