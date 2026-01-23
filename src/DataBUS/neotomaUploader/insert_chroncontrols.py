import DataBUS.neotomaHelpers as nh
from DataBUS import ChronControl, Response

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
    response = Response()
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
        if not inputs['analysisunitid']:
            inputs['analysisunitid']= uploader['anunits'].auid
        indices = [i for i, value in enumerate(inputs['age']) if value is not None]
        inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    except Exception as e:
        response.validAll = False 
        response.message.append("Chronology parameters cannot be properly extracted. Verify the CSV file.")
        response.message.append(e)
        return response
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    chroncontrol_q = """SELECT chroncontroltypeid FROM ndb.chroncontroltypes
                WHERE LOWER(chroncontroltype) = LOWER(%(chroncontroltype)s)"""
    try:
        if isinstance(inputs.get("agetype"), list):
            if len(inputs["agetype"]) > 0:
                new_ages = []
                for age in inputs["agetype"]:
                    cur.execute(agetype_q , {"agetype": age})
                    agetype = cur.fetchone()
                    if agetype:
                        response.message.append("✔ The provided age type is correct.")
                        response.valid.append(True)
                        new_ages.append(age)
                    else:
                        response.message.append("✗ The provided age type is incorrect..")
                        response.valid.append(False)
                        new_ages.append(None)
                inputs["agetype"] = new_ages
                inputs["agetype"] = inputs["agetype"][0]
            else:
                inputs["agetype"] = None
        elif isinstance(inputs.get("agetype"), str):
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
    if inputs.get('chroncontroltypeid'):
        inputs['chroncontroltypeid'] = [sisal_t.get(item, item) for item in inputs['chroncontroltypeid']]
        elements = set(inputs['chroncontroltypeid'])
        elements.discard(None)
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
    chronologies = uploader['chronology'].id
    for chron in chronologies:
        for values in zip(*iterable_params.values()):
            try:
                kwargs = dict(zip(iterable_params.keys(), values))
                kwargs.update(static_params)
                kwargs['chronologyid'] = chron
                cc = ChronControl(**kwargs)
                ccid = cc.insert_to_db(cur)
                response.id.append(ccid)
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"✗  Could not create chron control {e}")
                response.valid.append(False)
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔  Chron control can be created")
    response.message = list(set(response.message))
    response.indices.append(indices)
    return response