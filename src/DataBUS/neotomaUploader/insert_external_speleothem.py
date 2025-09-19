import DataBUS.neotomaHelpers as nh
from DataBUS import Response, ExternalSpeleothem

def insert_external_speleothem(cur, yml_dict, csv_file, uploader): 
    """
    Inserts external speleothem data into the system using parameters provided via a YAML 
    dictionary and a CSV file. This function validates the provided parameters by querying 
    the external databases table via the given database cursor, cleans up the external 
    description, and attempts to create an ExternalSpeleothem object. It collects messages 
    and validity flags in a Response object to indicate the status of each operation.
    Parameters:
        cur (object): A database cursor object used to execute SQL queries.
        yml_dict (dict): A dictionary containing configuration parameters extracted from a YAML file.
        csv_file (str): The path or identifier for a CSV file containing additional parameters.
    Returns:
        Response: An object that contains:
            - message (list): A list of status messages indicating the success or failure of each step.
            - valid (list): A list of booleans corresponding to the validity of each major operation.
    Example:
        response = insert_external_speleothem(cur, yml_dict, csv_file)
    """

    params = ['externalid', 'externaldescription', 'extdatabaseid']
    response = Response()

    query = """SELECT extdatabaseid 
               FROM ndb.externaldatabases
               WHERE LOWER(extdatabasename) = %(element)s OR
               LOWER(url) ILIKE %(element)s;"""
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.externalspeleothemdata")
    except Exception as e:
        response.message.append(f"✗  Cannot pull external speleothem parameters from CSV file. {e}")
        response.valid.append(False)

    if isinstance(inputs.get('extdatabaseid'), str):
        cur.execute(query, {'element': inputs['extdatabaseid'].lower()})
        inputs['extdatabaseid'] = cur.fetchone()
        if not inputs['extdatabaseid']:
            response.message.append(f"✗  extdatabaseid for {inputs.get('extdatabaseid')} not found. "
                                    f"Does it exist in Neotoma?")
            response.valid.append(False)
        else:
            inputs['extdatabaseid'] = inputs['extdatabaseid'][0]
            response.valid.append(True)
            response.message.append(f"✔  extdatabaseid for {inputs.get('extdatabaseid')} found.")
    
    if isinstance(inputs.get('externaldescription'), str):
        inputs['externaldescription'] = inputs['externaldescription'].strip(', ')
        response.valid.append(True)
    try:
        es = ExternalSpeleothem(entityid=uploader['speleothem'].id,
                                externalid=inputs.get('externalid'),
                                extdatabaseid=inputs.get('extdatabaseid'),
                                externaldescription=inputs.get('externaldescription'))
        response.valid.append(True)
        try:
            es.insert_externalspeleothem_to_db(cur)
            response.message.append("✔  ExternalSpeleothem inserted into database.")
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗  Cannot insert ExternalSpeleothem object into database. {e}")
            response.valid.append(False)
    except Exception as e:
        response.message.append(f"✗  Cannot create ExternalSpeleothem object. {e}")
        response.valid.append(False)

    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response