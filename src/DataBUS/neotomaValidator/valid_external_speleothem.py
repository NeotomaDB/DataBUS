import DataBUS.neotomaHelpers as nh
from DataBUS import Response, ExternalSpeleothem

def valid_external_speleothem(cur, yml_dict, csv_file): 
    """
    Validates external speleothem data by checking required parameters and cross-referencing with the database.
    This function pulls input parameters related to an external speleothem record from the given
    YAML dictionary and CSV file, verifies the external database identifier by querying the database,
    and attempts to create an ExternalSpeleothem object to ensure data integrity.
    Parameters:
        cur (cursor): A database cursor used to execute queries against the ndb.externaldatabases table.
        yml_dict (dict): A dictionary of configuration parameters, likely loaded from a YAML file.
        csv_file (str): The path or identifier of a CSV file containing external speleothem data.
    Returns:
        Response: An object containing two main attributes:
            - valid (list of bool): Flags indicating the success or failure of each validation step.
            - message (list of str): Messages detailing the outcome of each validation, including errors.
    Exceptions:
        Any exceptions raised during parameter extraction or database operations are caught,
        and corresponding error messages are appended to the response.
    """
    params = ['externalid', 'externaldescription', 'extdatabaseid']
    response = Response()

    query = """SELECT extdatabaseid 
               FROM ndb.externaldatabases
               WHERE LOWER(extdatabasename) = %(element)s OR
               LOWER(url) ILIKE %(element)s;"""
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.externalspeleothemdata")
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
            ExternalSpeleothem(entityid=2, #placeholder
                                    externalid=inputs.get('externalid'),
                                    extdatabaseid=inputs.get('extdatabaseid'),
                                    externaldescription=inputs.get('externaldescription'))
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗  Cannot create ExternalSpeleothem object. {e}")
            response.valid.append(False)
    except Exception as e:
        response.message.append(f"✗  Cannot pull external speleothem parameters from CSV file. {e}")
        response.valid.append(False)

    return response