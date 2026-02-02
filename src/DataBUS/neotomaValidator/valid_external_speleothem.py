import DataBUS.neotomaHelpers as nh
from DataBUS import Response, ExternalSpeleothem
from DataBUS.Speleothem import EX_SP_PARAMS

def valid_external_speleothem(cur, yml_dict, csv_file):
    """Validates external speleothem data against the Neotoma database.

    Validates external speleothem parameters including external database ID,
    external ID, and description. Queries the database for valid external
    database references and creates ExternalSpeleothem objects.

    Args:
        cur (cursor): Database cursor to query ndb.externaldatabases table.
        yml_dict (dict): Dictionary of configuration parameters from YAML file.
        csv_file (str): Path or identifier for CSV file with external speleothem data.

    Returns:
        Response: Response object with validation results and messages.

    Exceptions:
        Catches exceptions during parameter extraction and database operations,
        appending error messages to the response.
    
    Examples:
        >>> valid_external_speleothem(cursor, config_dict, "ext_speleothem_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    params = EX_SP_PARAMS
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