import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Speleothem
from DataBUS.Speleothem import SPELEOTHEM_PARAMS

def valid_speleothem(cur, yml_dict, csv_file):
    """Validates speleothem data against the Neotoma database.

    Validates speleothem parameters including entity properties, drip type, geology,
    cover type, and land use information. Queries the database for valid values
    and creates a Speleothem object with validated parameters.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing speleothem data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.
    
    Examples:
        >>> valid_speleothem(cursor, config_dict, "speleothem_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    params = SPELEOTHEM_PARAMS
    response = Response()

    driptype_q = """SELECT speleothemdriptypeid
                    FROM ndb.speleothemdriptypes
                    WHERE LOWER(speleothemdriptype) = %(element)s;"""
    entity_q = """SELECT rocktypeid 
                  FROM ndb.rocktypes
                  WHERE LOWER(rocktype) = %(element)s;"""
    entitystatus_q = """SELECT entitystatusid 
                        FROM ndb.speleothementitystatuses
                        WHERE LOWER(entitystatus) = %(element)s;"""
    speleothemtypes_q = """SELECT speleothemtypeid 
                           FROM ndb.speleothemtypes
                           WHERE LOWER(speleothemtype) = %(element)s;"""
    covertype_q = """SELECT entitycoverid 
                     FROM ndb.entitycovertypes
                     WHERE LOWER(entitycovertype) = %(element)s;"""
    landusecovertype_q = """SELECT landusecovertypeid 
                            FROM ndb.landusetypes
                            WHERE LOWER(landusecovertype) = %(element)s;"""
    vegetationcovertype_q = """SELECT vegetationcovertypeid 
                               FROM ndb.vegetationcovertypes
                               WHERE LOWER(vegetationcovertype) = %(element)s;"""
    rockage_q = """SELECT relativeageid
                   FROM ndb.relativeages
                   WHERE LOWER(relativeage) = %(element)s;"""
    rocktype_q = """SELECT rocktypeid
                    FROM ndb.rocktypes
                    WHERE LOWER(rocktype) = %(element)s;"""
    units_q = """SELECT variableunitsid
                 FROM ndb.variableunits
                 WHERE LOWER(variableunits) = %(element)s"""
    par = {'speleothemdriptypeid': [driptype_q, 'speleothemdriptypeid'],
           'entitystatusid': [entitystatus_q, 'entitystatusid'],
           'speleothemtypeid': [speleothemtypes_q, 'speleothemtypeid'],
           'speleothemgeologyid': [entity_q, 'speleothemgeologyid'],
           'covertypeid': [covertype_q, 'entitycoverid'],
           'landusecovertypeid': [landusecovertype_q, 'landusecovertypeid'],
           'vegetationcovertypeid': [vegetationcovertype_q, 'vegetationcovertypeid'],
           'rockageid': [rockage_q, 'rockageid'],
           'rocktypeid': [rocktype_q, 'rocktypeid'],
           'dripheightunitsid': [units_q, 'dripheightunitsid'],
           'entitycoverunitsid': [units_q, 'entitycoverunitsid'],
           'entrancedistanceunitsid': [units_q, 'entrancedistanceunitsid']}
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.speleothems")
    except Exception as e:
        if 'ref_id' in str(e):
            params.remove('ref_id')
            inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.speleothems")
        else:
            inputs = {}
        response.valid.append(False)
        response.message.append(f"Speleothem elements in the CSV file are not properly defined.\n"
                                f"Please verify the CSV file. {e}")
    if inputs.get('monitoring', '').lower() == 'yes':
        inputs['monitoring'] = True
    else:
        inputs['monitoring'] = False
    if isinstance(inputs.get('ref_id'), str):
        inputs['ref_id'] = list(map(int, inputs.get('ref_id', []).split(',')))
    for inp in inputs:
        el = inputs[inp]
        if isinstance(inputs.get(inp), str) and 'id' in inp and inputs[inp] != None:
            query = par[inp][0]
            cur.execute(query, {'element': inputs[inp].lower()})
            inputs[inp] = cur.fetchone()
            if not inputs[inp]:
                response.message.append(f"✗  {inp} for {el} not found. "
                                        f"Does it exist in Neotoma?")
                response.valid.append(False)
            else:
                inputs[inp] = inputs[inp][0]
                response.valid.append(True)
                response.message.append(f"✔  {inp} for {inputs[inp]} found.")
    try:
        Speleothem(siteid=1, # Temporary siteid
                    entityid=inputs['entityid'],
                    entityname=inputs.get('entityname'),
                    monitoring=inputs.get('monitoring'),
                    rockageid=inputs.get('rockageid'),
                    entrancedistance=inputs.get('entrancedistance'),
                    entrancedistanceunits=inputs.get('entrancedistanceunits'),
                    speleothemtypeid=inputs.get('speleothemtypeid'))
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Speleothem Entity cannot be created: " 
                                f"{e}")
    response.message = list(set(response.message))
    if response.validAll:
        response.message.append("✔ Speleothem can be created")
    return response