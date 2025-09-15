import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Speleothem

def insert_speleothem(cur, yml_dict, csv_file, uploader):
    """
    Insert speleothem data into the database.
    This function extracts parameters from a provided CSV file using a YAML dictionary for
    configuration, validates and maps specific speleothem attributes (such as drip type, geology,
    status, and type) against existing entries in the database, and constructs Speleothem objects
    for insertion into the database. If multiple values exist for a parameter, the function iterates
    over the lists, creating and inserting individual speleothem records. It updates a response object
    with validation messages and status.
    Parameters:
        cur (cursor): Database cursor used to execute SQL queries.
        yml_dict (dict): Dictionary with YAML configuration parameters used for mapping CSV fields.
        csv_file (file): CSV file containing speleothem data records.
        uploader (dict): Dictionary containing uploader information; expects a 'sites' key with a 'siteid'
                         attribute to identify the site for the speleothem record.
    Returns:
        Response: An object that includes:
            - speleothem_id: The database identifier of the inserted speleothem record (if insertion succeeds).
            - valid (list): A list of booleans indicating the success or failure of each validation step.
            - message (list): A list of string messages detailing errors or confirmations during processing.
            - validAll (bool): A flag indicating whether all validations passed and the insertion was successful.
    """
    params = ['siteid', 'entityid', 'entityname', 'monitoring', 'rockageid', 'entrancedistance', 
              'entrancedistanceunitsid', 'speleothemtypeid', 'entitystatusid', 'speleothemgeologyid',
              'speleothemdriptypeid', 'dripheight', 'dripheightunitsid', 'covertypeid', 'coverthickness',
              'entitycoverunitsid', 'landusecovertypeid', 'landusecoverpercent', 'landusecovernotes',
              'vegetationcovertypeid', 'vegetationcoverpercent', 'vegetationcovernotes', 'ref_id']
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
        response.validAll = False
        response.message.append(f"Speleothem elements in the CSV file are not properly defined.\n"
                                f"Please verify the CSV file.")
    if inputs['monitoring'].lower() == 'yes':
        inputs['monitoring'] = True
    else:
        inputs['monitoring'] = False
    if isinstance(inputs['ref_id'], str):
        inputs['ref_id'] = list(map(int, inputs['ref_id'].split(',')))
    for inp in inputs:
        if isinstance(inputs[inp], str) and 'id' in inp:
            query = par[inp][0]
            cur.execute(query, {'element': inputs[inp].lower()})
            inputs[inp] = cur.fetchone()
            if not inputs[inp]:
                response.message.append(f"✗  {inp} for {inputs[inp]} not found. "
                                        f"Does it exist in Neotoma?")
                response.valid.append(False)
            else:
                inputs[inp] = inputs[inp][0]
                response.valid.append(True)
    sp = Speleothem(siteid=uploader['sites'].siteid,
                    entityid=inputs['entityid'],
                    entityname=inputs.get('entityname'),
                    monitoring=inputs.get('monitoring'),
                    rockageid=inputs.get('rockageid'),
                    entrancedistance=inputs.get('entrancedistance'),
                    entrancedistanceunits=inputs.get('entrancedistanceunits'),
                    speleothemtypeid=inputs.get('speleothemtypeid'))
    try:
        sp.insert_to_db(cur)
        sp.insert_cu_speleothem_to_db(cur, id = sp.entityid, cuid = uploader['collunitid'].cuid)
        sp.insert_entitygeology_to_db(cur, id = sp.entityid,
                                      speleothemgeologyid = inputs.get('speleothemgeologyid'),
                                      notes = inputs.get('notes'))
        sp.insert_entitydripheight_to_db(cur, id = sp.entityid,
                                         speleothemdriptypeid = inputs.get('speleothemdriptypeid'),
                                         entitydripheight=inputs.get('dripheight'),
                                         entitydripheightunit=inputs.get('dripheightunitsid'))
        sp.insert_entitycovers_to_db(cur, id = sp.entityid,
                                     entitycoverid = inputs.get('entitycoverid'),
                                     entitycoverthickness = inputs.get('coverthickness'),
                                     entitycoverunits = inputs.get('entitycoverunitsid'))
        sp.insert_entitylandusecovers_to_db(cur, id = sp.entityid, 
                                            landusecovertypeid = inputs.get('landusecovertypeid'), 
                                            landusecoverpercent = inputs.get('landusecoverpercent'), 
                                            landusecovernotes = inputs.get('landusecovernotes'))
        sp.insert_entityvegetationcovers_to_db(cur, id = sp.entityid, 
                                               vegetationcovertypeid = inputs.get('vegetationcovertypeid'), 
                                               vegetationcoverpercent = inputs.get('vegetationcoverpercent'), 
                                               vegetationcovernotes = inputs.get('vegetationcovernotes'))
        for i in inputs.get('ref_id', []):
            sp.insert_entityrelationship_to_db(cur, id = sp.entityid, 
                                               entitystatusid = inputs.get('entitystatusid'),
                                               referenceentityid = i)
        response.valid.append(True)
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Speleothem Entity cannot be inserted: " 
                                f"{e}")
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔ Speleothem uploaded ")
    return response