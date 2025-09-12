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
    params = ['siteid', 'entityid', 'entityname', 
              'monitoring', 'rockageid', 'entrancedistance', 
              'entrancedistanceunits', 'speleothemtypeid', 
              'entitystatusid', 'speleothemgeologyid', 'speleothemdriptypeid',
              'dripheight', 'dripheightunits',
              'covertype', 'coverthickness', 'entitycoverunits',
              'landusecovertypeid', 'landusecoverpercent', 'landusecovernotes']
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
    
    par = {'speleothemdriptypeid': [driptype_q, 'speleothemdriptypeid'],
           'entitystatusid': [entitystatus_q, 'entitystatusid'],
           'speleothemtypeid': [speleothemtypes_q, 'speleothemtypeid'],
           'speleothemgeologyid': [entity_q, 'speleothemgeologyid'],
           'covertype': [covertype_q, 'entitycoverid'],
           'landusecovertypeid': [landusecovertype_q, 'landusecovertypeid']}
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.speleothems")
        print('inputs', inputs)
    except Exception as e:
        response.validAll = False
        response.message.append(f"Speleothem elements in the CSV file are not properly defined.\n"
                                f"Please verify the CSV file.")
    kwargs = {}
    properties = {}
    kwargs['siteid']=uploader['sites'].siteid
    #kwargs['entityid']=inputs['entityid']
    counter = 0
    for k,v in par.items():
        if inputs[k]:
            if inputs[k] != '':
                cur.execute(v[0], {'element': inputs[k].lower()})
                properties[v[1]] = cur.fetchone()
                if not properties[v[1]]:
                    counter +=1
                    response.message.append(f"✗  {k} ID for {inputs[k]} not found. "
                                            f"Does it exist in Neotoma?")
                    response.valid.append(False)
                    properties[v[1]] = None
                else:
                    properties[v[1]] = properties[v[1]][0]
            else:
                inputs[k] = None
                properties[v[1]] = None
                response.message.append(f"?  {inputs[k]} ID not given. ")
                response.valid.append(True)
        else:
            counter +=1
            response.message.append(f"?  {k} ID not given. ")
            response.valid.append(True)
            properties[v[1]] = counter
    kwargs['speleothemtypeid'] = properties['speleothemtypeid']
    sp = Speleothem(**kwargs)
    try:
        if 'dripheight' not in inputs:
            inputs['dripheight'] = None
        if 'entitycoverthickness' not in inputs:
            inputs['coverthickness'] = None
        print(inputs)
        print(properties)
        id = sp.insert_to_db(cur)
        sp.insert_cu_speleothem_to_db(cur, id = id, cuid = uploader['collunitid'].cuid)
        sp.insert_entitygeology_to_db(cur, id = id, 
                                           speleothemgeologyid = properties['speleothemgeologyid'],
                                           notes = None)
        sp.insert_entityrelationship_to_db(cur, id = id, 
                                           entitystatusid = properties['entitystatusid'],
                                           referenceentityid = id)#inputs['referenceentityid'])
        sp.insert_entitydripheight_to_db(cur,
                                         id = id,
                                         speleothemdriptypeid = properties['speleothemdriptypeid'],
                                         entitydripheight=inputs['dripheight'],
                                         entitydripheightunit=inputs['dripheightunits'])
        sp.insert_entitycovers_to_db(cur,
                                      id = id,
                                      entitycoverid = properties['entitycoverid'],
                                      entitycoverthickness = inputs['coverthickness'],
                                      entitycoverunits = inputs['entitycoverunits'])
        print(properties['landusecovertypeid'])
        sp.insert_entitylandusecovers_to_db(cur, 
                                            id = id, 
                                            landusecovertypeid = properties['landusecovertypeid'], 
                                            landusecoverpercent = inputs['landusecoverpercent'], 
                                            landusecovernotes = inputs['landusecovernotes'])
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