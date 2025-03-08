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
              'entitystatusid', 'speleothemgeologyid', 'speleothemdriptypeid']
    response = Response()

    driptype_q = """SELECT speleothemdriptypeid 
                    FROM ndb.speleothemdriptypes
                    WHERE LOWER(speleothemdriptype) = %(element)s;"""
    entity_q = """SELECT speleothemgeologyid 
                  FROM ndb.speleothementitygeology
                  WHERE LOWER(speleothemgeology) = %(element)s;"""
    entitystatus_q = """SELECT entitystatusid 
                        FROM ndb.speleothementitystatuses
                        WHERE LOWER(entitystatus) = %(element)s;"""
    speleothemtypes_q = """SELECT speleothemtypeid 
                           FROM ndb.speleothemtypes
                           WHERE LOWER(speleothemtype) = %(element)s;"""
    
    par = {'speleothemdriptypeid': [driptype_q, 'speleothemdriptypeid'], 
           'speleothemgeologyid': [entity_q, 'speleothemgeologyid'], 
           'entitystatusid': [entitystatus_q, 'entitystatusid'],
           'speleothemtypeid': [speleothemtypes_q, 'speleothemtypeid']}
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.speleothems")
    except Exception as e:
        response.validAll = False
        response.message.append(f"Speleothem elements in the CSV file are not properly defined.\n"
                                f"Please verify the CSV file.")
        inputs = {}
    kwargs = {}
    kwargs['siteid']=uploader['sites'].siteid
    kwargs['entityid']=inputs['entityid']
    counter = 0
    for k,v in par.items():
        if inputs[k]:
            if inputs[k] != '':
                cur.execute(v[0], {'element': inputs[k].lower()})
                kwargs[v[1]] = cur.fetchone()
                if not kwargs[v[1]]:
                    counter +=1
                    response.message.append(f"✗  {k} ID for {inputs[k][i]} not found. "
                                            f"Does it exist in Neotoma?")
                    response.valid.append(False)
                    kwargs[v[1]] = None
                else:
                    kwargs[v[1]] = kwargs[v[1]][0]
            else:
                inputs[k] = None
                kwargs[v[1]] = None
                response.message.append(f"?  {inputs[k]} ID not given. ")
                response.valid.append(True)
        else:
            counter +=1
            response.message.append(f"?  {k} ID not given. ")
            response.valid.append(True)
            kwargs[v[1]] = counter
    sp = Speleothem(**kwargs)
    try:
        sp.insert_to_db(cur)
        sp.insert_cu_speleothem_to_db(cur, uploader['collunitid'].cuid)
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