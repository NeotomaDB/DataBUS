import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Speleothem

def valid_speleothem(cur, yml_dict, csv_file):
    """
    Validates speleothem data from a CSV file against the Neotoma database.
    Parameters:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to the CSV file containing speleothem data.
    Returns:
        Response: An object containing validation results and messages.
    """
    params = ["siteid", 'entityid', 'entityname', 
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
    inputs['siteid']=None

    iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
    static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
    for values in zip(*iterable_params.values()):  # Loops over the lists
        try:
            kwargs = dict(zip(iterable_params.keys(), values))  # Create dictionary with lists
            kwargs.update(static_params)
            counter = 0
            for k,v in par.items():
                if kwargs[k]:
                    if kwargs[k] != '':
                        cur.execute(v[0], {'element': kwargs[k].lower()})
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
                        kwargs[k] = None
                        kwargs[v[1]] = None
                        response.message.append(f"?  {kwargs[k]} ID not given. ")
                        response.valid.append(True)
                else:
                    response.message.append(f"?  {k} ID not given. ")
                    response.valid.append(True)
                    kwargs[v[1]] = counter
            Speleothem(**kwargs)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Speleothem Entity cannot be created: " 
                                    f"{e}")
        break
    
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append("✔ Speleothem can be created")
    return response