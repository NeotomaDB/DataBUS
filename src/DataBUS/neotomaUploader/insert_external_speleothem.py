import DataBUS.neotomaHelpers as nh
from DataBUS import Response, ExternalSpeleothem

def insert_external_speleothem(cur, yml_dict, csv_file):
    """
    """
    params = ['externalid', 'extdatabaseid', 'externaldescription']
    response = Response()

    query = """SELECT extdatabaseid 
                    FROM ndb.externaldatabases
                    WHERE LOWER(extdatabasename) = %(element)s OR
                    LOWER(url) ILIKE %(element)s;"""
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.externalspeleothemdata")
        print(inputs)
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
            
        try:
            ExternalSpeleothem(entityid=2, #placeholder
                                   externalid=inputs.get('externalid'),
                                   extdatabaseid=inputs.get('extdatabaseid'),
                                   externaldescription=inputs.get('externaldescription'))
        except Exception as e:
            response.message.append(f"✗  Cannot create ExternalSpeleothem object. {e}")
            response.valid.append(False)
    except Exception as e:
        response.message.append(f"✗  Cannot pull external speleothem parameters from CSV file. {e}")
        response.valid.append(False)

    return response