import DataBUS.neotomaHelpers as nh
from DataBUS import Response, UThSeries

def insert_uth_series(cur, yml_dict, csv_file, uploader):
    """
    Uploads data from a CSV file against a YAML dictionary and a database.
    Parameters:
    cur (psycopg2.cursor): Database cursor for executing SQL queries.
    yml_dict (dict): Dictionary containing YAML configuration data.
    csv_file (str): Path to the CSV file containing data to be validated.
    validator (Validator): Validator object for additional validation logic.
    Returns:
    Response: A response object containing validation results and messages.
    """
    response = Response()
    params = ['decayconstantid',
              'ratio230th232th', 'ratiouncertainty230th232th',
              'activity230th238u', 'activityuncertainty230th238u', 
              'activity234u238u', 'activityuncertainty234u238u',  
              'iniratio230th232th', 'iniratiouncertainty230th232th']
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.uraniumseries")

    indices = [i for i, values in enumerate(zip(*inputs.values()))
               if any(value is not None for value in values)]
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if k in inputs and k != "decayconstantid" 
                                                                      else value for k, value in inputs.items()}
    inputs['geochronid'] = uploader['geochron'].id
    #assert that geochronid.id and all other inputs have the same length
    if not all(len(inputs['geochronid']) == len(v) for k, v in inputs.items() if k not in ['geochronid', 'decayconstantid']):
        response.message.append("✗ Length of geochronid does not match length of other inputs")
        response.valid.append(False)

    if inputs.get('decayconstantid') is not None:
        decay_query = """SELECT decayconstantid FROM ndb.decayconstants
                    WHERE LOWER(decayconstant) = %(decayconstant)s;"""
        cur.execute(decay_query, {'decayconstant': 
                                  inputs.get('decayconstantid', '').lower()})
        decayconstantid = cur.fetchone()
        if decayconstantid:
            inputs['decayconstantid'] = decayconstantid[0]
            response.valid.append(True)
            response.message.append("✔ Decay constant found in database")
        else:
            response.valid.append(False)
            response.message.append(f"✗ Decay constant {inputs['decayconstantid']} not found in database")
    
    for i in range(len(indices)):
        try:
            uth = UThSeries(geochronid=uploader['geochron'].id[i],
                            decayconstantid=inputs['decayconstantid'],
                            ratio230th232th=inputs['ratio230th232th'][i],
                            ratiouncertainty230th232th=inputs['ratiouncertainty230th232th'][i],
                            activity230th238u=inputs['activity230th238u'][i],
                            activityuncertainty230th238u=inputs['activityuncertainty230th238u'][i],
                            activity234u238u=inputs['activity234u238u'][i],
                            activityuncertainty234u238u=inputs['activityuncertainty234u238u'][i],
                            iniratio230th232th=inputs['iniratio230th232th'][i],
                            iniratiouncertainty230th232th=inputs['iniratiouncertainty230th232th'][i])
            response.valid.append(True)
            try:
                uth.insert_to_db(cur)
                response.valid.append(True)
                response.message.append("✔ UThSeries has been inserted")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ UThSeries cannot be inserted: {e}")
        except Exception as e:
            response.message.append(f"✗ UThSeries cannot be created: {e}")
            response.valid.append(False)
    
    
    # Insert UraniumSeriesData
    uraniumseries = ['238U', '230Th', '232Th']
    uthdata = {}
    for u in uraniumseries:
        uthdata[u] = uploader['data'].data_id.get(u)
        if uthdata[u] is None:
            uthdata.pop(u, None)
    uthdata = {k: [v for v in vals if v is not None] for k, vals in uthdata.items()}

    if not all(len(inputs['geochronid']) == len(v) for v in uthdata.values()):
        response.message.append("✗ Length of geochronid does not match length of other inputs")
        response.valid.append(False)
        
    for i in inputs['geochronid']:
        for u in uraniumseries:
            if uthdata.get(u):
                try:
                    uth.insert_uraniumseriesdata(cur, uthdata[u][i], inputs['geochronid'][i])
                    response.valid.append(True)
                    response.message.append("✔ UraniumSeriesData has been inserted")
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗ Uranium Series Data cannot be inserted: {e}")
            else:
                response.valid.append(True)
                response.message.append("✔ No UraniumSeriesData to insert")
                continue
                
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response