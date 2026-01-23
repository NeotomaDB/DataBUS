from DataBUS import Response, UThSeries, insert_uraniumseriesdata
import DataBUS.neotomaHelpers as nh

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
    if not uploader['geochron'].id:
        response.message.append("✔ No geochron IDs to insert")
        response.valid.append(True)
        response.validAll = all(response.valid)
        return response
    else:
        params = ['decayconstantid',
                'ratio230th232th', 'ratiouncertainty230th232th',
                'activity230th238u', 'activityuncertainty230th238u', 
                'activity234u238u', 'activityuncertainty234u238u',  
                'iniratio230th232th', 'iniratiouncertainty230th232th']
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.uraniumseries")
        inputs['geochronid'] = uploader['geochron'].id
    if isinstance(inputs.get('decayconstantid'), list):
        elements = [x for x in params if x not in {'geochronid'}]
    else:
        elements = [x for x in params if x not in {'geochronid', 'decayconstantid'}]
    filtered_inputs = {k: v for k, v in inputs.items() if k in elements}
    indices = [i for i, values in enumerate(zip(*filtered_inputs.values()))
               if any(value is not None for value in values)]

    inputs = {k: [v for i, v in enumerate(filtered_inputs[k]) if i in indices] if k in filtered_inputs
                                                              else value for k, value in inputs.items()}
    decay_query = """SELECT decayconstantid FROM ndb.decayconstants
                                WHERE LOWER(decayconstant) = %(decayconstant)s;"""
    if inputs.get('decayconstantid') is not None:
        if isinstance(inputs['decayconstantid'], list):
            new_dc = []
            for dc in inputs['decayconstantid']:
                cur.execute(decay_query, {'decayconstant': dc.lower()})
                decayconstantid = cur.fetchone()
                if decayconstantid is not None:
                    new_dc.append(decayconstantid[0])
                    response.valid.append(True)
                    response.message.append("✔ Decay constant found in database")
                else:
                    new_dc.append(None)
                    response.valid.append(False)
                    response.message.append(f"✗ Decay constant {dc} not found in database")
            inputs['decayconstantid'] = new_dc
        elif isinstance(inputs['decayconstantid'], str):
            decay_query = """SELECT decayconstantid FROM ndb.decayconstants
                        WHERE LOWER(decayconstant) = %(decayconstant)s;"""
            cur.execute(decay_query, {'decayconstant': inputs['decayconstantid'].lower()})
            decayconstantid = cur.fetchone()
            if decayconstantid is not None:
                inputs['decayconstantid'] = decayconstantid[0]
                response.valid.append(True)
                response.message.append("✔ Decay constant found in database")
            else:
                inputs['decayconstantid'] = None
                response.valid.append(False)
                response.message.append(f"✗ Decay constant {inputs['decayconstantid']} not found in database")
        
        if not indices:
            response.message.append("✔ No UTh Series data to insert")
            response.valid.append(True)
            response.validAll = all(response.valid)
            return response
        else:
            for i in range(len(indices)):
                try:
                    if isinstance(inputs.get('decayconstantid'), list):
                        dc_id = inputs['decayconstantid'][i]
                    else:
                        dc_id = inputs['decayconstantid']
                    uth = UThSeries(geochronid=inputs['geochronid'][i],
                                    decayconstantid=dc_id,
                                    ratio230th232th=inputs['ratio230th232th'][i],
                                    ratiouncertainty230th232th=inputs['ratiouncertainty230th232th'][i],
                                    activity230th238u=inputs['activity230th238u'][i],
                                    activityuncertainty230th238u=inputs['activityuncertainty230th238u'][i],
                                    activity234u238u=inputs['activity234u238u'][i],
                                    activityuncertainty234u238u=inputs['activityuncertainty234u238u'][i],
                                    iniratio230th232th=inputs['iniratio230th232th'][i],
                                    iniratiouncertainty230th232th=inputs['iniratiouncertainty230th232th'][i])
                    response.valid.append(True)
                    response.message.append("✔ UThSeries can be created")
                    try:
                        uth.insert_to_db(cur)
                        response.valid.append(True)
                        response.message.append("✔ UThSeries has been inserted")
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"✗ UThSeries cannot be inserted: {e}")
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗ UThSeries cannot be created: {e}")

        # For the insert, insert UraniumSeriesData ID and the geochronID associated with the UThSeries
        # Insert UraniumSeriesData
        uraniumseries = ['238U', '230Th', '232Th']
        uthdata = {}
        for u in uraniumseries:
            uthdata[u] = uploader['data'].data_id.get(u)
            if uthdata[u] is None:
                uthdata.pop(u, None)
        uthdata = {k: [v for v in vals if v is not None] for k, vals in uthdata.items()}
        if uthdata:
            for k, v in uthdata.items():
                if isinstance(v, list) and len(v): 
                    for i in range(len(v)):
                        try:
                            insert_uraniumseriesdata(cur, v[i], inputs['geochronid'][i])
                            response.valid.append(True)
                            response.message.append("✔ UraniumSeriesData has been inserted")
                        except Exception as e:
                            response.valid.append(False)
                            response.message.append(f"✗ Uranium Series Data cannot be inserted: {e}")              
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response