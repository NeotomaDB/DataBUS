import DataBUS.neotomaHelpers as nh
from DataBUS import Response, UThSeries

def valid_uth_series(cur, yml_dict, csv_file):
    """
    Validates data from a CSV file against a YAML dictionary and a database.
    Parameters:
    cur (psycopg2.cursor): Database cursor for executing SQL queries.
    yml_dict (dict): Dictionary containing YAML configuration data.
    csv_file (str): Path to the CSV file containing data to be validated.
    validator (Validator): Validator object for additional validation logic.
    Returns:
    Response: A response object containing validation results and messages.
    """
    params = ['geochronid', 'decayconstantid',
              'activityuncertainty234u238u', 'activity234u238u', 'ratio230th232th',
              'activityuncertainty230th238u', 'activity230th238u', 'ratiouncertainty230th232th',
              'iniratiouncertainty230th232th', 'iniratio230th232th']
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.uraniumseries")

    elements = [x for x in params if x not in {'geochronid', 'decayconstantid'}]
    response = Response()
    filtered_inputs = {k: v for k, v in inputs.items() if k in elements}
    indices = [i for i, values in enumerate(zip(*filtered_inputs.values()))
               if any(value is not None for value in values)]

    inputs = {k: [v for i, v in enumerate(filtered_inputs[k]) if i in indices] if k in filtered_inputs
                                                              else value for k, value in inputs.items()}
    if inputs['decayconstantid'] is not None:
        decay_query = """SELECT decayconstantid FROM ndb.decayconstants
                    WHERE LOWER(decayconstant) = %(decayconstant)s;"""
        cur.execute(decay_query, {'decayconstant': inputs['decayconstantid'].lower()})
        decayconstantid = cur.fetchone()
        if decayconstantid is not None:
            inputs['decayconstantid'] = decayconstantid[0]
            response.valid.append(True)
            response.message.append("✔ Decay constant found in database")
        else:
            response.valid.append(False)
            response.message.append(f"✗ Decay constant {inputs['decayconstantid']} not found in database")
    
    for i in range(len(indices)):
        try:
            uth = UThSeries(geochronid=inputs['geochronid'],
                            decayconstantid=inputs['decayconstantid'][i],
                            ratio230th232th=inputs['ratio230th232th'][i],
                            ratiouncertainty230th232th=inputs['ratiouncertainty230th232th'][i],
                            activity230th238u=inputs['activity230th238u'][i],
                            activityuncertainty230th238u=inputs['activityuncertainty230th238u'][i],
                            activity234th238u=inputs['activity234th238u'][i],
                            activityuncertainty234th238u=inputs['activityuncertainty234th238u'][i],
                            iniratio230th232th=inputs['iniratio230th232th'][i],
                            iniratiouncertainty230th232th=inputs['iniratiouncertainty230th232th'][i])
            uth.insert_to_db(cur)
            response.valid.append(True)
            response.message.append("✔ UThSeries can be created")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ UThSeries cannot be created: {e}")
    
    return response