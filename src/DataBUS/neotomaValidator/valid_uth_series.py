import DataBUS.neotomaHelpers as nh
from DataBUS import Response, UThSeries
from DataBUS.UThSeries import UTH_PARAMS

def valid_uth_series(cur, yml_dict, csv_file):
    """Validates uranium-thorium series data for geochronological samples.

    Validates U-Th series isotope data including isotope ratios, activities, and
    associated decay constants. Verifies decay constants exist in the database and
    creates UThSeries objects with validated parameters.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data with U-Th parameters.
        csv_file (str): Path to CSV file containing U-Th series data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.
    
    Examples:
        >>> valid_uth_series(cursor, config_dict, "uth_series_data.csv")
        Response(valid=[True, True], message=[...], validAll=True)
    """
    params = UTH_PARAMS
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.uraniumseries")
    if isinstance(inputs.get('decayconstantid'), list):
        elements = [x for x in params if x not in {'geochronid'}]
    else:
        elements = [x for x in params if x not in {'geochronid', 'decayconstantid'}]
    response = Response()
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
    
    for i in range(len(indices)):
        try:
            if isinstance(inputs.get('decayconstantid'), list):
                dc_id = inputs['decayconstantid'][i]
            else:
                dc_id = inputs['decayconstantid']
            UThSeries(geochronid=inputs['geochronid'],
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
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ UThSeries cannot be created: {e}")
    response.message = list(set(response.message))
    return response