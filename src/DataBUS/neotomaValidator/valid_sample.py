import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response

def valid_sample(cur, yml_dict, csv_file, validator):
    """
    Validates sample data from a YAML dictionary and CSV file against a database.
    Parameters:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): Path to the CSV file containing sample data.
        validator (dict): Dictionary containing validation parameters.
    Returns:
        Response: An object containing validation results
    """
    response = Response()
    params = ["samplename", "analysisdate", "labnumber", 
              "prepmethod", "notes","sampledate",  
              "taxonname"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.samples")

    response.sa_counter = 0
    get_taxonid = """SELECT * FROM ndb.taxa 
                     WHERE LOWER(taxonname) %% %(taxonname)s;"""
    for j in range(0, validator["analysisunit"].aucounter):
        response.sa_counter += 1
        if 'taxonname' in inputs and isinstance(inputs['taxonname'], str):
            inputs['taxonname']=inputs['taxonname'].lower()
            cur.execute(get_taxonid, {"taxonname": inputs["taxonname"]})
            inputs['taxonid'] = cur.fetchone()
            if inputs['taxonid']:
                inputs['taxonid'] = int(inputs['taxonid'][0])
            else:
                inputs['taxonid'] = None
                inputs['taxonid']
        elif 'taxonid' in inputs and isinstance(inputs['taxonid'], int):
            inputs['taxonid'] = int(inputs['taxonid'][0])
        else:
            inputs['taxonid'] = None

        try:
            if 'taxonname' in inputs:
                del inputs['taxonname'] 
            Sample(**inputs)
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Samples data is not correct: {e}")
            response.valid.append(False)
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔ Sample can be created.")
    return response