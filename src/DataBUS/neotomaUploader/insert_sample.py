import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response


def insert_sample(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted samples.
            - 'samples' (list): List of sample IDs inserted into the database.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    params = ["sampledate", "samplename", "analysisdate",
             "labnumber", "prepmethod", "notes", "taxonname"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.samples")
    
    get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
    for j in range(len(uploader["anunits"].auid)):
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
            inputs.pop('taxonname', None)  
            inputs['analysisunitid'] = uploader["anunits"].auid[j]
            inputs['datasetid'] = uploader["datasets"].datasetid
            sample = Sample(**inputs)
            response.valid.append(True)
            try:
                s_id = sample.insert_to_db(cur)
                response.sampleid.append(s_id)
                response.valid.append(True)
                response.message.append(f"✔  Added Sample {s_id}.")
            except Exception as e:
                s_id = sample.insert_to_db(cur)
                response.sampleid.append(s_id)
                response.valid.append(True)
                response.message.append(f"✗  Cannot add sample: {e}.")
        except Exception as e:
            sample = Sample()
            response.message.append(f"✗ Samples data is not correct: {e}")
            response.valid.append(False)

    if not len(uploader["anunits"].auid) == len(response.sampleid):
        response.message.append("✗  Analysis Units and Samples do not have same length.")
    response.validAll = all(response.valid)
    return response