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

    for i, unit in enumerate(uploader["anunits"].auid):
        entry = {'analysisunitid': unit,
                 'datasetid': uploader["datasets"].datasetid,
                 'sampledate': inputs['sampledate'][i] if isinstance(inputs.get('sampledate'), list) else inputs['sampledate'],
                 'samplename': inputs['samplename'][i] if isinstance(inputs.get('samplename'), list) else inputs['samplename'],
                 'analysisdate': inputs['analysisdate'][i] if isinstance(inputs.get('analysisdate'), list) else inputs['analysisdate'],
                 'taxonname': inputs['taxonname'][i] if isinstance(inputs.get('taxonname'), list) else inputs['taxonname'],
                 'labnumber': None if isinstance(inputs.get('labnumber'), list) and inputs['labnumber'][i] == ''
                                   else inputs['labnumber'][i] if isinstance(inputs.get('labnumber'), list) 
                                   else inputs['labnumber']}
        if 'taxonname' in entry and isinstance(entry['taxonname'], str):
            entry['taxonname']=entry['taxonname'].lower()
            cur.execute(get_taxonid, {"taxonname": entry["taxonname"]})
            entry['taxonid'] = cur.fetchone()
            if entry['taxonid']:
                entry['taxonid'] = int(entry['taxonid'][0])
            else:
                entry['taxonid'] = None
                entry['taxonid']
        elif 'taxonid' in entry and isinstance(entry['taxonid'], int):
            entry['taxonid'] = int(entry['taxonid'][0])
        else:
            entry['taxonid'] = None
        try:
            entry.pop('taxonname', None)
            sample = Sample(**entry)
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