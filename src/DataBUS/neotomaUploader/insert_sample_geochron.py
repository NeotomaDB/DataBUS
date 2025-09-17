import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response

def insert_sample_geochron(cur, yml_dict, csv_file, uploader):
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
    inputs = {}
    inputs['anunits'] = uploader['anunits'].auid
    age = nh.pull_params(["age"], yml_dict, csv_file, "ndb.geochronology")['age']
    indices = [i for i, value in enumerate(age) if value is not None]
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}

    for unit in inputs['anunits']:
        entry = {'analysisunitid': unit,
                 'datasetid': uploader["geochrondatasets"].datasetid}
        try:
            sample = Sample(**entry)
            response.valid.append(True)
            try:
                s_id = sample.insert_to_db(cur)
                response.sampleid.append(s_id)
                response.valid.append(True)
                response.message.append(f"✔  Added Geochron Samples.")
            except Exception as e:
                s_id = sample.insert_to_db(cur)
                response.sampleid.append(s_id)
                response.valid.append(True)
                response.message.append(f"✗  Cannot add geocrhon sample: {e}.")
        except Exception as e:
            sample = Sample()
            response.message.append(f"✗ Geocrhon sample data is not correct: {e}")
            response.valid.append(False)
    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response