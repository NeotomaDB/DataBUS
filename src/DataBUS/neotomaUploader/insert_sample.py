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
    params = ["sampledate", "samplename", "analysisdate",
             "labnumber", "prepmethod", "notes", "taxonname"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.samples")
    get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""

    entry = {'datasetid': uploader["geochrondatasets"].datasetid}
    try:
        sample = Sample(**entry)
        response.valid.append(True)
        try:
            s_id = sample.insert_to_db(cur)
            response.sampleid.append(s_id)
            response.valid.append(True)
            response.message.append(f"✔  Added Samples.")
        except Exception as e:
            s_id = sample.insert_to_db(cur)
            response.sampleid.append(s_id)
            response.valid.append(True)
            response.message.append(f"✗  Cannot add sample: {e}.")
    except Exception as e:
        sample = Sample()
        response.message.append(f"✗ Samples data is not correct: {e}")
        response.valid.append(False)

    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response