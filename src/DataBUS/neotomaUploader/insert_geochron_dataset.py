import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response

def insert_geochron_dataset(cur, yml_dict, csv_file, uploader, name=None):
    """
    Inserts a dataset associated with a collection unit into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted dataset.
            'datasetid' (int): IDs for the inserted dataset.
            'valid' (bool): Indicates if insertions were successful.
    """
    response = Response()

    inputs = {}
    inputs['datasettypeid'] = 1
    inputs['collectionunitid'] = uploader['collunitid'].cuid

    ds = Dataset(**inputs)
    try:
        response.datasetid = ds.insert_to_db(cur)
        response.valid.append(True)
        response.message.append(f"✔ Added Dataset {response.datasetid}.")
    except Exception as e:
        response.datasetid = ds.insert_to_db(cur)
        response.valid.append(True)
        response.message.append(
            f"✗ Cannot add Dataset {response.datasetid}." f"Using temporary ID."
        )

    response.validAll = all(response.valid)
    return response