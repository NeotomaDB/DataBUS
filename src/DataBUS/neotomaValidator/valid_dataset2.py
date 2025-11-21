import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response

def valid_dataset2(cur, yml_dict, csv_file, name=None):
    """
    Validates a dataset based on provided YAML dictionary and CSV file.

    Args:
        cur (cursor): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): Path to the CSV file.
        name (str, optional): Name of the dataset. Defaults to None.

    Returns:
        Response: An object containing validation results, including messages and validity status.

    Raises:
        Exception: If there are issues with retrieving values from the YAML dictionary or creating the dataset.
"""
    response = Response()


    params = ["datasetname","datasettypeid", "datasettype"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.datasets")

    query = """SELECT datasettypeid 
            FROM ndb.datasettypes 
            WHERE LOWER(datasettype) = %(ds_type)s"""
    if isinstance(inputs.get('datasettype'), str) and not(inputs.get('datasettypeid')):
        cur.execute(query, {"ds_type": f"{inputs.get('datasettype').lower()}"})
        datasettypeid = cur.fetchone()
        del inputs['datasettype']
    else:
        datasettypeid = None

    if datasettypeid:
        inputs["datasettypeid"] = datasettypeid[0]
        response.valid.append(True)
    else:
        inputs["datasettypeid"] = None
        response.message.append(f"✗ Dataset type is not known to Neotoma and needs to be created first")
        response.valid.append(False)
    inputs["notes"] = nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets", name).get('notes', "")
    try:
        Dataset(**inputs)
        response.message.append(f"✔ Dataset can be created.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Dataset cannot be created: {e}")
        response.valid.append(False)
    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response