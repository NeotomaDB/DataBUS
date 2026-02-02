import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response
from DataBUS.Dataset import DATASET_PARAMS

def valid_dataset(cur, yml_dict, csv_file):
    """Validates a dataset based on YAML configuration and CSV data.

    Validates dataset name and dataset type against the Neotoma database.
    Attempts to resolve dataset type by querying the database if not provided.
    Creates a Dataset object with validated parameters.

    Args:
        cur (cursor): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Raises:
        Exception: If there are issues retrieving values from YAML dict or creating the dataset.
    
    Examples:
        >>> valid_dataset(cursor, config_dict, "data.csv", name="MyDataset")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    params = DATASET_PARAMS
    inputs = {}
    for param in params:
        val = nh.retrieve_dict(yml_dict, param[1])
        if val:
            try:
                inputs[param[0]] = val[0]['value']
                response.message.append(f"")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ {param[0]} value is missing in template: {e}")
        else:
            inputs[param[0]] = None

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
    inputs["notes"] = nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets").get('notes', "")
    try:
        Dataset(**inputs)
        response.message.append(f"✔ Dataset can be created.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Dataset cannot be created: {e}")
        response.valid.append(False)
    response.message = list(set(response.message))
    return response