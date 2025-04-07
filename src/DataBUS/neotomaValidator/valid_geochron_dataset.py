import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response

def valid_geochron_dataset(cur, yml_dict, csv_file, name=None):
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

    inputs = {}
    inputs['datasettypeid'] = 1

    try:
        Dataset(**inputs)
        response.message.append(f"✔ Geochronology Dataset can be created.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Geochronology Dataset cannot be created: {e}")
        response.valid.append(False)

    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response