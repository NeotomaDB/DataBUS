from DataBUS import Dataset, Response

def valid_geochron_dataset(cur, yml_dict, csv_file):
    """Validates geochronological dataset for database insertion.

    Creates and validates a Dataset object with the geochronological dataset type ID.
    Used specifically for geochronological data that requires special dataset type handling.

    Examples:
        >>> valid_geochron_dataset(cursor, config_dict, "geochron_data.csv")
        Response(valid=[True], message=[...], validAll=True)

    Args:
        cur (cursor): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing geochronological data.

    Returns:
        Response: Response object containing validation messages and overall validity status.

    Raises:
        Exception: If there are issues retrieving values from YAML dict or creating the dataset.
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

    response.message = list(set(response.message))
    return response