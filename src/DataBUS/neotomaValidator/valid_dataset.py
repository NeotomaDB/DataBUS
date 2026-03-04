import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response
from DataBUS.Dataset import DATASET_PARAMS

def valid_dataset(cur, yml_dict, csv_file, databus=None):
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

    Examples:
        >>> valid_dataset(cursor, config_dict, "data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    inputs = {}
    for param in DATASET_PARAMS:
        val = nh.retrieve_dict(yml_dict, param[1])
        if val:
            try:
                inputs[param[0]] = val[0]['value']
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ {param[0]} value is missing in template: {e}")
        else:
            inputs[param[0]] = None
    query = """SELECT datasettypeid
               FROM ndb.datasettypes
               WHERE LOWER(datasettype) = %(ds_type)s"""
    if isinstance(inputs.get('datasettypeid'), str):
        cur.execute(query, {"ds_type": inputs.get("datasettypeid").lower().strip()})
        datasettypeid = cur.fetchone()
        if datasettypeid:
            inputs["datasettypeid"] = datasettypeid[0]
            response.valid.append(True)
        else:
            inputs["datasettypeid"] = None
            response.message.append("✗ Dataset type is not known to Neotoma and needs to be created first.")
            response.valid.append(False)
    else:
        datasettypeid = inputs.get('datasettypeid')
        ch = """SELECT datasettypeid, datasettype
               FROM ndb.datasettypes
               WHERE datasettypeid = %(ds_typeid)s"""
        cur.execute(ch, {"ds_typeid": datasettypeid})
        datasettypeid = cur.fetchone()
        if datasettypeid:
            inputs["datasettypeid"] = datasettypeid[0]
            response.message.append(f"✔ Dataset type ID {datasettypeid[0]} corresponds to dataset type '{datasettypeid[1]}'.")
            response.valid.append(True)
        else:
            inputs["datasettypeid"] = None
            response.message.append("✗ Dataset type is not known to Neotoma and needs to be created first.")
            response.valid.append(False)
    inputs["notes"] = nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets").get('notes')
    if databus is not None:
        inputs["collectionunitid"] = databus['collunits'].id_int
    else:
        inputs["collectionunitid"] = 1  # placeholder
        response.valid.append(False)
        response.message.append("✗ Collection unit ID not available; using placeholder.")
    try:
        ds = Dataset(**inputs)
        response.message.append("✔ Dataset can be created.")
        response.valid.append(True)
        if databus is not None:
            try:
                response.id_int = ds.insert_to_db(cur)
                response.message.append(f"✔ Dataset inserted with ID {response.id_int}.")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Failed to insert dataset: {e}")
    except Exception as e:
        response.message.append(f"✗ Dataset cannot be created: {e}")
        response.valid.append(False)
    return response