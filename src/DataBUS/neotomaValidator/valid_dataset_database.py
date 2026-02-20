import DataBUS.neotomaHelpers as nh
from DataBUS import Response, DatasetDatabase

def valid_dataset_database(cur, yml_dict):
    """Validates dataset-database associations.

    Validates the database name provided in YAML configuration against the
    Neotoma database's constituent databases. Creates a DatasetDatabase object
    with the validated database ID.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.

    Returns:
        Response: Response object with validation results, messages, and database ID.

    Examples:
        >>> valid_dataset_database(cursor, config_dict)
        Response(valid=[True], message=[...], validAll=True, id=1)
    """
    response = Response()
    db_name = nh.retrieve_dict(yml_dict, "ndb.datasetdatabases.databasename")
    inputs = {"databasename": db_name[0]["value"]}

    db_query = """SELECT databaseid FROM ndb.constituentdatabases
               WHERE LOWER(databasename) LIKE %(databasename)s"""
    if isinstance(inputs["databasename"], str):
        cur.execute(db_query, {"databasename": inputs["databasename"].lower().strip()})
        inputs["databaseid"] = cur.fetchone()
        if inputs["databaseid"]:
            inputs["databaseid"] = inputs["databaseid"][0]
        else:
            response.valid.append(False)
            response.message.append(f"✗ Database '{inputs['databasename']}' not found in Neotoma.")
            return response
    try:
        DatasetDatabase(databaseid = inputs["databaseid"], datasetid = 1) # placeholder for datasetid
        response.valid.append(True)
        response.message.append(f"✔ Dataset linked to Database ID {inputs['databaseid']} created.")
    except Exception as e:
        response.message.append(f"✗ Cannot create Database object: {e}")
        response.valid.append(False)
    return response