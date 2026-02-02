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

    query = """SELECT databaseid from ndb.constituentdatabases
                WHERE LOWER(databasename) LIKE %(databasename)s"""
    cur.execute(query, {"databasename": f"%{inputs['databasename'].lower()}%"})
    inputs["databaseid"] = cur.fetchone()
    if inputs["databaseid"]:
        inputs["databaseid"] = inputs["databaseid"][0]
    try:
        DatasetDatabase(databaseid=int(inputs["databaseid"]))
        response.valid.append(True)
        response.message.append(f"✔ Database ID {inputs['databaseid']} " f"created.")
    except Exception as e:
        response.message.append(f"✗ Cannot create Database object: {e}")
        response.valid.append(False)
    response.id.append(inputs["databaseid"])
    response.message = list(set(response.message)) 
    return response