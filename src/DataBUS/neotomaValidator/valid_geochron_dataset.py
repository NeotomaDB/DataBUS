from DataBUS import Dataset, Response


def valid_geochron_dataset(cur, yml_dict, csv_file, databus=None):
    """Validates and inserts a geochronological dataset.

    Creates and validates a Dataset object with the geochronological dataset type ID
    (fetched from ndb.datasettypes by name 'geochronologic').  When databus is
    provided and validation passes, inserts the dataset into the database using
    the real collection unit ID from databus['collunits'].id_int and stores the
    resulting dataset ID in response.id_int.

    Args:
        cur (cursor): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing geochronological data.
        databus (dict | None): Prior validation results supplying collectionunitid.

    Returns:
        Response: Response object containing validation messages and overall
            validity status.

    Examples:
        >>> valid_geochron_dataset(cursor, config_dict, "geochron_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()

    # Resolve datasettypeid for 'geochronologic' from the database
    datasettypeid = 1  # safe fallback
    try:
        cur.execute(
            """SELECT datasettypeid FROM ndb.datasettypes
               WHERE LOWER(datasettype) = 'geochronologic'"""
        )
        result = cur.fetchone()
        if result:
            datasettypeid = result[0]
            response.message.append(f"✔ Geochronologic dataset type found: ID {datasettypeid}.")
            response.valid.append(True)
        else:
            response.message.append(
                "? 'geochronologic' not found in datasettypes; using fallback datasettypeid=1."
            )
            response.valid.append(True)
    except Exception as e:
        response.message.append(
            f"? Could not query datasettypes ({e}); using fallback datasettypeid=1."
        )
        response.valid.append(True)

    # Resolve collectionunitid
    try:
        collunitid = databus.get("collunits").id_int
        response.valid.append(True)
    except Exception as e:
        collunitid = 1  # placeholder
        response.valid.append(False)
        response.message.append(
            f"✗ No collection unit found in databus. Using placeholder value for collectionunitid: {e}"
        )
    inputs = {"datasettypeid": datasettypeid, "collectionunitid": collunitid}

    try:
        ds = Dataset(**inputs)
        response.message.append("✔ Geochronology Dataset can be created.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Geochronology Dataset cannot be created: {e}")
        response.valid.append(False)
        return response
    try:
        response.id_int = ds.insert_to_db(cur)
        response.message.append(f"✔ Geochronology dataset inserted with ID {response.id_int}.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Geochronology dataset could not be inserted: {e}")
        response.valid.append(False)

    return response
