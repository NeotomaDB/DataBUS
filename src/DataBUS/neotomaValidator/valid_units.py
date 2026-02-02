def valid_units(cur, yml_dict, df):
    """Validates DataFrame column units against YAML-specified vocabularies.

    Validates units in DataFrame columns by comparing against controlled
    vocabularies defined in YAML metadata. Supports both fixed (single unique
    value) and list-based vocabulary constraints.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing metadata and configuration from YAML file.
        df (pandas.DataFrame): DataFrame containing data to be validated.

    Returns:
        dict: Dictionary with 'valid' (bool) and 'message' (list of strings).
    
    Examples:
        >>> valid_units(cursor, config_dict, dataframe)
        {'valid': True, 'message': ['✔ Column age contains valid units.', ...]}
    """
    response = {"valid": list(), "message": list()}

    # Extract entries from the yml_dict that contain a vocab
    yml_dict = yml_dict.get("metadata", [None])
    vocab_entries = [
        entry for entry in yml_dict if "vocab" in entry and (entry["vocab"] is not None)
    ]
    for entry in vocab_entries:
        column_values = df[entry["column"]].tolist()
        if entry["vocab"] == ["fixed"]:
            if all(elem.lower() == column_values[0].lower() for elem in column_values):
                response["valid"].append(True)
                response["message"].append(
                    f"✔ Column {entry['column']} contains valid units."
                )
            else:
                response["valid"].append(False)
                response["message"].append(
                    f"✗ Column {entry['column']} should be unique. Multiple values found"
                )
        else:
            if all(value in entry["vocab"] for value in column_values):
                response["valid"].append(True)
                response["message"].append(
                    f"✔ Column {entry['column']} contains valid units."
                )
            else:
                response["valid"].append(False)
                response["message"].append(
                    f"✗ Column {entry['column']} contains invalid units."
                )
    response["valid"] = all(response["valid"])
    return response