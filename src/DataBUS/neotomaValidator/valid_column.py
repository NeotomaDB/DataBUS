import datetime

def is_valid_date(value):
    """Check if a value is a valid date in YYYY-MM-DD format.

    Args:
        value (str): String value to validate as date.

    Returns:
        bool: True if valid date format, False otherwise.
    """
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_numeric(value):
    """Check if a value can be cast as an integer or float.

    Attempts conversion to int first, then float if int fails.

    Args:
        value (str or number): Value to check for numeric type.

    Returns:
        bool: True if value is numeric, False otherwise.
    """
    try:
        int(value)
        return True
    except ValueError:
        try:
            float(value)
            return True
        except ValueError:
            return False


def valid_column(pointer):
    """Validates column values against expected data types.

    Checks that all values in a column conform to the specified type
    (string, number, or date). Returns empty string if validation passes,
    error message if validation fails.

    Examples:
        >>> valid_column({'column': 'age', 'type': 'number', 'values': ['10', '20']})
        ''

    Args:
        pointer (dict): Dictionary with 'column' name, 'type', and 'values' list.

    Returns:
        str or list: Empty string if valid, error message string if invalid.
    """
    response = {"message": []}
    allowed_types = {"string": str, "number": is_numeric, "date": is_valid_date}
    value_type = pointer.get("type")
    values_list = pointer.get("values")
    if callable(allowed_types[value_type]):
        # If the type is a date check, call the function for each value
        result = all(allowed_types[value_type](value) for value in values_list)

    else:
        # If the type is a standard Python type, perform the isinstance check
        result = all(
            isinstance(value, allowed_types[value_type]) for value in values_list
        )
    if result is False:
        response["message"].append(f'✗ {pointer["column"]} is not properly formatted.')
        response["message"] = "".join(response["message"])
    return response["message"]