from DataBUS import Response

def valid_geochroncontrol(validator):
    """Validates geochronological control data consistency.

    Verifies that both chroncontrols and geochron validation results are present
    and successful. Acts as a prerequisite check before geochronological data insertion.
    Note: Elements are obtained from uploader; no inserts occur during validation.

    Args:
        validator (dict): Dictionary containing 'chron_controls' and 'geochron' Response objects.

    Returns:
        Response: Response object containing validation messages and overall status.

    Raises:
        AssertionError: If required keys are missing or validation failed.

    Examples:
        >>> valid_geochroncontrol({'chron_controls': response1, 'geochron': response2})
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        assert 'chron_controls' in validator, "Validator dictionary must contain 'chron_controls' key"
        assert 'geochron' in validator, "Validator dictionary must contain 'geochron' key"
        assert validator['chron_controls'].validAll is True, "Chroncontrols were not successful in validation."
        assert validator['geochron'].validAll is True, "Geochron was not successful in validation."
        response.valid.append(True)
        response.message.append("✔  Geochron Control and Chroncontrols are present in validator.")
    except AssertionError as e:
        response.valid.append(False)
        response.message.append(f"✘ {str(e)}")
        response.valid.append(False)
    response.message = list(set(response.message))
    return response