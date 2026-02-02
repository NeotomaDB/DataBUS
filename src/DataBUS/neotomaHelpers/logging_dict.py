from DataBUS import Response

def logging_response(response, logfile):
    """Append Response object string representation to logfile.

    Validates that response is a Response object, then appends its string representation
    to the logfile.

    Examples:
        >>> logfile = []
        >>> logging_response(pollen_response, logfile)  # doctest: +SKIP
        [<string representation of pollen_response>]
        >>> logfile = []
        >>> logging_response(chronology_response, logfile)  # doctest: +SKIP
        [<string representation of chronology_response>]

    Args:
        response (Response): Response object from DataBUS module.
        logfile (list): List to append the response to.

    Returns:
        list: The updated logfile list with response appended.

    Raises:
        AssertionError: If response is not an instance of Response class.
    """
    assert isinstance(response, Response), "response needs to be a Response"
    logfile.append(f"{response}")
    return logfile
