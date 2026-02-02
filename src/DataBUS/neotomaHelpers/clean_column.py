def clean_column(column, template, clean=True):
    """Extract and clean a column from template data.

    Extracts a single column from template data and optionally reduces it to unique
    values. Handles special cases where there are multiple non-empty values by raising
    an error, unless one value is empty/None.

    Examples:
        >>> template = [{'sampletype': 'Pollen'}, {'sampletype': 'Pollen'}]
        >>> clean_column('sampletype', template, clean=True)
        'Pollen'
        >>> template = [{'sitename': 'Mirror Lake'}, {'sitename': 'Mirror Lake'}]
        >>> clean_column('sitename', template, clean=True)
        'Mirror Lake'

    Args:
        column (str): The name of the column to extract.
        template (list): The CSV file as a list of dictionaries.
        clean (bool, optional): If True, reduces to unique values. If False, returns all values.
                               Defaults to True.

    Returns:
        str, list, or None: Single value if all values are identical, list of values if clean=False,
                           or None if column is empty.

    Raises:
        ValueError: If clean=True and there are multiple different non-empty values in the column.
    """
    # SUGGESTION: Refactor to reduce lambda usage and improve readability
    if clean:
        setlist = list(set(map(lambda x: x[column] if isinstance(x[column], str) else x[column], template)))
        clean_list = list(set(map(lambda x: x[column].lower() if isinstance(x[column], str) else x[column], template)))
        if len(clean_list) == 1:
            setlist = setlist[0]
        elif len(setlist) == 0:
            setlist = None
        elif len(setlist) == 2:
            if any(value in ['', None] for value in setlist) and any(value is not None for value in setlist):
                setlist = next(value for value in setlist if value not in ['', None])
            else:
                raise ValueError(f"There are multiple values in a not rowwiseelement {column}."
                                 " Correct the template or the data.")
        else:
            raise ValueError(f"There are multiple values in a not rowwiseelement {column}."
                             " Correct the template or the data.")
    else:
        setlist = list(map(lambda x: x[column], template))
        if not setlist:
            setlist = None

    return setlist