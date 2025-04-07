def clean_column(column, template, clean=True):
    """_cleanCol_ 

    Args:
        column (_list_): _The name of the column to use_
        template (_list_): _The CSV file as a list of dictionaries_
        clean (bool, optional): _Does the column get reduced to only the unique values?_. Defaults to True.

    Returns:
        _list_: _The cleaned column._
    """
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