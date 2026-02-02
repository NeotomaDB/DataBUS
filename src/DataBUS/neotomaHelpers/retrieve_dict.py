import warnings
import logging
import re

def retrieve_dict(yml_dict, sql_column):
    """Get Dictionary for a Neotoma column using the YAML template.

    Searches through YAML metadata for entries matching a specific SQL column using
    regex pattern matching with word boundaries.

    Examples:
        >>> yml = {'metadata': [{'neotoma': 'sites.siteid', 'type': 'int'}, {'neotoma': 'sites.sitename', 'type': 'str'}]}
        >>> retrieve_dict(yml, 'sites.siteid')
        [{'neotoma': 'sites.siteid', 'type': 'int'}]
        >>> retrieve_dict(yml, 'sites.sitename')
        [{'neotoma': 'sites.sitename', 'type': 'str'}]

    Args:
        yml_dict (dict): The YAML template object imported by the user containing 'metadata' key.
        sql_column (str): A character string indicating the SQL column to be matched.

    Returns:
        list: A list of all dictionaries associated with a particular Neotoma table/column.
    """
    # result = next((d['column'] for d in yml_dict if d['neotoma'] == sqlColumn), None)
    # retrieving the dict instead:
    try:
        assert isinstance(yml_dict, dict)
        assert yml_dict.get("metadata")
    except AssertionError:
        logging.error(
            "The yml_dict must be a dict object (not a list) containing the key 'metadata'.",
            exc_info=True,
        )
    result = [d for d in yml_dict["metadata"] if re.search(fr'\b{sql_column}\b', d["neotoma"])]
    if result is None:
        warnings.warn("No matching dictionary entry found.")
        return None
    else:
        return result