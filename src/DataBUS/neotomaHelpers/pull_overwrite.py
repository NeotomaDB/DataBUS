import re
from .retrieve_dict import retrieve_dict


def pull_overwrite(params, yml_dict, table=None):
    """Pull parameters overwrite value from YAML template.

    Extracts the overwrite flag for each parameter from the YAML template.
    Handles both single table strings and multiple tables (list). For 'geog'
    parameters, duplicates the overwrite value to coordinate fields.

    Examples:
        >>> params = ['siteid', 'sitename']
        >>> yml = {'metadata': [{'neotoma': 'sites.siteid', 'overwrite': True}]}
        >>> pull_overwrite(params, yml, 'sites')
        {'siteid': True, 'sitename': False}

    Args:
        params (list): A list of strings for the columns needed to generate the insert statement.
        yml_dict (dict): A dict returned by the YAML template.
        table (str or list, optional): The name of the table(s) the parameters are being drawn for.
                                      If a list, returns results for each table.

    Returns:
        dict or list: Parameters with overwrite T/F value. If table is a list, returns
                      list of dicts, one per table. If table is a str, returns single dict.
    """
    results = []
    if isinstance(table, str):
        if re.match(".*\.$", table) == None:
            table = table + "."
        result = dict()
        for i in params:
            valor = retrieve_dict(yml_dict, table + i)
            if len(valor) == 1:
                result[i] = valor[0]["overwrite"]
            else:
                result[i] = False
        if "geog" in params:
            result["coordlo"] = result["geog"]
            result["coordla"] = result["geog"]
            result["ns"] = result["geog"]
            result["ew"] = result["geog"]
            # SUGGESTION: Extract geographic coordinate mapping to a separate function for reusability
        return result

    elif isinstance(table, list):
        for item in table:
            results.append(pull_overwrite(params, yml_dict, item))
        return results
