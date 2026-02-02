import re
from .retrieve_dict import retrieve_dict


def pull_required(params, yml_dict, table=None):
    """Pull parameters required value from YAML template.

    Extracts the required flag for each parameter from the YAML template.
    Handles both single table strings and multiple tables (list).

    Examples:
        >>> params = ['siteid', 'sitename', 'altitude']
        >>> yml = {'metadata': [{'neotoma': 'sites.siteid', 'required': True}]}
        >>> pull_required(params, yml, 'sites')
        {'siteid': True, 'sitename': False, 'altitude': False}
        >>> params = ['depth', 'age', 'error']
        >>> yml = {'metadata': [{'neotoma': 'chroncontrols.depth', 'required': True}]}
        >>> pull_required(params, yml, 'chroncontrols')
        {'depth': True, 'age': False, 'error': False}

    Args:
        params (list): A list of strings for the columns needed to generate the insert statement.
        yml_dict (dict): A dict returned by the YAML template.
        table (str or list, optional): The name of the table(s) the parameters are being drawn for.
                                      If a list, returns results for each table.

    Returns:
        dict or list: Parameters with required T/F value. If table is a list, returns
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
                result[i] = valor[0]["required"]
            else:
                result[i] = False
        return result

    elif isinstance(table, list):
        for item in table:
            results.append(pull_required(params, yml_dict, item))
        return results
