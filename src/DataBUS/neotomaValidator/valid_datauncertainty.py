import DataBUS.neotomaHelpers as nh
from DataBUS import Response, DataUncertainty
from DataBUS.DataUncertainty import DATAUNCERTAINTY_PARAMS

def valid_datauncertainty(cur, yml_dict, csv_file):
    """Validates data uncertainty values against the Neotoma database.

    Validates uncertainty values, units, and basis information. Queries database
    for valid uncertainty basis IDs and variable unit IDs, then creates DataUncertainty
    objects with validated parameters. Supports both long and wide data formats.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration parameters.
        csv_file (str): Path to CSV file containing data to validate.
        wide (bool, optional): Flag for wide format taxa handling. Defaults to False.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_datauncertainty(cursor, config_dict, "uncertainty_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()

    try:
        inputs = nh.pull_params(DATAUNCERTAINTY_PARAMS, yml_dict, csv_file, "ndb.datauncertainties")
        if all(v is None for v in inputs.values()):
            response.message.append("? No Uncertainty Values to validate.")
            response.valid.append(True)
            return response
    except Exception as e:
        response.message.append(f"✗  Error pulling parameters for data uncertainty validation: {e}")
        response.valid.append(False)
        return response

    basis_query = """SELECT uncertaintybasisid FROM ndb.uncertaintybases
                     WHERE LOWER(uncertaintybasis) = %(uncertaintybasisid)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits
                     WHERE LOWER(variableunits) = %(uncertaintyunitid)s;"""
    par = {'uncertaintybasisid': [basis_query, 'uncertaintybasisid'],
           'uncertaintyunitid': [units_query, 'uncertaintyunitid']}
    
    vals = {}
    for taxon in inputs:
        if inputs[taxon].get('uncertaintyvalue') is None:
            continue
        inputs[taxon]['uncertaintybasisid'] = [inputs[taxon].get('uncertaintybasisid')] * len(inputs[taxon].get('uncertaintyvalue'))
        for datum in zip(*inputs.get(taxon).values()):
            datum = dict(zip(list(inputs[taxon].keys()), datum))
            if datum.get('uncertaintyvalue') is None:
                continue
            for param, (query, key) in par.items():
                if isinstance(datum.get(param), str):
                    if datum[param].lower().strip() in vals:
                        datum[param] = vals[datum[param].lower().strip()]
                    else:
                        cur.execute(query, {key: datum[param].lower().strip()})
                        result = cur.fetchone()
                        if result:
                            name = datum[param]
                            vals[datum[param].lower().strip()] = result[0]
                            datum[param] = result[0]
                            if f"✔ The provided {param} is correct: {result[0]}" not in response.message:
                                response.message.append(f"✔ The provided {param} ({name}) is correct: {result[0]}")
                            response.valid.append(True)
                        else:
                            if f"✗ The provided {param} with value {datum[param]} does not exist in Neotoma DB." not in response.message:
                                response.message.append(f"✗ The provided {param} with value {datum[param]} does not exist in Neotoma DB.")
                            response.valid.append(False)
            try:
                DataUncertainty(dataid = 3,  # Placeholder required for validation
                                uncertaintyvalue = datum.get('uncertaintyvalue'),
                                uncertaintyunitid = datum.get('variableunitsid'),
                                uncertaintybasisid = datum.get('uncertaintybasisid'),
                                notes = datum.get('notes'))
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                if f"✗  Datum Uncertainty cannot be created: {e}" not in response.message:
                    response.message.append(f"✗  Datum Uncertainty cannot be created: {e}")
    if response.validAll:
        response.message.append("✔  Datum Uncertainty can be created.")
    return response