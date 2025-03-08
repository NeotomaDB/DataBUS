import DataBUS.neotomaHelpers as nh
from DataBUS import Response, DataUncertainty

def valid_datauncertainty(cur, yml_dict, csv_file, wide=False):
    """
    Validates data uncertainty values and their associated units and basis.

    Parameters:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration parameters.
        csv_file (str): Path to the CSV file containing data to be validated.
        wide (bool, optional): Flag indicating whether to use wide format for taxa. Defaults to False.
    Returns:
        Response: An object containing validation results, including messages and validity status.
    """ 
    inputs = nh.pull_params(["uncertaintyvalue"], yml_dict, csv_file, "ndb.datauncertainties")
    response = Response()
    if 'value' in inputs:
        if not inputs['value']:
            response.message.append("? No Values to validate.")
            response.validAll = False
            return response
    basis_query = """SELECT uncertaintybasisid FROM ndb.uncertaintybases
                    WHERE LOWER(uncertaintybasis) = %(element)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits 
                     WHERE LOWER(variableunits) = %(element)s;"""

    par = {'uncertaintybasis': [basis_query, 'uncertaintybasisid'], 
           'variableunits': [units_query, 'variableunitsid']}
    if wide == True:
        taxa = inputs.copy()
    else:
        taxa = {'value': inputs['value']}

    for n, key in enumerate(taxa.keys()): #variables.variableunits
        params = [v for k, v in taxa[key].items() if k not in ['value', 'uncertaintybasis']]
        inputs2 = nh.pull_params(params, yml_dict, csv_file, "ndb.variables", values=True)
        if 'uncertaintybasis' in taxa[key]:
            inputs2['uncertaintybasis'] = taxa[key]['uncertaintybasis']
        inputs2['taxon'] = key
        entries = {}
        counter = 0
        for k,v in par.items():
            key_ = f"{key}_{k}"
            if k in inputs2:
                if isinstance(inputs2[k], list):
                    cur.execute(v[0], {'element': inputs2[k][i].lower()})
                    entries[v[1]] = cur.fetchone()
                    if not entries[v[1]]:
                        counter +=1
                        response.message.append(f"✗  {k} ID for {key} not found. "
                                                f"Does it exist in Neotoma?")
                        response.valid.append(False)
                        entries[v[1]] = None
                    else:
                        entries[v[1]] = entries[v[1]][0]
                elif isinstance(inputs2[k], str):
                    cur.execute(v[0], {'element': inputs2[k].lower()})
                    entries[v[1]] = cur.fetchone()
                    if not entries[v[1]]:
                        counter +=1
                        response.message.append(f"✗  {k} ID for {inputs2[k]} not found. "
                                                f"Does it exist in Neotoma?")
                        response.valid.append(False)
                        entries[v[1]] = None 
                    else:
                        entries[v[1]] = entries[v[1]][0]
                else:
                    inputs[key_] = None
                    entries[v[1]] = None
                    response.message.append(f"?  {key} {k} ID not given. ")
                    response.valid.append(True)
            else:
                    response.message.append(f"?  {key} {k} ID not given. ")
                    response.valid.append(True)
                    entries[v[1]] = counter
        for i in taxa[key]['value']:
            try:
                DataUncertainty(dataid = 3, # Placeholder required for the validation.
                                uncertaintyvalue= i, 
                                uncertaintyunitid=entries['variableunitsid'], 
                                uncertaintybasisid=entries['uncertaintybasisid'],
                                notes=None)
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Datum Uncertainty cannot be created: {e}")
    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    if response.validAll:
        response.message.append(f"✔  Datum Uncertainty can be created.")
    return response