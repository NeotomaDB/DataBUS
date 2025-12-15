import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Variable, Datum

def valid_data(cur, yml_dict, csv_file, wide = False):
    """
    Validates data from a CSV file against a YAML dictionary and a database.
    Parameters:
    cur (psycopg2.cursor): Database cursor for executing SQL queries.
    yml_dict (dict): Dictionary containing YAML configuration data.
    csv_file (str): Path to the CSV file containing data to be validated.
    validator (Validator): Validator object for additional validation logic.
    Returns:
    Response: A response object containing validation results and messages.
    """
    inputs = nh.pull_params(["value"], yml_dict, csv_file, "ndb.data", values = False)
    response = Response()
    if 'value' in inputs: 
        if not inputs['value']:
            response.message.append("? No Values to validate.")
            response.validAll = False
            return response
    var_query = """SELECT variableelementid FROM ndb.variableelements
                    WHERE LOWER(variableelement) = %(element)s;"""
    taxon_query = """SELECT * FROM ndb.taxa 
                     WHERE LOWER(taxonname) = %(element)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits 
                     WHERE LOWER(variableunits) = %(element)s;"""
    context_query = """SELECT variablecontextid FROM ndb.variablecontexts
                       WHERE LOWER(variablecontext) = %(element)s;"""

    par = {'taxon': [taxon_query, 'taxonid'],
           'variableelement': [var_query, 'variableelementid'], 
           'variableunits': [units_query, 'variableunitsid'],
           'variablecontext': [context_query, 'variablecontextid']}
    if wide == True:
        taxa = inputs.copy()
    else:
        taxa_in = nh.pull_params(['taxon', 'variableunits',
                                  'variableelement', 'variablecontext'], 
                                  yml_dict, csv_file, "ndb.variables", 
                                  values=wide)
        values = inputs['value']
        taxa_d = taxa_in.get('taxon', None)    
        variables = {"variableunits": taxa_in.get('variableunits', None),
                     "variableelement": taxa_in.get('variableelement', None),
                     "variablecontext": taxa_in.get('variablecontext', None)}
        taxa = {}    
        for i, (taxon, value) in enumerate(zip(taxa_d, values)):
            if taxon not in taxa:
                taxa[taxon] = {'value': [],
                               'variablecontext': [],
                               'variableelement': [], 
                               'variableunits': []}
            taxa[taxon]['value'].append(int(value))
            for n, p in variables.items():
                if isinstance(p, list):
                    taxa[taxon][f'{n}'].append(p[i])
                else:
                    taxa[taxon][f'{n}'].append(p)
    for key in taxa.keys():
        if wide == True:
            params = [v for k, v in taxa[key].items() if k != 'value']
            inputs2 = nh.pull_params(params, yml_dict, csv_file, "ndb.variables", values=wide)
            inputs2['taxon'] = key
        else:
            inputs2 = taxa[key]
            inputs2['taxon'] = key
        entries = {}
        counter = 0
        for k,v in par.items():
            if k in inputs2:
                if isinstance(inputs2[k], list):
                    if inputs2[k][0]:
                        inputs2[k][0] = inputs2[k][0].lower()
                    cur.execute(v[0], {'element': inputs2[k][0]})
                    entries[v[1]] = cur.fetchone()
                    if not entries[v[1]]:
                        counter +=1
                        if v[1] == 'taxonid':
                           response.valid.append(False)
                           response.message.append(f"✗  {k} ID for {inputs2[k][0]} not found. "
                                                f"Does it exist in Neotoma?")
                        else:
                           response.message.append(f"?  {k} not found. "
                                                   f"Does it exist in Neotoma?")
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
                    entries[v[1]] = None
                    response.message.append(f"?  {inputs2[k]} ID not given. ")
                    response.valid.append(True)
            else:
                response.message.append(f"?  {k} ID not given. ")
                response.valid.append(True)
                entries[v[1]] = counter
        var = Variable(**entries)
        response.valid.append(True)
        try:
            varid = var.get_id_from_db(cur)
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Var ID cannot be retrieved from db: {e}")
            varid = None
        if varid:
            varid = varid[0]
            response.valid.append(True)
        else:
            varunits = inputs2['variableunits'][0] if isinstance(inputs2.get('variableunits', None), list) else inputs2.get('variableunits')
            varcontextid = inputs2['variablecontext'][0] if isinstance(inputs2.get('variablecontext', None), list) else inputs2.get('variablecontext')
            varelement = inputs2['variableelement'][0] if isinstance(inputs2.get('variableelement', None), list) else inputs2.get('variableelement')
            response.message.append(f"? Var ID not found for: \n "
                                    f"variableunitsid: {varunits}, ID: {entries['variableunitsid']},\n"
                                    f"taxon: {key.lower()}, ID: {entries['taxonid']},\n"
                                    f"variableelement: {varelement}, ID: {entries['variableelementid']},\n"
                                    f"variablecontextid: {varcontextid}, ID: {entries['variablecontextid']}\n")
            response.valid.append(True)
        for i in taxa[key]['value']:
            try:
                Datum(sampleid=int(3), variableid=varid, value=i)
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Datum cannot be created: {e}")
            
    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    if response.validAll:
        response.message.append(f"✔  Datum can be created.")
    return response 