import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Datum, Variable
#import traceback
 
def insert_data(cur, yml_dict, csv_file, uploader, wide = False):
    """
    Inserts data into the database based on the provided YAML dictionary and CSV file.
    Parameters:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to the CSV file containing data to be inserted.
        uploader (dict): Dictionary containing uploader information.
    Returns:
        Response: An object containing the status of the insertion process, including messages and validity of each insertion.
    """
    inputs = nh.pull_params(["value"], yml_dict, csv_file, "ndb.data", values = False)
    response = Response()
    if 'value' in inputs:
        if not inputs['value']:
            response.message.append("? No Values to validate.")
            response.validAll = False
            return response
        
    response = Response()

    var_query = """SELECT variableelementid FROM ndb.variableelements
                    WHERE LOWER(variableelement) = %(element)s;"""
    taxon_query = """SELECT taxonid FROM ndb.taxa 
                     WHERE LOWER(taxonname) = %(element)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits 
                     WHERE LOWER(variableunits) = %(element)s;"""
    context_query = """SELECT LOWER(variablecontext) FROM ndb.variablecontexts
                       WHERE LOWER(variablecontext) = %(element)s;"""

    par = {'variableelement': [var_query, 'variableelementid'], 
           'taxon': [taxon_query, 'taxonid'], 
           'variableunits': [units_query, 'variableunitsid'],
           'variablecontext': [context_query, 'variablecontextid']}

    if wide == True:
        taxa = inputs.copy()
    else:
        taxa_in = nh.pull_params(['taxon', 'variableunits',
                                  'variableelement', 'variablecontext'], 
                                  yml_dict, csv_file, "ndb.variables", 
                                  values=wide)
        taxa_in['value'] = inputs['value']
        
        taxa = {}   
        for i, taxon in enumerate(taxa_in['taxon']):
            if taxon not in taxa:
                taxa[taxon] = {'value': [],
                                'variablecontext': [],
                                'variableelement': [], 
                                'variableunits': []}
            for v in ['value', 'variablecontext', 'variableelement', 'variableunits']:
                if isinstance(taxa_in[v], list):
                    taxa[taxon][f'{v}'].append(taxa_in[v][i])
                else:
                    taxa[taxon][f'{v}'].append(taxa_in[v])
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
                if isinstance(inputs2[k], list) and inputs2[k]:
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
                    entries[v[1]] = cur.fetchall()
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
        if varid:
            varid = varid[0]
            response.valid.append(True)
        else:
            varunits = inputs2['variableunits'][0] if isinstance(inputs2.get('variableunits'), list) else inputs2.get('variableunits')
            varcontextid = inputs2['variablecontext'][0] if isinstance(inputs2.get('variablecontext'), list) else inputs2.get('variablecontext')
            varelement = inputs2['variableelement'][0] if isinstance(inputs2.get('variableelement'), list) else inputs2.get('variableelement')
            response.message.append(f"? Var ID not found for: \n "
                                    f"variableunitsid: {varunits}, ID: {entries['variableunitsid']},\n"
                                    f"taxon: {key.lower()}, ID: {entries['taxonid']},\n"
                                    f"variableelement: {varelement}, ID: {entries['variableelementid']},\n"
                                    f"variablecontextid: {varcontextid}, ID: {entries['variablecontextid']}\n")
            response.valid.append(True)
            varid = var.insert_to_db(cur)
        for i, j in enumerate(taxa[key]['value']):
            if key not in response.data_id:
                response.data_id[key]=[]
            try:
                if len(uploader['samples'].sampleid)>1:
                    sampleid = uploader['samples'].sampleid[i]
                else:
                    sampleid = uploader['samples'].sampleid[0]
                datum = Datum(sampleid=sampleid, 
                              variableid=varid, 
                              value=j)
                d_id = datum.insert_to_db(cur)
                response.valid.append(True)
                response.data_id[key].append(d_id)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Datum cannot be inserted: {e}")
                d_id = 2
                cur.execute("ROLLBACK;") # exit error status
                continue

    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔  Data inserted.")
    response.message = list(set(response.message))
    return response