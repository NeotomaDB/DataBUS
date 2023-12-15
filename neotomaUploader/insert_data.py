import logging
from .pull_params import pull_params

def insert_data(cur, cur_p, yml_dict, csv_template, uploader):
    results_dict = {'data_points': [], 'valid': []}
    data_query = """
                 SELECT ts.insertdata(_sampleid := %(sampleid)s,
                                      _variableid := %(variableid)s,
                                      _value := %(value)s)
                 """
    params = ['value']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.data')

    get_varid = """
                 SELECT * FROM ndb.variables 
                           WHERE variableunitsid = %(unitsid)s 
                           AND taxonid = %(taxonid)s
                           AND variableelementid = %(variableelementid)s
                           AND variablecontextid = %(variablecontextid)s
                """
    
    var_query = """
                 SELECT ts.insertvariable(_taxonid := %(taxonid)s,
                                            _variableelementid := %(variableelementid)s,
                                            _variableunitsid := %(variableunitsid)s,
                                            _variablecontextid := %(variablecontextid)s)
                """

    for i in range(len(uploader['samples']['samples'])):
        counter = 0
        for val_dict in inputs:
            # Getting TaxonID
            get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
            cur_p.execute(get_taxonid, {'taxonname': val_dict['taxonname']})
            taxonid = cur_p.fetchone()
            if taxonid != None:
                taxonid = int(taxonid[0])
            else:
                counter +=1
                taxonid = counter #placeholder
            
            val_dict['value'] = [None if item == 'NA' else item for item in val_dict['value']]
            val_dict['variableelementid'] = None # placeholder
            val_dict['variablecontextid'] = None # placeholder
            # Get UnitsID
            get_unitsid = """SELECT * FROM ndb.variableunits WHERE variableunits %% %(units)s;"""
            cur_p.execute(get_unitsid, {'units': val_dict['unitcolumn'][i]})
            unitsid = cur_p.fetchone()[0] # This is just getting the varunitsid
            # Insertion might happen if I don't get it, 
            # should I search in proper or tank?
            #For now HT
            cur.execute(get_varid, 
                        {'unitsid':unitsid, 
                         'taxonid': taxonid, 
                         'variableelementid': val_dict['variableelementid'], 
                         'variablecontextid': val_dict['variablecontextid']})
            varid = cur.fetchone()
            if varid != None:
                varid = int(varid[0])
            else:
                # Should I insert in proper or HT? For now HT
                cur.execute(var_query, {'taxonid': taxonid,
                                        'variableelementid': None,
                                        'variableunitsid': unitsid,
                                        'variablecontextid': None}) # inputs[i]['variablecontextid']})
                varid = cur.fetchone()[0]
            
            try:
                cur.execute(data_query, {'sampleid': int(uploader['samples']['samples'][i]),
                                        'variableid': int(varid),
                                        'value': val_dict['value'][i]})
                result = cur.fetchone()[0]
                results_dict['data_points'].append(result)
                results_dict['valid'].append(True)

            except Exception as e:
                logging.error(f"Samples Data is not correct. {e}")
                cur.execute(data_query, {'sampleid': int(uploader['samples']['samples'][i]),
                                         'variableid': None,
                                         'value': None})
                result = cur.fetchone()[0]
                results_dict['data_points'].append(result)
                results_dict['valid'].append(False)
    
    results_dict['valid'] = all(results_dict['valid'])
    return results_dict