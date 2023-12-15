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
    all_data_points = []
    for i in range(len(uploader['samples']['samples'])):
        data_points_for_sample = []  # Collect data points for each sample
        counter =0
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
            # Get UnitsID from HT
            get_unitsid = """SELECT * FROM ndb.variableunits WHERE variableunits %% %(units)s;"""
            cur.execute(get_unitsid, {'units': val_dict['unitcolumn'][i]})
            unitsid = cur.fetchone()[0] # This is just getting the varunitsid
            cur.execute(get_varid, 
                        {'unitsid':unitsid, 
                         'taxonid': taxonid, 
                         'variableelementid': val_dict['variableelementid'], 
                         'variablecontextid': val_dict['variablecontextid']})
            varid = cur.fetchone()
            if varid != None:
                varid = int(varid[0])
            else:
                cur.execute(var_query, {'taxonid': taxonid,
                                        'variableelementid': None,
                                        'variableunitsid': unitsid,
                                        'variablecontextid': None}) # inputs[i]['variablecontextid']})
                varid = cur.fetchone()[0]

            try:
                val_dict['value'] = [None if item == None else float(item) for item in val_dict['value']]
                data_points_for_sample.append((int(uploader['samples']['samples'][i]),
                                              int(varid),
                                              val_dict['value'][i]))
                results_dict['valid'].append(True)

            except Exception as e:
                logging.error(f"Samples Data is not correct. {e}")
                data_points_for_sample.append((int(uploader['samples']['samples'][i]),
                                              None,
                                              None))
                results_dict['valid'].append(False)
        
        all_data_points.extend(data_points_for_sample)  # Extend data points for all samples

    # Insert all data points at once
    try:
        print(all_data_points)
        cur.executemany(data_query, all_data_points)
        # Assuming 'data_query' can handle multiple inserts at once
        results_dict['valid'] = results_dict['valid'].append(True)
        #results_dict['data_points'].extend(cur.fetchone())

    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        results_dict['valid'] = results_dict['valid'].append(False)

    results_dict['valid'] = all(results_dict['valid'])
    return results_dict