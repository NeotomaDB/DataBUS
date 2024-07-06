from DataBUS.neotomaHelpers.pull_params import pull_params

def valid_analysisunit(yml_dict, csv_file):
    """_Inserting analysis units_"""
    params = ["analysisunitname", "depth", "thickness", "faciesid", "mixed", "igsn", "notes"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.analysisunits')
    
    response = {'valid': [],
                'message': []}

    for k in inputs:
        if len(inputs[k]) == 0:
            response['message'].append(f'? {k} has no values.')
            response['valid'].append(True)
        else:
            response['message'].append(f'✔ {k} has values.')
            response['valid'].append(True)
    response['valid'] = all(response['valid'])

    return response

        