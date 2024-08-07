import DataBUS.neotomaHelpers as nh
with open('./DataBUS/sqlHelpers/insert_data_uncertainties.sql', 'r') as sql_file:
    data_uncertainties_query = sql_file.read()

def insert_data_uncertainties(cur, yml_dict, csv_file, uploader):
    response = {'data_uncertainties': list(), 'valid': list(), 'message': list()}
    
    params = ['value']
    inputs1 = nh.pull_params(params, yml_dict, csv_file, 'ndb.data')
    print(inputs1[1])

    print('uploader')
    print(uploader['data'])
    #params2 = []
    #inputs2 = nh.pull_params()

    return response
