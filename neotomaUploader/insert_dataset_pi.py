import logging
from .pull_params import pull_params

def insert_dataset_pi(cur, yml_dict, csv_template, uploader):
    """
    Inserts dataset principal investigator data into Neotomas.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted dataset principal investigators.
            - 'dataset_pi_ids' (list): List of dictionaries containing details of the contacts, including their IDs and order.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    results_dict = {'dataset_pi_ids': [], 'valid': []}
    params = ['contactname']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.contacts')
    
    get_contact = """SELECT * FROM ndb.contacts WHERE contactname %% %(name)s;"""
    
    baseid = 1
    contids = []
    for i in inputs['contactname']:
        cur_p.execute(get_contact, {'name': i})
        contids.append({'name': i, 'id': cur.fetchone()[0], 'order': baseid})
        baseid = baseid + 1
    
    inserter = """SELECT ts.insertdatasetpi(_datasetid := %(datasetid)s, 
                                            _contactid := %(contid)s,
                                            _piorder := %(piorder)s);"""
    for contact in contids:
        try:
            cur.execute(inserter, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                   'contid': int(contact['id']),
                                   'piorder': int(contact['order'])})
            results_dict['valid'].append(True)

        except Exception as e:
            logging.error(f"DatasetPI is not correct. {e}")
            cur.execute(inserter, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                   'contid': None,
                                   'piorder': None})
            results_dict['valid'].append(False)

    results_dict['dataset_pi_ids'] = contids
    results_dict['valid'] = all(results_dict['valid'])
    return results_dict