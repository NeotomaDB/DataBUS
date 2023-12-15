import logging
from .pull_params import pull_params

def insert_data_processor(cur, cur_p, yml_dict, csv_template, uploader):
    """
    Inserts data processors into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted data processors.
            - 'processorid' (list): List of processors' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    results_dict = {'processorid': [], 'valid': []}
    params = ['contactid']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.sampleanalysts')

    get_contact = """SELECT * FROM ndb.contacts WHERE contactname %% %(name)s;"""

    contids = list()
    for i in inputs['contactid']:
        cur_p.execute(get_contact, {'name': i})
        contids.append({'name': i, 'id': cur.fetchone()[0]})
    results_dict['processorid'] = contids

    processor = """SELECT ts.insertdataprocessor(_datasetid := %(datasetid)s, 
                                                 _contactid := %(contactid)s)"""
    
    for contact in contids:
        try:
            cur.execute(processor, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                    'contactid': int(contact['id'])})
            results_dict['valid'].append(True)
        except Exception as e:
            logging.error(f"Data processor is not correct. {e}")
            cur.execute(processor, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                    'contactid': None})
            results_dict['valid'].append(True)

    results_dict['valid'] = all(results_dict['valid'])
    return results_dict