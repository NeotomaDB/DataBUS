import logging
from .pull_params import pull_params

def insert_sample_analyst(cur, cur_p, yml_dict, csv_template, uploader):
    """
    Inserts sample analyst data into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted sample analysts.
            - 'contids' (list): List of dictionaries containing details of the analysts' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    results_dict = {'contids': [], 'valid': []}
    params = ['contactid']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.sampleanalysts')
    get_contact = """SELECT * FROM ndb.contacts WHERE contactname %% %(contactname)s;"""
    
    contids = []
    baseid = 1
    for i in inputs['contactid']:
        cur_p.execute(get_contact, {'contactname': i})
        contids.append({'contactname': i, 'id': cur.fetchone()[0], 'order': baseid})
        baseid = baseid + 1
    results_dict['contids'] = contids

    inserter = """
                SELECT ts.insertsampleanalyst(_sampleid := %(sampleid)s,
                                              _contactid := %(contactid)s,
                                              _analystorder := %(analystorder)s)
                """
    
    for i in range(len(uploader['samples']['samples'])):
        for contact in contids:
            try:
                cur.execute(inserter, {'sampleid': int(uploader['samples']['samples'][i]), 
                                       'contactid': int(contact['id']),
                                       'analystorder': int(contact['order'])})
                results_dict['valid'].append(True)
            except Exception as e:
                logging.error(f"Sample Analyst data is not correct. {e}")
                cur.execute(inserter, {'sampleid': int(uploader['samples']['samples'][i]), 
                                       'contactid': None,
                                       'analystorder': None})
                results_dict['valid'].append(False)
    results_dict['valid'] = all(results_dict['valid'])
    
    return results_dict