import logging
from .pull_params import pull_params
import datetime

def insert_sample(cur, cur_p, yml_dict, csv_template, uploader):
    """
    Inserts sample data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted samples.
            - 'samples' (list): List of sample IDs inserted into the database.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    results_dict = {'samples': [], 'valid': []}

    sample_query = """
                   SELECT ts.insertsample(_analysisunitid := %(analysisunitid)s,
                                          _datasetid := %(datasetid)s,
                                          _samplename := %(samplename)s,
                                          _sampledate := %(sampledate)s,
                                          _analysisdate := %(analysisdate)s,
                                          _taxonid := %(taxonid)s,
                                          _labnumber := %(labnumber)s,
                                          _prepmethod := %(prepmethod)s,
                                          _notes := %(notes)s)
                    """
    
    params = ['lab_number', 'sampledate', 'analysisdate', 'labnumber', 'prepmethod', 
              'notes', 'taxonname', 'samplename']       
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.samples')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
  
    for j in range(len(uploader['anunits']['anunits'])):
        get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
        cur_p.execute(get_taxonid, {'taxonname': inputs['taxonname']})
        taxonid = cur_p.fetchone()
        if taxonid != None:
            taxonid = int(taxonid[0])
        else:
            taxonid = None

        try:    
            cur.execute(sample_query, {'analysisunitid': int(uploader['anunits']['anunits'][j]),
                                    'datasetid': int(uploader['datasetid']['datasetid']),
                                    'samplename': inputs['samplename'],
                                    'sampledate': inputs['sampledate'], # datetime.datetime.today().date(),
                                    'analysisdate': inputs['analysisdate'],
                                    'taxonid': taxonid,
                                    'labnumber': inputs['lab_number'],
                                    'prepmethod': inputs['prepmethod'],
                                    'notes': inputs['notes']})
            sampleid = cur.fetchone()[0]
            results_dict['samples'].append(sampleid)
            results_dict['valid'].append(True)
        except Exception as e:
            logging.error(f"Samples data is not correct. {e}")
            cur.execute(sample_query, {'analysisunitid': int(uploader['anunits']['anunits'][j]),
                                    'datasetid': int(uploader['datasetid']['datasetid']),
                                    'samplename': None,
                                    'sampledate': datetime.datetime.today().date(),
                                    'analysisdate': datetime.datetime.today().date(),
                                    'taxonid': None,
                                    'labnumber': None,
                                    'prepmethod': None,
                                    'notes': None})
            sampleid = cur.fetchone()[0]
            results_dict['samples'].append(sampleid)
            results_dict['valid'].append(False)

    assert len(uploader['anunits']['anunits']) == len(results_dict['samples']), "Analysis Units and Samples do not have same length"
    
    results_dict['valid'] = all(results_dict['valid'])
    return results_dict