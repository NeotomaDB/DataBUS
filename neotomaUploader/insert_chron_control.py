import logging
import numpy as np
from .pull_params import pull_params

def insert_chron_control(cur, yml_dict, csv_template, uploader):
    """
    Inserts chronological control data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted chronological control units.
            'chron_control_units' (list): List of IDs for the inserted chronological control units.
            'valid' (bool): Indicates if all insertions were successful.

    Raises:
        AssertionError: If the number of analysis units, ages, and thicknesses are not consistent.
    """
    results_dict = {'chron_control_units': [], 'valid': []}

    addcontrol = """
        SELECT ts.insertchroncontrol(_chronologyid := %(chronid)s,
            _chroncontroltypeid := 10,
            _analysisunitid := %(annuid)s,
            _depth := %(depth)s,
            _thickness := %(thickness)s,
            _agetypeid := %(agetypeid)s,
            _age := %(age)s,
            _agelimityounger := %(ageyoung)s,
            _agelimitolder := %(ageold)s,
            _notes := %(notes)s)"""
    
    params = ["depth", "thickness", 'notes',]
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.analysisunits')

    params_age = ['age']
    inputs_age = pull_params(params_age, yml_dict, csv_template, 'ndb.sampleages')

    inputs_age['age'] = [float(value) if value != 'NA' else np.nan for value in inputs_age['age']]
    inputs_age['uncertainty'] = [float(value) if value != 'NA' else np.nan for value in inputs_age['uncertainty']]
    
    agetype = list(set(inputs_age['unitcolumn']))
    agetype = agetype[0]
    
    assert len(uploader['anunits']['anunits']) == len(inputs_age['age']) == len(inputs['thickness']), \
           "The number of analysis units, ages, and thicknesses is not the same. Please check."
    
    for i in range(len(uploader['anunits']['anunits'])):
        if inputs_age['unitcolumn'][i] == 'cal yr BP':
            agetypeid = 2
        elif inputs_age['unitcolumn'][i] == 'CE/BCE':
            agetypeid = 1
        else:
            logging.error("The provided age type is incorrect..")
        try:
            cur.execute(addcontrol, {'chronid': int(uploader['chronology']['chronology']),
                                    'annuid': int(uploader['anunits']['anunits'][i]),
                                    'depth': inputs['depth'][i],
                                    'thickness': inputs['thickness'][i],
                                    'agetypeid': agetypeid,
                                    'age': inputs_age['age'][i],
                                    'notes':inputs['notes'][i],
                                    'ageyoung': inputs_age['age'][i] + inputs_age['uncertainty'][i],
                                    'ageold': inputs_age['age'][i] -  inputs_age['uncertainty'][i]})
            results_dict['chron_control_units'].append(cur.fetchone()[0])
            results_dict['valid'].append(True)

        except Exception as e:
            logging.error(f"Chronology Data is not correct. Error message: {e}")
            cur.execute(addcontrol, {'chronid': int(uploader['chronology']['chronology']),
                                    'annuid': int(uploader['anunits']['anunits'][i]),
                                    'depth': np.nan,
                                    'thickness': np.nan,
                                    'agetypeid': np.nan,
                                    'age': np.nan,
                                    'notes': 'NULL',
                                    'ageyoung': np.nan,
                                    'ageold': np.nan})
            results_dict['chron_control_units'].append(cur.fetchone()[0])
            results_dict['valid'].append(False)
    results_dict['valid'] = all(results_dict['valid'])
    return results_dict