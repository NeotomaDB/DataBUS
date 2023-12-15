import datetime
import logging
import datetime
from .pull_params import pull_params

def insert_chronology(cur, cur_p, yml_dict, csv_template, uploader):
    """
    Inserts chronology data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): Dictionary containing information about the inserted chronology.
        Contains keys:
            'chronology': ID of the inserted chronology.
            'valid': Boolean indicating if the insertion was successful.
    """
    results_dict = {'chronology': None, 'valid': False}

    addChron = """
    SELECT ts.insertchronology(_collectionunitid := %(collunitid)s,
                               _agetypeid := %(agetype)s,
                               _contactid := %(contactid)s,
                               _isdefault := TRUE,
                               _chronologyname := %(chronologyname)s,
                               _dateprepared := %(dateprepared)s,
                               _agemodel := %(agemodel)s,
                               _ageboundyounger := %(maxage)s,
                               _ageboundolder := %(minage)s)
                """
    
    get_cont = """SELECT contactid FROM ndb.contacts WHERE %(contactname)s = contactname;"""    
    
    params = ["contactid", "agemodel", "notes"]
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.chronologies')

    params2 = ['age']
    inputs_age = pull_params(params2, yml_dict, csv_template, 'ndb.sampleages')

    inputs_age['age'] = [float(value) if value != 'NA' else None for value in inputs_age['age']]
    agetype = list(set(inputs_age['unitcolumn']))
    agetype = agetype[0]

    cur_p.execute(get_cont, {'contactname': inputs['contactid'][0]})
    contactid = cur_p.fetchone()[0]

    if agetype == 'cal yr BP':
        agetypeid = 2
    elif agetype == 'CE/BCE':
        agetypeid = 1
    else:
        logging.error("The provided age type is incorrect..")
    try:
        maxage = int(max(inputs_age['age']))
    except:
        maxage = None

    try:
        minage = int(min(inputs_age['age']))
    except:
        minage = None
    try:
        cur.execute(addChron, {'collunitid': int(uploader['collunitid']['collunitid']), 
                            'contactid': contactid,
                            'chronologyname': 'Default 210Pb',  # This is a default but might be better to specify in template
                            'agetype': agetypeid, # Comming from column X210Pb.Date.Units which should be linked to params3
                            'dateprepared': datetime.datetime.today().date(),  # Default but should be coming from template s
                            'agemodel': inputs['agemodel'][0],
                            'maxage': maxage, 
                            'minage': minage})
        results_dict['chronology'] = cur.fetchone()[0]
        results_dict['valid'] = True

    except Exception as e:
        logging.error(f"Chronology Data is not correct. Error message: {e}")
        cur.execute(addChron, {'collunitid': int(uploader['collunitid']['collunitid']), 
                            'contactid': contactid,
                            'chronologyname': 'NULL', 
                            'agetype': None, 
                            'dateprepared': datetime.datetime.today().date(),  
                            'agemodel': None,
                            'maxage': None, 
                            'minage': None})
        results_dict['chronology'] = cur.fetchone()[0]
        results_dict['valid'] = False
    
    return results_dict
