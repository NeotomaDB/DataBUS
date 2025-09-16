import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, Response
from datetime import datetime

def insert_chronology(cur, yml_dict, csv_file, uploader, multiple = False):
    """
    Inserts chronology data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): Dictionary containing information about the inserted chronology.
        Contains keys:
            'chronology': ID of the inserted chronology.
            'valid': Boolean indicating if the insertion was successful.
    """
    response = Response()
    params = ['ageboundolder', 'ageboundyounger', 'agemodel',
              'agetype', 'contactid', 'dateprepared', 'notes', 'age']
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.chronologies", values = False)
    except Exception as e:
        error_message = str(e)
        try:
            if "time data" in error_message.lower():
                age_dict = nh.retrieve_dict(yml_dict, "ndb.chronologies.age")
                column = age_dict[0]['column']
                if isinstance(csv_file[0][column], str) and len(csv_file[0][column]) >= 4:
                    if len(csv_file[0][column]) == 7 and csv_file[0][column][4] == '-' and csv_file[0][column][5:7].isdigit():
                        new_date = f"{csv_file[0][column]}-01"
                        new_date = new_date.replace('-', '/')
                        new_date = datetime.strptime(new_date, "%Y/%m/%d")
                    elif csv_file[0][column][:4].isdigit():
                        new_date = int(csv_file[0][column][:4])
                    else:
                        new_date = None
                else:
                    new_date = None
            if 'age' in params:
                params.remove('age')
                inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.chronologies")
                inputs['age'] = new_date
                response.valid.append(True)
        except Exception as inner_e:
            response.validAll = False
            response.message.append(f"Chronology parameters cannot be properly extracted. {e}\n"
                                    f"{str(inner_e)}")
            return response
    
    if len(inputs['chronologies']) >1:
        response.message.append("✔ File with multiple chronologies: ")
        response.message.append(f"{str(list(inputs['chronologies'].keys())).replace('[', '').replace(']', '')}")
    for chron in inputs['chronologies']:
        ch = inputs['chronologies'][chron]
        if ch.get("agetype", inputs['agetype']) is not None: 
            ch.get("agetype", inputs['agetype']).replace("cal yr BP", 'Calendar years BP')
            agetype_query = """SELECT agetypeid FROM ndb.agetypes
                                WHERE LOWER(agetype) = %(agetype)s"""
            cur.execute(agetype_query, {'agetype': ch.get("agetype", inputs['agetype']).lower()})
            id = cur.fetchone()
            if id:
                ch['agetypeid'] = id[0]
                response.message.append(f"✔ The provided age type is correct: {id[0]}")
                response.valid.append(True)
            else:
                response.message.append("✗ The provided age type does not exist in Neotoma DB.")
                response.valid.append(False)
                inputs["agetypeid"] = None
        else:
            response.message.append("? No age type provided.")
            response.valid.append(True)
            inputs["agetypeid"] = None
        # create the chronology
        c = {'collectionunitid': uploader["collunitid"].cuid,
             'agetypeid': ch.get('agetypeid', inputs.get('agetypeid')),
             'contactid': ch.get('contactid', inputs.get('contactid')),
             'isdefault': ch.get('isdefault', inputs.get('isdefault')),
             'chronologyname': chron,
             'dateprepared': ch.get('dateprepared', inputs.get('dateprepared')),
             'agemodel': ch.get('agemodel', inputs.get('agemodel')),
             'ageboundyounger': ch.get('ageboundyounger'),
             'ageboundolder': ch.get('ageboundolder'),
             'notes': ch.get('notes', inputs.get('notes')),
             'recdatecreated': ch.get('recdatecreated', inputs.get('recdatecreated')),
             'recdatemodified': ch.get('recdatemodified', inputs.get('recdatemodified'))}
        try:
            if ch.get('agemodel', inputs.get('agemodel')) == "collection date":
                if isinstance(ch.get('age', inputs.get('age')), (float, int)):
                    ch['age'] = 1950 - ch.get('age', inputs.get('age'))
                elif isinstance(ch.get('age', inputs.get('age')), datetime):
                    ch['age'] = 1950 - ch.get('age', inputs.get('age')).year
                elif isinstance(ch.get('age', inputs.get('age')), list):
                    ch['age'] = [1950 - value.year if isinstance(value, datetime) else 1950 - value
                                    for value in ch.get('age', inputs.get('age'))]
            c["ageboundolder"]= int(max([num for num in ch.get('ageboundolder', ch.get('age')) if num is not None]))
            c["ageboundyounger"]= int(min([num for num in ch.get('ageboundyounger', ch.get('age')) if num is not None]))
            cronology = Chronology(**c)
            response.valid.append(True)
            try:
                chronid = cronology.insert_to_db(cur)
                response.id.append(chronid)
                response.valid.append(True)
                response.message.append(f"✔ Added Chronology {chronid}.")
            except Exception as e:
                response.message.append(f"✗  Chronology Data is not correct. "
                                        f"Error message: {e}")
                response.valid.append(False)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Chronology cannot be created: {e}")
    
    response.validAll = all(response.valid)
    return response  