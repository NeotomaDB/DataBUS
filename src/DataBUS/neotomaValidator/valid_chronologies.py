import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, Response
from DataBUS.Chronology import CHRONOLOGY_PARAMS
import datetime

def valid_chronologies(cur, yml_dict, csv_file):
    """Validates chronologies for geochronological data.

    Validates chronology parameters including age type, contact ID, date prepared,
    and age bounds. Handles age model conversions (e.g., collection date to years BP)
    and creates Chronology objects with validated parameters.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Raises:
        ValueError: If there are issues with extracted parameters.
        AssertionError: If date format in CSV file is incorrect.
    
    Examples:
        >>> valid_chronologies(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...])
    """
    def collapse_into_chronology(data, chron_k='chronologies'):
        """Collapse chronology data structure into a single chronology if applicable.

        Takes nested chronology data and, if only one chronology exists,
        merges shared parameters into that chronology's data dictionary.

        Args:
            data (dict): Data dictionary containing chronologies and shared parameters.
            chron_k (str): Key name for chronologies in data dict. Defaults to 'chronologies'.

        Returns:
            dict: Data dict with single chronology collapsed or original structure if multiple.
        """
        chron = data.get(chron_k, {})
        if len(chron) == 1:
            key = next(iter(chron))
            inner = chron[key]
            for k, v in data.items():
                if k != chron_k:
                    inner[k] = v
            return {chron_k: {key: inner}}
        return data

    response = Response()
    params = CHRONOLOGY_PARAMS
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
            else:
                inputs = {}
        except Exception as inner_e:
            inputs = {}
            response.valid.append(False)
            response.message.append(f"✗  Chronology parameters cannot be properly extracted. {e}\n"
                                    f"{str(inner_e)}")
            return response
    inputs = collapse_into_chronology(inputs)
    if len(inputs['chronologies']) >1:
        response.message.append("✔ File with multiple chronologies")
        response.message.append(f"{list(inputs['chronologies'].keys())}")
    for chron in inputs['chronologies']:
        ch = inputs['chronologies'][chron]
        if ch.get("agetype", inputs.get('agetype')) is not None: 
            ch.get("agetype", inputs.get('agetype')).replace("cal yr BP", 'Calendar years BP')
            agetype_query = """SELECT agetypeid FROM ndb.agetypes
                                WHERE LOWER(agetype) = %(agetype)s"""
            cur.execute(agetype_query, {'agetype': ch.get("agetype", inputs.get('agetype')).lower()})
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
        c = {'agetypeid': ch.get('agetypeid', inputs.get('agetypeid')),
             'contactid': ch.get('contactid', inputs.get('contactid')),
             'isdefault': ch.get('isdefault', inputs.get('isdefault')),
             'chronologyname': chron,
             'dateprepared': ch.get('dateprepared', inputs.get('dateprepared')),
             'agemodel': ch.get('agemodel', inputs.get('agemodel')),
             'ageboundyounger': ch.get('ageboundyounger'),
             'ageboundolder': ch.get('ageboundolder'),
             'notes': ch.get('notes', inputs.get('notes'))}
        try:
            if ch.get('agemodel', inputs.get('agemodel')) == "collection date":
                if isinstance(ch.get('age', inputs.get('age')), (float, int)):
                    ch['age'] = 1950 - ch.get('age', inputs.get('age'))
                elif isinstance(ch.get('age', inputs.get('age')), datetime):
                    ch['age'] = 1950 - ch.get('age', inputs.get('age')).year
                elif isinstance(ch.get('age', inputs.get('age')), list):
                    ch['age'] = [1950 - value.year if isinstance(value, datetime) else 1950 - value
                                    for value in ch.get('age', inputs.get('age'))]
            for param in ['ageboundolder', 'ageboundyounger']:
                if param in ch:
                    if isinstance(ch[param], list):
                        c[param]= int(min([num for num in ch.get(param, ch.get('age')) if num is not None])) if param == 'ageboundyounger' else int(max([num for num in ch.get(param, ch.get('age')) if num is not None]))
                    else:
                        c[param]= ch[param]
                else:
                    if isinstance(ch['age'], list):
                        c[param]= int(min([num for num in ch.get('age') if num is not None])) if param == 'ageboundyounger' else int(max([num for num in ch.get('age') if num is not None]))
                    else:
                        c[param]= None
            Chronology(**c)
            response.valid.append(True)
            response.message.append("✔  Chronology can be created")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Chronology cannot be created: {e}")
    response.message = list(set(response.message))
    return response