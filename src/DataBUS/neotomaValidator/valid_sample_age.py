import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response
from DataBUS.SampleAge import SAMPLE_AGE_PARAMS
import datetime

def valid_sample_age(cur, yml_dict, csv_file):
    """Validates sample age data for paleontological samples.

    Validates sample age parameters including age values, uncertainty bounds, and
    age type. Handles date parsing for collection dates, validates age types against
    database, and creates SampleAge objects for each chronology.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
        validator (dict): Dictionary containing validation parameters from prior steps.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.
    
    Examples:
        >>> valid_sample_age(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...], validAll=True)
    """
    def collapse_into_chronology(data, chron_k='sampleages'):
        """Collapse sample age data structure into a single chronology if applicable.

        Takes nested sample age data and, if only one chronology exists,
        merges shared parameters into that chronology's data dictionary.

        Args:
            data (dict): Data dictionary containing sample ages and shared parameters.
            chron_k (str): Key name for sample ages in data dict. Defaults to 'sampleages'.

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
    try:
        params = SAMPLE_AGE_PARAMS
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")
        inputs = collapse_into_chronology(inputs, chron_k='sampleages')
        if "agetype" in inputs:
            if isinstance(inputs.get("agetype"), list):
                inputs["agetype"] = set(inputs.get("agetype"))
                inputs['agetype'].discard("Event; hiatus")
                inputs["agetype"].discard(None)
                inputs["agetype"] = list(inputs.get("agetype"))
                if inputs["agetype"] == []:
                    inputs["agetype"] = ""
        if isinstance(inputs.get("agetype", None), list) and (len(inputs.get("agetype", [])) == 1):
            inputs["agetype"] = inputs["agetype"][0]
            response.valid.append(True)
        elif isinstance(inputs.get("agetype", None), list) and (len(inputs.get("agetype", [])) > 1):
            at = inputs["agetype"].copy()
            inputs["agetype"] = inputs["agetype"][0]
            response.valid.append(False)
            response.message.append(f"✗ Sample Age agetype must be unique for all entries: {set(at)}.")
    except Exception as e:
        error = str(e)
        try: 
            if "time data" in error.lower():
                age_dict = nh.retrieve_dict(yml_dict, "ndb.sampleages.age")
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
                if "age" in params:
                    params.remove('age')
                    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")
                    inputs['age'] = new_date
                response.valid.append(True)
        except Exception as inner_e:
            response.valid.append(False)
            response.message.append(f"Sample Age parameters cannot be properly extracted. {e}\n {inner_e}")
            return response
    sa_id_placeholder = nh.pull_params(['depth'], yml_dict, csv_file, "ndb.analysisunits")
    inputs['sampleid'] = sa_id_placeholder['depth']
    if inputs['sampleid'] is None:
        sa_id_placeholder['depth'] = [1]
        response.message.append("? No depths found; using placeholder ID 1.")
    for chron in inputs['sampleages'].keys():
        sa = inputs['sampleages'][chron]
        if sa.get("agetype", inputs.get('agetype')) is not None: 
            sa.get("agetype", inputs.get('agetype')).replace("cal yr BP", 'Calendar years BP')
            agetype_query = """SELECT agetypeid FROM ndb.agetypes
                                WHERE LOWER(agetype) = %(agetype)s"""
            cur.execute(agetype_query, {'agetype': sa.get("agetype", inputs.get('agetype')).lower()})
            id = cur.fetchone()
            if id:
                sa['agetypeid'] = id[0]
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
        chron_id = 3 #placeholder
        for idx, sa_id in enumerate(sa_id_placeholder['depth']):
            try:
                if isinstance(sa.get('agemodel'), str):
                    if sa.get('agemodel', inputs.get('agemodel')).lower() == "collection date":
                        
                        if isinstance(sa.get('age', inputs.get('age')), (float, int)):
                            sa['age'] = 1950 - sa.get('age', inputs.get('age'))
                        elif isinstance(sa.get('age', inputs.get('age')), (datetime.date)):
                            sa['age'] = 1950 - sa.get('age', inputs.get('age')).year
                        elif isinstance(sa.get('age', inputs.get('age')), list):
                            sa['age'] = [1950 - value.year if isinstance(value, (datetime.date)) else 1950 - value
                                            for value in sa.get('age', inputs.get('age'))]                
                if sa["age"] is None:
                    continue
                else:
                    s = {}
                    for param in ["ageyounger", "ageolder"]:
                        if param in sa:
                            if isinstance(sa[param], list):
                                s[param]= int(min([num for num in sa.get(param, sa.get('age')) if num is not None])) if param == 'ageboundyounger' else int(max([num for num in sa.get(param, sa.get('age')) if num is not None]))
                            else:
                                s[param]= sa[param]
                        else:
                            if isinstance(sa['age'], list):
                                s[param]= int(min([num for num in sa.get('age') if num is not None])) if param == 'ageboundyounger' else int(max([num for num in sa.get('age') if num is not None]))
                            else:
                                s[param]= None
                        sa[param] = s[param]
                    if sa_id is None:
                        continue
                    else:
                        if isinstance(sa['age'], list):
                            sa_age = sa['age'][idx]
                        else:
                            sa_age = sa['age']
                        SampleAge(sampleid=int(sa_id),
                                chronologyid =int(chron_id),
                                age = sa_age,
                                ageyounger = sa['ageyounger'],
                                ageolder = sa['ageolder'],)
                        response.valid.append(True)
                        response.message.append(f"✔ Sample Age is valid.")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Samples ages cannot be created. {e}")
    response.message = list(set(response.message))
    return response