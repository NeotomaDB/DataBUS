import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response
import datetime

def insert_sample_age(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample age data into a database
    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.
    Returns:
        response (dict): A dictionary containing information about the inserted sample ages.
            - 'sampleAge' (list): List of IDs for the inserted sample age data.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    try:
        params = ["age", "ageyounger", "ageolder", "agetype"]
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")
        if "agetype" in inputs:
            inputs["agetype"] = set(inputs.get("agetype"))
            inputs['agetype'].discard("Event; hiatus")
            inputs["agetype"].discard(None)
            inputs["agetype"] = list(inputs.get("agetype"))
        if isinstance(inputs.get("agetype", None), list) and (len(inputs.get("agetype", [])) == 1):
            inputs["agetype"] = inputs["agetype"][0]
            response.valid.append(True)
        elif isinstance(inputs.get("agetype", None), list) and (len(inputs.get("agetype", [])) > 1):
            response.valid.append(False)
            response.message.append(f"✗ Sample Age agetype must be unique for all entries: {set(inputs['agetype'])}.")
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
            response.validAll = False
            response.message.append(f"Sample Age parameters cannot be properly extracted. {e}\n {inner_e}")
            return response
    if inputs.get('agetype', '').lower() == "collection date":
        if isinstance(inputs.get('age', None), (float, int)):
            inputs['age'] = 1950 - inputs['age']
        elif isinstance(inputs.get('age', None), (datetime.datetime, datetime.date)):
            inputs['age'] = 1950 - inputs['age'].year
        elif isinstance(inputs.get('age', None), list):
            inputs['age'] = [1950 - value.year if isinstance(value, datetime.datetime) else 1950 - value
                            for value in inputs['age']]
            if 'age' in inputs:
                if not (inputs["ageboundolder"] and inputs["ageboundyounger"]):
                    inputs["ageboundyounger"]= int(min(inputs["age"])) 
                    inputs["ageboundolder"]= int(max(inputs["age"])) 
                del inputs['age']
    if "agetype" in inputs:
        del inputs['agetype']
    for chronos, id in uploader['chronology'].name.items():
        chron_id = id
        chron_name = chronos
        for idx, sa_id in enumerate(uploader['samples'].sampleid):
            ageyounger = inputs['sampleages'][chron_name].get('ageyounger')
            ageolder   = inputs['sampleages'][chron_name].get('ageolder')   
            if not ageyounger or not ageolder:
                if inputs['sampleages'][chron_name].get('uncertainty', None):
                    inputs['sampleages'][chron_name]['ageyounger'] = inputs['sampleages'][chron_name]["age"] - inputs['sampleages'][chron_name]["uncertainty"]
                    inputs['sampleages'][chron_name]['ageolder'] = inputs['sampleages'][chron_name]["age"] + inputs['sampleages'][chron_name]["uncertainty"]
                else:
                    response.message.append("? No uncertainty to substract. Ageyounger/Ageolder will be None.")
                    inputs['sampleages'][chron_name]['ageyounger'] = None
                    inputs['sampleages'][chron_name]['ageolder'] = None 
            if inputs['sampleages'][chron_name]['age'][idx] is None:
                    continue
            try:
                if inputs['sampleages'][chron_name]['ageolder'] is None:
                    ageolder_ = None
                else:
                    ageolder_ = float(inputs['sampleages'][chron_name]['ageolder'][idx])
                if inputs['sampleages'][chron_name]['ageyounger'] is None:
                    ageyounger_ = None
                else:
                    ageyounger_ = float(inputs['sampleages'][chron_name]['ageyounger'][idx])
                sample_age = SampleAge(sampleid=int(sa_id),
                                        chronologyid =int(chron_id),
                                        age = inputs['sampleages'][chron_name]['age'][idx],
                                        ageyounger = ageyounger_,
                                        ageolder = ageolder_)
                response.valid.append(True)
                try:
                    sample_age.insert_to_db(cur)
                except Exception as e:
                    response.message.append(f"✗ Samples ages cannot be inserted: {e}")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Samples ages cannot be created. {e}")
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response