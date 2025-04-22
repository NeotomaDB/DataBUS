import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, ChronResponse
from datetime import datetime

# Updated from Sisal
def valid_chronologies(cur, yml_dict, csv_file, multiple = False):
    """
        Validates chronologies based on provided parameters and data.
    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
    Returns:
        ChronResponse: An object containing validation results, messages, and status.
    Raises:
        ValueError: If there is an issue with the extracted parameters.
        AssertionError: If the date format in the CSV file is incorrect.
    """
    response = ChronResponse()

    if multiple == True:
        params = ['age', 'ageolder', 'ageyounger']
    else:
        params = ['chronologyname', 'agemodel', 'ageboundolder', 'ageboundyounger', 
                  'agetype', 'contactid', 'isdefault', 'dateprepared', 'notes']
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
    if multiple == True:
        response.message.append("✔ File with multiple chronologies")
        for chron in inputs:
            c = {'agetypeid': inputs[chron].get('agetypeid'),
                 'contactid': inputs[chron].get('contactid'),
                 'isdefault': inputs[chron].get('isdefault'),
                 'chronologyname': chron,
                 'dateprepared': inputs[chron].get('dateprepared'),
                 'agemodel': inputs[chron].get('agemodel'),
                 'ageboundyounger': inputs[chron].get('ageboundyounger'),
                 'ageboundolder': inputs[chron].get('ageboundolder'),
                 'notes': inputs[chron].get('notes'),
                 'recdatecreated': inputs[chron].get('recdatecreated'),
                 'recdatemodified': inputs[chron].get('recdatemodified')}
            try:
                if not (c.get("ageboundolder") and c.get("ageboundyounger")):
                    c["ageboundolder"]= int(max(inputs[chron].get('age')))
                    c["ageboundyounger"]= int(min(inputs[chron].get('age')))
                else:
                    c["ageboundolder"]= int(max(c["ageboundolder"]))
                    c["ageboundyounger"]= int(min(c["ageboundyounger"]))
                Chronology(**c)
                response.valid.append(True)
                response.message.append("✔  Chronology can be created")
            except Exception as e:
                print("Chronology error", e)
                response.valid.append(False)
                response.message.append(f"✗  Chronology cannot be created: {e}")
    else: 
        if inputs.get('agemodel') == "collection date":
            if isinstance(inputs.get('age', None), (float, int)):
                inputs['age'] = 1950 - inputs['age']
            elif isinstance(inputs.get('age', None), datetime):
                inputs['age'] = 1950 - inputs['age'].year
            elif isinstance(inputs.get('age', None), list):
                inputs['age'] = [1950 - value.year if isinstance(value, datetime) else 1950 - value
                                for value in inputs['age']]
                if 'age' in inputs:
                    if not (inputs["ageboundolder"] and inputs["ageboundyounger"]):
                        inputs["ageboundyounger"]= int(min(inputs["age"])) 
                        inputs["ageboundolder"]= int(max(inputs["age"])) 
        
        # to add for lead models because they use more calendar format
        if inputs.get("agetype"): 
            inputs["agetype"].replace("cal yr BP", 'Calendar years BP')
            agetype_query = """SELECT agetypeid FROM ndb.agetypes
                            WHERE LOWER(agetype) = %(agetype)s"""
            cur.execute(agetype_query, {'agetype': inputs["agetype"].lower()})
            id = cur.fetchone()
            if id:
                inputs["agetypeid"] = id[0]
                response.message.append("✔ The provided age type is correct.")
                response.valid.append(True)
            else:
                response.message.append("✗ The provided age type does not exist in Neotoma DB.")
                response.valid.append(False)
                inputs["agetypeid"] = None
        else:
            response.message.append("? No age type provided.")
            response.valid.append(True)
            inputs["agetypeid"] = None

        if "agetype" in inputs:
            del inputs["agetype"]
        if "age" in inputs:
            del inputs["age"]
        for k in inputs:
            if not inputs[k]:
                response.message.append(f"? {k} has no values.")
            else:
                response.message.append(f"✔ {k} looks valid.")
                response.valid.append(True)
        try:
            Chronology(**inputs)
            response.valid.append(True)
            response.message.append("✔  Chronology can be created")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Chronology cannot be created: {e}")
        response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response