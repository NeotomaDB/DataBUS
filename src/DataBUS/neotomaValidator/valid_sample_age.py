import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response
from DataBUS.SampleAge import SAMPLE_AGE_PARAMS

def valid_sample_age(cur, yml_dict, csv_file):
    """Validates sample age data for paleontological samples.

    Validates sample age parameters including age values, uncertainty bounds, and
    age type. Handles date parsing for collection dates, validates age types against
    database, and creates SampleAge objects for each chronology.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_sample_age(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(SAMPLE_AGE_PARAMS, yml_dict, csv_file, "ndb.sampleages")
        response.valid.append(True)
        if not inputs.get("sampleages"):
            response.valid.append(True)
            response.message.append("? No sample age parameters provided.")
            return response
        else:
            agemodel = inputs.get("agemodel")
            inputs = inputs.get("sampleages")
    except Exception as e:
        response.valid.append(False)
        response.message.append(
            f"✗ Sample Age parameters cannot be properly extracted. {e}")
        return response
    # retrieve a tuple with chronIds and chronnames
    for chron in inputs:
        sa = inputs[chron]
        sa = {k: v if isinstance(v, list) else [v] for k, v in sa.items()}
        for row in zip(*sa.values()):
            sa_age = dict(zip(sa.keys(), row))
            sa_age['agemodel'] = agemodel
            sa_age['chronologyid'] = 1 # placeholder for chronologyid
            sa_age['sampleid'] = 1 # placeholder for sampleid
            if not sa_age.get('age'):
                continue
            if isinstance(sa_age.get('agemodel'), str):
                if (sa_age.get('agemodel').lower() == 'collection date' or
                    'calendar years' in sa_age.get('agemodel').lower()): 
                    try:
                        sa_age['age'] = nh.convert_to_bp(sa_age.get('age'))
                        sa_age['ageolder'] = nh.convert_to_bp(sa_age.get('ageolder'))
                        sa_age['ageyounger'] = nh.convert_to_bp(sa_age.get('ageyounger'))
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"✗ Error parsing collection date: {e}")
                        continue  
            try:
                sa_age.pop('agemodel')
                SampleAge(**sa_age)
                response.valid.append(True)
                if not f"✔ Sample Age is valid." in response.message:
                    response.message.append("✔ Sample Age is valid.")
            except Exception as e:
                response.valid.append(False)
                if not f"✗ Samples ages cannot be created. {e}" in response.message:
                  response.message.append(f"✗ Samples ages cannot be created. {e}")
    return response