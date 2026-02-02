import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response
from DataBUS.Sample import SAMPLE_PARAMS

def valid_sample(cur, yml_dict, csv_file, validator):
    """Validates sample data against the Neotoma database.

    Validates sample parameters including taxon information, analysis dates, and
    preparation methods. Creates Sample objects for each analysis unit with validated
    parameters.

    Examples:
        >>> valid_sample(cursor, config_dict, "sample_data.csv", validator)
        Response(valid=[True], message=[...], validAll=True, counter=5)

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing sample data.
        validator (dict): Dictionary containing validation parameters from prior steps.

    Returns:
        Response: Response object containing validation messages, validity list, and sample counter.
    """
    response = Response()
    params = SAMPLE_PARAMS
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.samples")

    response.counter = 0
    get_taxonid = """SELECT * FROM ndb.taxa 
                     WHERE LOWER(taxonname) %% %(taxonname)s;"""
    for j in range(0, validator["analysisunit"].counter):
        response.counter += 1
        if 'taxonname' in inputs and isinstance(inputs['taxonname'], str):
            inputs['taxonname']=inputs['taxonname'].lower()
            cur.execute(get_taxonid, {"taxonname": inputs["taxonname"]})
            inputs['taxonid'] = cur.fetchone()
            if inputs['taxonid']:
                inputs['taxonid'] = int(inputs['taxonid'][0])
            else:
                inputs['taxonid'] = None
                inputs['taxonid']
        elif 'taxonid' in inputs and isinstance(inputs['taxonid'], int):
            inputs['taxonid'] = int(inputs['taxonid'][0])
        else:
            inputs['taxonid'] = None

        try:
            if 'taxonname' in inputs:
                del inputs['taxonname'] 
            Sample(**inputs)
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Samples data is not correct: {e}")
            response.valid.append(False)
    if response.validAll:
        response.message.append(f"✔ Sample can be created.")
    response.message = list(set(response.message))
    return response