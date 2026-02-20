import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response
from DataBUS.Sample import SAMPLE_PARAMS

def valid_sample(cur, yml_dict, csv_file, validator):
    """Validates sample data against the Neotoma database.

    Validates sample parameters including taxon information, analysis dates, and
    preparation methods. Creates Sample objects for each analysis unit with validated
    parameters.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing sample data.
        validator (dict): Dictionary containing validation parameters from prior steps.

    Returns:
        Response: Response object containing validation messages, validity list, and sample counter.

    Examples:
        >>> valid_sample(cursor, config_dict, "sample_data.csv", validator)
        Response(valid=[True], message=[...], validAll=True, counter=5)
    """
    response = Response()
    try:
        inputs = nh.pull_params(SAMPLE_PARAMS, yml_dict, csv_file, "ndb.samples")
        inputs['analysisunitid'] = list(range(1, validator['analysisunit'].counter+1))
        inputs['datasetid'] = list(range(1, validator['analysisunit'].counter+1))
        inputs = {k: v for k, v in inputs.items() if v is not None}
    except Exception as e:
        response.message.append(f"✗ Error pulling sample parameters: {e}")
        response.valid.append(False)
        return response
    response.counter = 0
    get_taxonid = """SELECT taxonid FROM ndb.taxa
                     WHERE LOWER(taxonname) %% %(taxonname)s;"""
    for row in zip(*inputs.values()):
        response.counter += 1
        sample = dict(zip(inputs.keys(), row))
        if isinstance(sample.get('taxonid'), str):
            cur.execute(get_taxonid, {"taxonname": sample["taxonid"].lower().strip()})
            sample['taxonid'] = cur.fetchone()
            if sample['taxonid']:
                sample['taxonid'] = sample['taxonid'][0]
        try:
            Sample(**sample)
            response.valid.append(True)
            if f"✔ Sample can be created." not in response.message:
                response.message.append(f"✔ Sample can be created.")
        except Exception as e:
            if f"✗  Samples in AU ID {sample.get('analysisunitid')} is not correct: {e}" not in response.message:
                response.message.append(f"✗  Samples in AU ID {sample.get('analysisunitid')} is not correct: {e}")
            response.valid.append(False)
    return response