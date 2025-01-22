import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response


def valid_sample(cur, yml_dict, csv_file, validator):
    """
    Inserts sample data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted samples.
            - 'samples' (list): List of sample IDs inserted into the database.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    params = [
        "sampledate",
        "analysisdate",
        "prepmethod",
        "notes",
        "taxonname",
        "samplename",
    ]
    inputs = nh.clean_inputs(nh.pull_params(params, yml_dict, csv_file, "ndb.samples"))
    inputs["labnumber"] = nh.retrieve_dict(yml_dict, "ndb.samples.labnumber")
    inputs["labnumber"] = inputs["labnumber"][0]["value"]

    response.sa_counter = 0
    for j in range(0, validator["analysisunit"].aucounter):
        response.sa_counter += 1
        get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
        cur.execute(get_taxonid, {"taxonname": inputs["taxonname"]})
        taxonid = cur.fetchone()
        if taxonid != None:
            taxonid = int(taxonid[0])
        else:
            taxonid = None

        try:
            Sample(
                samplename=inputs["samplename"],
                sampledate=inputs["sampledate"],
                analysisdate=inputs["analysisdate"],
                taxonid=taxonid,
                labnumber=inputs["labnumber"],
                prepmethod=inputs["prepmethod"],
                notes=inputs["notes"],
            )
            response.valid.append(True)

        except Exception as e:
            response.message.append(f"✗ Samples data is not correct: {e}")
            response.valid.append(False)

    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append("✔ Sample can be created.")
    return response
