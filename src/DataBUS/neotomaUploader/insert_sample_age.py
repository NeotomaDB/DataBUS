import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response


def insert_sample_age(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample age data into a database.

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

    params = ["age"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")

    # inputs['age'] = [float(value) if value != 'NA' else None for value in inputs['age']]
    inputs["uncertainty"] = [
        float(value) if value != "NA" else None for value in inputs["uncertainty"]
    ]

    for i in range(len(uploader["samples"].sampleid)):
        if isinstance(inputs["age"][i], (int, float)):
            age_older = inputs["age"][i] - inputs["uncertainty"][i]
            age_younger = inputs["age"][i] + inputs["uncertainty"][i]
        else:
            response.message.append(
                "? Age is set to None. Ageyounger/Ageolder will be None."
            )
            age_younger = None
            age_older = None
        try:
            sa = SampleAge(
                sampleid=int(uploader["samples"].sampleid[i]),
                chronologyid=int(uploader["chronology"].chronid),
                age=inputs["age"][i],
                ageyounger=age_younger,
                ageolder=age_older,
            )
            response.valid.append(True)
        except Exception:
            response.valid.append(False)
            sa = SampleAge()
        finally:
            try:
                sa_id = sa.insert_to_db(cur)
                response.valid.append(True)
                response.message.append(
                    f"✔ Added sample age {sa_id} for sample {uploader['samples'].sampleid[i]}"
                )
            except Exception as e:
                response.message.append(f"✗ Cannot add Samples Age: {e}")
                response.valid.append(False)

    response.validAll = all(response.valid)
    return response
