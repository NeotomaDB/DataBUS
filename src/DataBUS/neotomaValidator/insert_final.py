from datetime import datetime

from DataBUS import Response


def insert_final(cur, databus):
    """Finalizes a dataset submission by inserting a record into ndb.datasetsubmissions.

    This function should be called only after all prior validation steps have passed
    (``validAll`` is True for every key in ``databus``). It records the submission
    in the Neotoma database, linking the dataset, database, and contact together with
    a submission date and a fixed submission type of 6.

    Args:
        cur: Database cursor for executing SQL queries.
        databus (dict): Accumulated validation results. Must contain:
            - ``databus["datasets"].id_int`` – ID of the validated dataset.
            - ``databus["database"].id_int`` – ID of the target database.
            - ``databus["contacts"].id_int`` – ID of the submitting contact.

    Returns:
        Response: Response object with validity status and a success/failure message.

    Examples:
        >>> if all(databus[k].validAll for k in databus):
        ...     result = insert_final(cur, databus=databus)
        Response(valid=[True], message=["✔ Dataset submission has been finalized"])
    """
    response = Response()
    query = """INSERT INTO ndb.datasetsubmissions(datasetid, databaseid, contactid,
                                                  submissiontypeid, submissiondate)
                VALUES(%(datasetid)s, %(databaseid)s, %(contactid)s, %(submissiontypeid)s, %(submissiondate)s)
                                                  """
    inputs = {
        "datasetid": databus["datasets"].id_int,
        "databaseid": databus["database"].id_int,
        "contactid": databus["contacts"].id_int,
        "submissiontypeid": 6,
        "submissiondate": datetime.now().date(),
    }
    try:
        cur.execute(query, inputs)
        response.valid.append(True)
        response.message.append("✔ Dataset submission has been finalized")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Dataset submission cannot be finalized: {e}")
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response
