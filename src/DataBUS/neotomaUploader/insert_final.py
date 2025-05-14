from datetime import datetime
from DataBUS import Response

def insert_final(cur, uploader):
    response = Response()
    query = """INSERT INTO ndb.datasetsubmissions(_datasetid, _databaseid, _contactid, 
                                                  _submissiontypeid, _submissiondate)
                VALUES(%(datasetid)s, %(databaseid)s, %(contactid)s, %(submissiontypeid)s, %(submissiondate)s)
                                                  """
    inputs = {'datasetid': uploader['datasetid'].id, 
              'databaseid': uploader['databaseid'].id, 
              'contactid': uploader['contactid'].id,  # P.I ID
              'submissiontypeid': 6, 
              'submissiondate': datetime.now().date()}
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