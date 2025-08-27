from datetime import datetime
from DataBUS import Response

def insert_final(cur, uploader):
    response = Response()
    query = """INSERT INTO ndb.datasetsubmissions(datasetid, databaseid, contactid, 
                                                  submissiontypeid, submissiondate)
                VALUES(%(datasetid)s, %(databaseid)s, %(contactid)s, %(submissiontypeid)s, %(submissiondate)s)
                                                  """
    inputs = {'datasetid': uploader['datasets'].datasetid, 
              'databaseid': uploader['database'].id[0], 
              'contactid': uploader['datasetpi'].datasetpi[0],  # P.I ID
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