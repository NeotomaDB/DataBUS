import importlib.resources
import DataBUS.neotomaHelpers as nh
from DataBUS import Response
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "insert_speleothem_cu.sql") as sql_file:
    insert_speleothem_cu = sql_file.read()

def insert_speleo_cu(cur, yml_dict, csv_file, uploader):
    params = ['persistid']
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.speleothemcollectionunits")
    response = Response()
    cur.execute(insert_speleothem_cu)
    query = """
            SELECT insert_speleothem_cu(_entityid := %(entityid)s,
                                        _collectionunitid := %(collectionunitid)s,
                                        _persistid := %(persistid)s);
            """
    inputs = {"entityid": uploader['speleothem'].id,
                "collectionunitid": uploader['collunitid'].cuid,
                "persistid": inputs.get('persistid')}
    try:
        cur.execute(query, inputs)
        response.valid.append(True)
        response.message.append(f"✔  Speleothem: {inputs['entityid']} and CollectionUnitID: "
                                f"{inputs['collectionunitid']} relationship inserted. \n"
                                f"PersistID: {inputs.get('persistid')}")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  Speleothem: {inputs['entityid']} and CollectionUnitID: "
                                f"{inputs['collectionunitid']} relationship could not be inserted: {e}")
    response.validAll = all(response.valid)
    return response