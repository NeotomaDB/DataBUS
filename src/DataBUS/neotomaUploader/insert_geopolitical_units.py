import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def insert_geopolitical_units(cur, yml_dict, csv_file, uploader):
    """_Inserting geopolitical units"""
    response = Response()
    params = ["geopoliticalunit1", "geopoliticalunit2"]
             # "geopoliticalunit3", "geopoliticalunit4"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sitegeopolitical")
    query = """SELECT geopoliticalid FROM ndb.geopoliticalunits
                WHERE LOWER(geopoliticalname) = %(geopoliticalname)s"""
    query2 = """SELECT geopoliticalid FROM ndb.geopoliticalunits
                WHERE LOWER(geopoliticalname) = %(geopoliticalname)s
                AND highergeopoliticalid = %(highergeopoliticalid)s"""
    insert_query = """SELECT ts.insertsitegeopol(_siteid:= %(siteid)s,
                                                 _geopoliticalid := %(geopolid)s)
                   """
    for unit in inputs:
        if unit == "geopoliticalunit1":
            if inputs[unit]:
                cur.execute(query, {'geopoliticalname' : inputs[unit].lower()})
                gpid1 = cur.fetchone()
                if not gpid1:
                    gpid1 = None
                else:
                    gpid1 = gpid1[0]
                    try:
                        cur.execute(insert_query, {'siteid': uploader["sites"].siteid,
                                            'geopolid': gpid1})
                        response.message.append(f"✔ Site SiteID {uploader['sites'].siteid}, GPUID {gpid1}, {inputs[unit]}, added.")
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"✗ Site SiteID {uploader['sites'].siteid}, GPUID {inputs[unit]} cannot be added. Verify the format {e}.")
        else:
            if (inputs.get(unit)) and (gpid1 is not None):
                cur.execute(query2, {'geopoliticalname' : inputs[unit].lower(),
                                     'highergeopoliticalid' : gpid1})
                gpid2 = cur.fetchone()
                if not gpid2:
                    gpid2 = None
                    response.message.append(f"? Regional GPU {inputs[unit]} under {inputs['geopoliticalunit1']} not found.")
                else:
                    gpid2 = gpid2[0]
                    cur.execute(insert_query, {'siteid': uploader["sites"].siteid,
                                               'geopolid': gpid2})
                    response.message.append(f"✔ Site SiteID {uploader['sites'].siteid}, GPUID {gpid2}, {inputs[unit]}, added.")
    response.validAll = all(response.valid)
    return response