from DataBUS import Response, GeochronControl

def insert_geochroncontrols(cur, yml_dict, csv_file, uploader):
    """Both elements are obtained from uploader. 
    Cannot validate as there are no inserts during the validation process."""
    
    entries = {'chroncontrolid': uploader['chroncontrols'].id, 
               'geochronid': uploader['geochron'].id}
    response = Response()
    if len(entries['chroncontrolid']) == 0 or len(entries['geochronid']) == 0:
        response.message.append("✔ No chroncontrol IDs or geochron IDs to insert.")
        response.valid.append(True)
        response.validAll = all(response.valid)
        return response
    
    try:
        #dividing entrieschroncontrolid by geochronid should be an exact number
        assert len(entries['chroncontrolid']) % len(entries['geochronid']) == 0 
        times = len(entries['chroncontrolid']) // len(entries['geochronid'])
        # duplicate geochronid the appropriate number of times
        entries['geochronid'] = entries['geochronid'] * times
    except AssertionError:
        response.message.append(f"✗  Number of chroncontrol IDs {entries['chroncontrolid']}"
                                f"does not match number of geochron IDs {entries['geochronid']}")
        response.valid.append(False)
    counter = 0
    
    for i in range(len(entries['chroncontrolid'])):
        counter += 1
        entry = {}
        entry['chroncontrolid'] = entries['chroncontrolid'][i]
        entry['geochronid'] = entries['geochronid'][i]
        try:
            gcc = GeochronControl(**entry)
            gcc.insert_to_db(cur)    
            response.valid. append(True)
            response.message.append("✔ GeochronControl inserted.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  GeochronControl not inserted. {e}")
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response