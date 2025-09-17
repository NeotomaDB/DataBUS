from DataBUS import Response, GeochronControl

def insert_geochroncontrols(cur, yml_dict, csv_file, uploader):
    """Both elements are obtained from uploader. 
    Cannot validate as there are no inserts during the validation process."""
    
    entries = {'chroncontrolid': uploader['chroncontrols'].id, 
               'geochronid': uploader['geochron'].id}

    response = Response()
    try:
        assert len(entries['chroncontrolid']) == len(entries['geochronid'])
    except AssertionError:
        response.message.append("✗  Number of chroncontrol IDs does not match number of geochron IDs")
        response.valid.append(False)
    
    for val in zip(*entries.values()):
        entry = {}
        entry['chroncontrolid'] = val[0]
        entry['geochronid'] = val[1]
        try:
            gcc = GeochronControl(**entry)
            gcc.insert_to_db(cur)    
            response.valid. append(True)
            response.message.append("GeochronControl inserted.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"GeochronControl not inserted. {e}")
    
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response