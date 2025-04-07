import DataBUS.neotomaHelpers as nh
from DataBUS import Response, GeochronControl

def insert_geochroncontrol(cur, yml_dict, csv_file, uploader):
    """Both elements are obtained from uploader. 
    Cannot validate as there are no inserts during the validation process."""
    
    entries = {'chroncontrolid': uploader['chroncontrols'].ccid, 
               'geochronid': uploader['geochron'].id}
    response = Response()
    print(entries)
    for entry in zip(*entries.values()):
        print(entry)
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
