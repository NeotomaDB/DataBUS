import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def valid_geochroncontrol(cur, yml_dict, csv_file):
    """Both elements are obtained from uploader. 
    Cannot validate as there are no inserts during the validation process."""
    ['chroncontrolid', 'geochronid']
    response = Response()
    response.valid = [True]
    response.message = ["No validation required for Geochron Control"]
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response
