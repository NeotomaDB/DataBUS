import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def valid_geochroncontrol(validator):
    """Both elements are obtained from uploader. 
    Cannot validate as there are no inserts during the validation process.
    """
    response = Response()
    try:
        assert 'chron_controls' in validator, "Validator dictionary must contain 'chron_controls' key"
        assert 'geochron' in validator, "Validator dictionary must contain 'geochron' key"
        assert validator['chron_controls'].validAll is True, "Chroncontrols were not successful in validation."
        assert validator['geochron'].validAll is True, "Geochron was not successful in validation."
        response.valid.append(True)
        response.message.append("✔  Geochron Control and Chroncontrols are present in validator.")
    except AssertionError as e:
        response.valid.append(False)
        response.message.append(f"✘ {str(e)}")
        response.valid.append(False)
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response
