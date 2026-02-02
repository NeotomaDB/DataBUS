import DataBUS.neotomaHelpers as nh
from DataBUS import AnalysisUnit, Response
from DataBUS.AnalysisUnit import ANALYSIS_UNIT_PARAMS

def valid_analysisunit(yml_dict, csv_file):
    """Validates analysis unit data.

    Validates analysis unit parameters including depth, thickness, facies ID,
    and other stratigraphic properties. Handles both single and multiple analysis
    units, creating AnalysisUnit objects with validated parameters.

    Args:
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.

    Returns:
        Response: Response object containing validation messages, validity list,
                   overall status, and count of created analysis units.
    
    Examples:
        >>> valid_analysisunit(yml_dict, csv_file)
        Response(valid=[True], message=[...], validAll=True, counter=1)
    """
    params = ANALYSIS_UNIT_PARAMS
    response = Response()
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.analysisunits")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"AU Elements in the CSV file are not properly inserted. Please verify the CSV file")
        inputs = {}
    inputs['collectionunitid']=None
    for k in inputs:
        if inputs[k] is None:
            response.message.append(f"? {k} has no values.")
            response.valid.append(True)
        else:
            response.message.append(f"✔ {k} has values.")
            response.valid.append(True)
    if isinstance(inputs.get("depth", None), list):
        response.counter = 0
        iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
        static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
        for values in zip(*iterable_params.values()):
            try:
                kwargs = dict(zip(iterable_params.keys(), values))
                kwargs.update(static_params) 
                AnalysisUnit(**kwargs)
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ AnalysisUnit cannot be created: {e}")
            response.counter += 1
    else:
        AnalysisUnit(**inputs)
        response.counter = 1

    if response.validAll:
        response.message.append("✔ AnalysisUnit can be created")
    response.message = list(set(response.message))
    return response