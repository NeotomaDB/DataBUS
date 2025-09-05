import DataBUS.neotomaHelpers as nh
from DataBUS import Hiatus, Response
from itertools import groupby

def find_clusters(indices):
    if not indices:
        return []
    indices = sorted(indices)
    clusters = [
        [x[1] for x in group]
        for k, group in groupby(enumerate(indices), lambda x: x[1] - x[0])
    ]
    return clusters

def valid_hiatus(cur, yml_dict, csv_file):
    """
    """
    response = Response()

    params = ['hiatus', 'notes']
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.hiatuses")
    # Check if hiatus is None
    if inputs['hiatus'] is not None:
        indices = [i for i, value in enumerate(inputs['hiatus']) if value is not None]
    else:
        indices = []
    clusters = find_clusters(indices)

    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    inputs['indices'] = indices # Only as placeholder for analysis unit IDs
    
    # For insert I need to check the following as well:
    # ['hiatuslength', 'hiatusuncertainty'] for "ndb.hiatuschronology"

    for values in clusters:
        try:
            h = Hiatus(analysisunitstart=values[0],
                       analysisunitend=values[-1],
                       notes=inputs['notes'])
            response.valid.append(True)
            response.message.append("✔ Hiatus can be created")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Hiatus cannot be created: {e}")

    if not clusters:
        response.valid.append(True)
        response.message.append("✔ No hiatuses found in the data")
    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response