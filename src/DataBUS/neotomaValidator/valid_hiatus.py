import DataBUS.neotomaHelpers as nh
from DataBUS import Hiatus, Response
from DataBUS.Hiatus import HIATUS_PARAMS
from itertools import groupby
 
def valid_hiatus(cur, yml_dict, csv_file):
    """Validates hiatus data for chronological models.

    Identifies hiatus intervals (stratigraphic gaps) in sample analysis units.
    Groups consecutive analysis units with hiatus data and creates Hiatus objects
    spanning from start to end of each hiatus interval.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing hiatus information.

    Returns:
        Response: Response object containing validation messages and overall validity status.

    Examples:
        >>> valid_hiatus(cursor, config_dict, "hiatus_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    inputs = nh.pull_params(HIATUS_PARAMS, yml_dict, csv_file, "ndb.hiatuses")

    if inputs.get('hiatus'):
        indices = [i for i, value in enumerate(inputs['hiatus']) if value is not None]
    else:
        response.valid.append(True)
        response.message.append("✔ No hiatuses found in the data.")
        return response
    
    clusters = _find_clusters(indices)
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices]
              if isinstance(inputs[k], list) else inputs[k]
              for k in inputs}
    inputs['indices'] = indices  # Only as placeholder for analysis unit IDs

    for values in clusters:
        try:
            Hiatus(analysisunitstart=values[0],
                   analysisunitend=values[-1],
                   notes=inputs.get('notes'))
            response.valid.append(True)
            if f"✔ Hiatus can be created." not in response.message:
                response.message.append("✔ Hiatus can be created.")
        except Exception as e:
            response.valid.append(False)
            if f"✗ Hiatus cannot be created: {e}" not in response.message:
             response.message.append(f"✗ Hiatus cannot be created: {e}")
    return response

def _find_clusters(indices):
    """Group consecutive indices into clusters.
    Takes a list of indices and groups consecutive integers together.
    Used to identify contiguous analysis units for hiatus definition.
    """
    if not indices:
        return []
    indices = sorted(indices)
    clusters = [
        [x[1] for x in group]
        for k, group in groupby(enumerate(indices), lambda x: x[1] - x[0])
    ]
    return clusters