import DataBUS.neotomaHelpers as nh
from DataBUS import Hiatus, Response
from itertools import groupby

def find_clusters(indices):
    """Group consecutive indices into clusters.

    Takes a list of indices and groups consecutive integers together.
    Used to identify contiguous analysis units for hiatus definition.

    Examples:
        >>> find_clusters([1, 2, 3, 5, 6])
        [[1, 2, 3], [5, 6]]

    Args:
        indices (list): List of integer indices.

    Returns:
        list: List of lists containing grouped consecutive indices.
    """
    if not indices:
        return []
    indices = sorted(indices)
    clusters = [
        [x[1] for x in group]
        for k, group in groupby(enumerate(indices), lambda x: x[1] - x[0])
    ]
    return clusters

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
    params = ['hiatus', 'notes']
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.hiatuses")

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
    response.message = list(set(response.message))
    return response