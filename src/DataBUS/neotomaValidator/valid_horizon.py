import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def valid_horizon(yml_dict, csv_template):
    """Validates dated horizons against analysis unit depths.

    Validates that the dating horizon (e.g., lead-210 dating level) corresponds to
    one of the reported depths in the analysis unit data. Returns the index of the
    matching depth if found.

    Args:
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_template (list): List of dictionaries representing CSV file data.

    Returns:
        Response: Response object with validation results, matched depth index, and messages.
    
    Examples:
        >>> valid_horizon(config_dict, csv_data)
        Response(valid=[True], index=3, message=[...], validAll=True)
    """
    response = Response()

    params = ["depth"]
    depths = nh.pull_params(params, yml_dict, csv_template, "ndb.analysisunits")

    params2 = ["datinghorizon"]
    horizon = nh.pull_params(params2, yml_dict, csv_template, "ndb.leadmodels")

    if len(horizon["datinghorizon"]) == 1:
        matchingdepth = [i == horizon["datinghorizon"][0] for i in depths["depth"]]
        if any(matchingdepth):
            response.valid.append(True)
            response.index = next(i for i, v in enumerate(matchingdepth) if v)
            response.message.append("✔  The dating horizon is in the reported depths.")
            response.valid.append(True)
        else:
            response.valid.append(False)
            response.index = -1
            response.message.append(
                "✗  There is no depth entry for the dating horizon in the 'depths' column."
            )
    else:
        response.valid.append(False)
        response.index = None
        if len(horizon) > 1:
            response.message.append("✗  Multiple dating horizons are reported.")
        else:
            response.message.append("✗  No dating horizon is reported.")

    return response