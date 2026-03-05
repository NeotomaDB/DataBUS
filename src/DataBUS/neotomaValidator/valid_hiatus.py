from itertools import groupby

import DataBUS.neotomaHelpers as nh
from DataBUS import Hiatus, Response
from DataBUS.Hiatus import HIATUS_PARAMS


def valid_hiatus(cur, yml_dict, csv_file, databus=None):
    """Validates hiatus data and inserts hiatus records when databus is provided.

    Identifies hiatus intervals (stratigraphic gaps) in sample analysis units.
    Groups consecutive analysis units with hiatus data and creates Hiatus objects
    spanning from start to end of each hiatus interval.

    When ``databus`` is provided, resolves cluster indices to real analysis unit IDs
    using ``databus["analysisunits"].id_list`` and inserts each Hiatus record into
    ``ndb.hiatuses`` via ``hiatus.insert_to_db(cur)``.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list[dict]): List of row dicts from the CSV file.
        databus (dict | None): Prior validation results. When not None, uses
            ``databus["analysisunits"].id_list`` to resolve AU IDs for the insert.

    Returns:
        Response: Response object containing validation messages and overall validity status.

    Examples:
        >>> valid_hiatus(cursor, config_dict, csv_rows)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    inputs = nh.pull_params(HIATUS_PARAMS, yml_dict, csv_file, "ndb.hiatuses")

    if inputs.get("hiatus"):
        indices = [i for i, value in enumerate(inputs["hiatus"]) if value is not None]
    else:
        response.valid.append(True)
        response.message.append("✔ No hiatuses found in the data.")
        return response

    clusters = _find_clusters(indices)
    inputs = {
        k: [v for i, v in enumerate(inputs[k]) if i in indices]
        if isinstance(inputs[k], list)
        else inputs[k]
        for k in inputs
    }
    inputs["indices"] = indices  # Only as placeholder for analysis unit IDs

    try:
        au_ids = databus["analysisunits"].id_list
        resolved = [
            [au_ids[c[0]], au_ids[c[-1]]] if len(c) > 1 else [au_ids[c[0]]] for c in clusters
        ]
        response.valid.append(True)
    except Exception as e:
        response.valid.append(False)
        resolved = clusters
        response.message.append(
            f"✗ Could not resolve analysis unit IDs from databus. Using placeholder indices: {e}"
        )

    for values in resolved:
        try:
            h = Hiatus(
                analysisunitstart=values[0], analysisunitend=values[-1], notes=inputs.get("notes")
            )
            response.valid.append(True)
            if "✔ Hiatus can be created." not in response.message:
                response.message.append("✔ Hiatus can be created.")
            try:
                h.insert_to_db(cur)
                response.valid.append(True)
                response.message.append("✔ Hiatus inserted.")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Could not insert hiatus: {e}")
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
        [x[1] for x in group] for k, group in groupby(enumerate(indices), lambda x: x[1] - x[0])
    ]
    return clusters
