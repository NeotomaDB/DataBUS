from DataBUS import Response
from DataBUS.AeDNAModel import AeDNAModel
from DataBUS.Sequence import Sequence
from DataBUS.SequenceData import SequenceData


def valid_sequence(cur, yml_dict, csv_file, databus=None):
    """Validates and inserts aeDNA sequence and model data into Neotoma.

    Runs AFTER valid_data. For each unique DNA entry (dnasequence + asv) in the
    YAML template, inserts one row into ndb.sequences (linked to the datasetid),
    then creates ndb.sequencedata rows linking each associated dataid to that
    sequence. Finally inserts a corresponding ndb.aednamodels row.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file: CSV/XLSX data (unused directly, kept for signature uniformity).
        databus (dict | None): Prior validation results. Requires
            ``databus["datasets"].id_int`` for the dataset ID and
            ``databus["data"].id_dict`` for data IDs.

    Returns:
        Response: Response object containing validation messages, validity list,
            and overall status. ``id_dict`` maps entry keys to
            ``{"sequenceid": int, "modelid": int}`` dicts.
    """
    response = Response()

    dna_entries = _extract_dna_entries(yml_dict)
    if not dna_entries:
        response.message.append("? No aeDNA sequence entries found in template.")
        response.valid.append(True)
        return response

    model_name, superseeds_list = _extract_model_info(yml_dict)
    if model_name is None:
        response.message.append(
            "? No aeDNA model entry (ndb.aednamodels.modelid) found in template. "
            "Sequences will be inserted without model records."
        )

    try:
        datasetid = databus["datasets"].id_int
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Dataset ID not available from valid_dataset: {e}")
        return response

    try:
        data_ids = databus["data"].id_dict
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Data IDs not available from valid_data: {e}")
        return response

    key_to_dna = _build_key_to_dna_map(dna_entries, data_ids)

    if not key_to_dna:
        response.message.append(
            "? Could not match any DNA entries to data IDs. "
            "Ensure template entries with dnasequence/asv match data entries."
        )
        response.valid.append(True)
        return response

    taxonid_query = """
        SELECT v.taxonid
        FROM ndb.data d
        JOIN ndb.variables v ON d.variableid = v.variableid
        WHERE d.dataid = %(dataid)s;
    """

    response.id_dict = {}

    for entry_key, dna_info in key_to_dna.items():
        dataid_list = data_ids.get(entry_key, [])
        if not dataid_list:
            continue

        dnasequence = dna_info["dnasequence"]
        asv = dna_info["asv"]

        try:
            seq = Sequence(datasetid=datasetid, sequence=dnasequence, asv=asv)
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            if f"✗ Sequence cannot be created: {e}" not in response.message:
                response.message.append(f"✗ Sequence cannot be created: {e}")
            continue

        try:
            sequenceid = seq.insert_to_db(cur)
            response.valid.append(True)
            if f"✔ Sequence inserted for {entry_key}" not in response.message:
                response.message.append(f"✔ Sequence inserted for {entry_key}")
        except Exception as e:
            response.valid.append(False)
            if f"✗ Sequence cannot be inserted: {e}" not in response.message:
                response.message.append(f"✗ Sequence cannot be inserted: {e}")
            continue

        for dataid in dataid_list:
            try:
                sd = SequenceData(dataid=dataid, sequenceid=sequenceid)
                sd.insert_to_db(cur)
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                if f"✗ SequenceData cannot be inserted: {e}" not in response.message:
                    response.message.append(f"✗ SequenceData cannot be inserted: {e}")

        if model_name is None:
            response.id_dict[entry_key] = {"sequenceid": sequenceid, "modelid": None}
            continue

        first_dataid = dataid_list[0]
        try:
            cur.execute(taxonid_query, {"dataid": first_dataid})
            result = cur.fetchone()
            if result:
                taxonid = result[0]
                response.valid.append(True)
            else:
                response.valid.append(False)
                response.message.append(f"✗ Could not retrieve taxonid for dataid {first_dataid}.")
                response.id_dict[entry_key] = {"sequenceid": sequenceid, "modelid": None}
                continue
        except Exception as e:
            response.valid.append(False)
            if f"✗ Error retrieving taxonid: {e}" not in response.message:
                response.message.append(f"✗ Error retrieving taxonid: {e}")
            response.id_dict[entry_key] = {"sequenceid": sequenceid, "modelid": None}
            continue

        try:
            aedna = AeDNAModel(
                sequenceid=sequenceid,
                taxonid=taxonid,
                model=model_name,
            )
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            if f"✗ AeDNAModel cannot be created: {e}" not in response.message:
                response.message.append(f"✗ AeDNAModel cannot be created: {e}")
            response.id_dict[entry_key] = {"sequenceid": sequenceid, "modelid": None}
            continue

        try:
            modelid = aedna.insert_to_db(cur)
            response.valid.append(True)
            if f"✔ AeDNAModel inserted for {entry_key}" not in response.message:
                response.message.append(f"✔ AeDNAModel inserted for {entry_key}")
        except Exception as e:
            response.valid.append(False)
            if f"✗ AeDNAModel cannot be inserted: {e}" not in response.message:
                response.message.append(f"✗ AeDNAModel cannot be inserted: {e}")
            response.id_dict[entry_key] = {"sequenceid": sequenceid, "modelid": None}
            continue

        if superseeds_list:
            try:
                updated = aedna.supersede_previous(cur, superseeds_list)
                if updated > 0:
                    response.message.append(
                        f"✔ {updated} previous model(s) marked as "
                        f"superseded by {model_name} for taxonid {taxonid}."
                    )
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                if f"✗ Error updating superseded models: {e}" not in response.message:
                    response.message.append(f"✗ Error updating superseded models: {e}")

        response.id_dict[entry_key] = {"sequenceid": sequenceid, "modelid": modelid}

    if not response.valid:
        response.valid.append(True)
        response.message.append("? No DNA data entries matched for sequence insertion.")

    return response


def _extract_dna_entries(yml_dict):
    """Extract template entries that have dnasequence and asv metadata.

    Returns:
        list[dict]: Each dict has keys: taxonname, dnasequence, asv, column.
    """
    entries = []
    for entry in yml_dict.get("metadata", []):
        if entry.get("neotoma") == "ndb.data.value" and "dnasequence" in entry and "asv" in entry:
            entries.append(
                {
                    "taxonname": entry["taxonname"],
                    "dnasequence": entry["dnasequence"],
                    "asv": entry["asv"],
                    "column": entry.get("column"),
                }
            )
    return entries


def _extract_model_info(yml_dict):
    """Extract the aeDNA model name and superseeds list from the template.

    Returns:
        tuple: (model_name, superseeds_list) or (None, []) if not found.
    """
    for entry in yml_dict.get("metadata", []):
        if entry.get("neotoma") == "ndb.aednamodels.modelid":
            model_name = entry.get("value")
            superseeds_list = entry.get("superseeds", [])
            if isinstance(superseeds_list, str):
                superseeds_list = [s.strip() for s in superseeds_list.split(",")]
            return model_name, superseeds_list
    return None, []


def _build_key_to_dna_map(dna_entries, data_ids):
    """Build a mapping from id_dict keys to DNA metadata.

    Supports two key formats:
    - Compound keys (``taxonname::asv``) used when a taxon has multiple ASVs
    - Plain taxon name keys (one-to-one taxon-to-ASV only)

    Args:
        dna_entries (list[dict]): DNA entries extracted from the template.
        data_ids (dict): Keys from ``databus["data"].id_dict``.

    Returns:
        dict: Mapping of id_dict key -> {"dnasequence": str, "asv": str}.
    """
    key_to_dna = {}

    for entry in dna_entries:
        taxon = entry["taxonname"]
        asv = entry["asv"]
        compound_key = f"{taxon}::{asv}"

        # Prefer compound key
        if compound_key in data_ids:
            key_to_dna[compound_key] = {
                "dnasequence": entry["dnasequence"],
                "asv": entry["asv"],
            }
        # Fall back to plain taxon key (one-to-one taxon-to-ASV)
        elif taxon in data_ids and taxon not in key_to_dna:
            key_to_dna[taxon] = {
                "dnasequence": entry["dnasequence"],
                "asv": entry["asv"],
            }

    return key_to_dna
