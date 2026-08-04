from DataBUS import AeDNAAssay

from ._aedna_entity_validator import validate_aedna_entity


def _resolve_assaytype(cur, inputs, response):
    """Resolve the assay type name in ``assaytypeid`` to its ndb.assaytypes id."""
    if isinstance(inputs.get("assaytypeid"), str):
        cur.execute(
            "SELECT assaytypeid FROM ndb.assaytypes WHERE LOWER(assaytype) = %(t)s",
            {"t": inputs["assaytypeid"].lower().strip()},
        )
        row = cur.fetchone()
        if row:
            inputs["assaytypeid"] = row[0]
            response.valid.append(True)
            response.message.append(f"✔ Assay type resolved to assaytypeid {row[0]}.")
        else:
            inputs["assaytypeid"] = None
            response.valid.append(False)
            response.message.append("✗ Assay type is not known to Neotoma.")


def valid_assays(cur, yml_dict, csv_file, databus=None):
    """Validate and insert the dataset's aeDNA assay (ndb.aednaassays).

    Reads the assay ``value:`` constants from the template, resolves the assay
    type name to an id, links the dataset, and inserts one ``AeDNAAssay``. The
    new assayid lands in ``response.id_int`` for ``valid_aednalibraries``.
    """
    return validate_aedna_entity(
        cur,
        yml_dict,
        AeDNAAssay,
        databus,
        links={"datasetid": "datasets"},
        present_fields=["assayname"],
        resolve=_resolve_assaytype,
    )
